"""Shopify client: bulk-result rebuild and throttle handling."""

import json

from pipelines.lib.shopify import ORDERS_BULK_QUERY, PRODUCTS_BULK_QUERY, _M, money, rebuild
from pipelines.shopify.load import order_row, payout_row


def test_rebuild_folds_children_under_parents():
    lines = [
        {"__typename": "Order", "id": "gid://shopify/Order/1", "name": "#1001",
         "refunds": [{"__typename": "Refund", "id": "gid://shopify/Refund/9", "totalRefundedSet": {"shopMoney": {"amount": "5.00"}}}],
         "transactions": [{"id": "gid://shopify/OrderTransaction/50", "kind": "SALE"}]},
        {"__typename": "LineItem", "id": "gid://shopify/LineItem/10", "sku": "A", "__parentId": "gid://shopify/Order/1"},
        {"__typename": "LineItem", "id": "gid://shopify/LineItem/11", "sku": "B", "__parentId": "gid://shopify/Order/1"},
        {"__typename": "RefundLineItem", "id": "gid://shopify/RefundLineItem/70", "quantity": 1, "__parentId": "gid://shopify/Refund/9"},
        {"__typename": "OrderTransaction", "id": "gid://shopify/OrderTransaction/51", "kind": "REFUND", "__parentId": "gid://shopify/Refund/9"},
        {"__typename": "DiscountCodeApplication", "code": "SAVE10", "__parentId": "gid://shopify/Order/1"},
        {"__typename": "Order", "id": "gid://shopify/Order/2", "name": "#1002", "refunds": [], "transactions": []},
    ]
    orders = rebuild(iter(lines))
    assert [o["id"] for o in orders] == ["gid://shopify/Order/1", "gid://shopify/Order/2"]
    o1, o2 = orders
    assert [i["sku"] for i in o1["lineItems"]] == ["A", "B"]
    assert o1["discountApplications"][0]["code"] == "SAVE10"
    assert o1["transactions"][0]["kind"] == "SALE"          # inlined list untouched
    refund = o1["refunds"][0]
    assert refund["refundLineItems"][0]["quantity"] == 1
    assert refund["transactions"][0]["kind"] == "REFUND"    # child of the inlined refund, not the order
    assert refund["refundShippingLines"] == []
    assert o2["lineItems"] == [] and o2["shippingLines"] == [] and o2["discountApplications"] == []
    assert "__parentId" not in json.dumps(orders)


def test_rebuild_products_inventory_levels():
    lines = [
        {"__typename": "Product", "id": "gid://shopify/Product/1", "title": "Pen"},
        {"__typename": "ProductVariant", "id": "gid://shopify/ProductVariant/5", "sku": "PEN-RED",
         "inventoryItem": {"__typename": "InventoryItem", "id": "gid://shopify/InventoryItem/8", "tracked": True},
         "__parentId": "gid://shopify/Product/1"},
        {"__typename": "InventoryLevel", "id": "gid://shopify/InventoryLevel/3?inventory_item_id=8",
         "location": {"id": "gid://shopify/Location/1", "name": "Warehouse"},
         "quantities": [{"name": "available", "quantity": 4}], "__parentId": "gid://shopify/ProductVariant/5"},
        {"__typename": "Product", "id": "gid://shopify/Product/2", "title": "Empty"},
    ]
    products = rebuild(iter(lines))
    v = products[0]["variants"][0]
    assert v["sku"] == "PEN-RED"
    assert v["inventoryItem"]["inventoryLevels"][0]["location"]["name"] == "Warehouse"
    assert products[1]["variants"] == []


def test_bulk_queries_have_no_pagination_args():
    q = ORDERS_BULK_QUERY % {"filter": json.dumps("updated_at:>=2026-01-01T00:00:00Z"), "m": _M}
    assert "first:" not in q and "after:" not in q
    assert 'orders(query: "updated_at:>=2026-01-01T00:00:00Z"' in q
    assert "first:" not in PRODUCTS_BULK_QUERY


def test_rows_and_money():
    assert money({"shopMoney": {"amount": "12.50", "currencyCode": "USD"}}) == 12.5
    assert money({"amount": "3"}) == 3.0
    assert money(None) is None
    o = {"id": "gid://shopify/Order/1", "name": "#1", "updatedAt": "2026-09-01T00:00:00Z", "test": False,
         "displayFinancialStatus": "PAID"}
    row = order_row(o, "2026-09-24T00:00:00+00:00", "run")
    assert row["order_id"] == "gid://shopify/Order/1" and row["financial_status"] == "PAID" and row["payload"] is o
    p = payout_row({"id": "gid://shopify/ShopifyPaymentsPayout/1", "status": "PAID",
                    "net": {"amount": "99.10", "currencyCode": "USD"}}, "t", "r")
    assert p["net"] == 99.1 and p["currency"] == "USD"


def test_throttled_query_waits_then_retries(monkeypatch):
    from pipelines.lib import shopify as mod

    calls = []

    class Resp:
        status_code = 200
        headers = {}
        text = ""

        def __init__(self, body):
            self._body = body

        def json(self):
            return self._body

    class Session:
        def post(self, url, **kw):
            calls.append(kw["json"]["query"])
            if len(calls) == 1:
                return Resp({"errors": [{"message": "Throttled", "extensions": {"code": "THROTTLED"}}],
                             "extensions": {"cost": {"requestedQueryCost": 100,
                                                     "throttleStatus": {"currentlyAvailable": 0, "restoreRate": 100}}}})
            return Resp({"data": {"shop": {"name": "Pop"}},
                         "extensions": {"cost": {"actualQueryCost": 1,
                                                 "throttleStatus": {"currentlyAvailable": 999, "restoreRate": 100}}}})

    slept = []
    monkeypatch.setattr(mod.time, "sleep", lambda s: slept.append(s))
    client = mod.ShopifyClient("popcolors", "tok", session=Session())
    assert client.shop_info() == {"name": "Pop"}
    assert len(calls) == 2 and slept and slept[0] >= 1.0


def test_client_credentials_grant(monkeypatch):
    from pipelines.lib import shopify as mod

    class Resp:
        status_code = 200
        text = ""

        def json(self):
            return {"access_token": "shpat_x", "expires_in": 86399}

    posted = {}

    class Session:
        def post(self, url, **kw):
            posted["url"] = url
            posted["data"] = kw["data"]
            return Resp()

    tok = mod.access_token_from_client_credentials("popcolors", "cid", "csec", Session())
    assert tok == "shpat_x"
    assert posted["url"] == "https://popcolors.myshopify.com/admin/oauth/access_token"
    assert posted["data"]["grant_type"] == "client_credentials"
