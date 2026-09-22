import datetime as dt
import json

import pandas as pd

from pipelines.lib.sku import parent_sku_legacy
from pipelines.spapi import awd_inventory, fba_ledger, finances, orders, settlements, traffic


def test_parent_sku_legacy_rule():
    assert parent_sku_legacy("STRANGR-CPNCLS-FBA-UPC") == "STRANGR-CPNCLS"
    assert parent_sku_legacy("amzn.gr.KPOP-CPNCLS") == "KPOP-CPNCLS"
    assert parent_sku_legacy("KPOP-CPNCLS") == "KPOP-CPNCLS"
    assert parent_sku_legacy(None) is None


def test_orders_window_is_trailing_30_days_ending_yesterday():
    start, end = orders.window(dt.datetime(2026, 9, 21, 12, tzinfo=dt.timezone.utc))
    assert start == dt.datetime(2026, 8, 22, tzinfo=dt.timezone.utc)
    assert end == dt.datetime(2026, 9, 20, 23, 59, 59, tzinfo=dt.timezone.utc)


def test_orders_transform_adds_parent_sku():
    df = orders.transform("amazon-order-id\tsku\tquantity\n111-1\tKPOP-CPNCLS-FBA\t2\n")
    assert list(df["Parent SKU"]) == ["KPOP-CPNCLS"]
    assert df["quantity"].iloc[0] == "2"  # stays text until coerced to the table schema


def test_traffic_transform_flattens_nested_sales_and_traffic():
    report = {"salesAndTrafficByAsin": [{
        "parentAsin": "B0P", "childAsin": "B0C", "sku": "KPOP-CPNCLS-FBA",
        "salesByAsin": {"unitsOrdered": 3, "orderedProductSales": {"amount": 71.97, "currencyCode": "USD"}},
        "trafficByAsin": {"sessions": 40, "pageViews": 55},
    }]}
    df = traffic.transform(json.dumps(report), dt.date(2026, 9, 20))
    row = df.iloc[0]
    assert row["date"] == dt.date(2026, 9, 20)
    assert row["orderedProductSales"] == 71.97 and row["sessions"] == 40
    assert row["Parent SKU"] == "KPOP-CPNCLS"
    assert traffic.transform({"salesAndTrafficByAsin": []}, dt.date(2026, 9, 20)).empty


def test_traffic_days_are_the_7_complete_pacific_days_before_today():
    d = traffic.days(dt.date(2026, 9, 21))
    assert d[0] == dt.date(2026, 9, 14) and d[-1] == dt.date(2026, 9, 20) and len(d) == 7


def test_traffic_skips_only_the_most_recent_empty_day(monkeypatch):
    calls = []

    class FakeSp:
        def run_report(self, *a, **k):
            calls.append(a[1].date())
            return json.dumps({"salesAndTrafficByAsin": []}) if a[1].date() == dt.date(2026, 9, 20) else json.dumps(
                {"salesAndTrafficByAsin": [{"sku": "A", "salesByAsin": {"unitsOrdered": 1}, "trafficByAsin": {"sessions": 2}}]})

    monkeypatch.setattr(traffic, "days", lambda today=None: [dt.date(2026, 9, 19), dt.date(2026, 9, 20)])
    assert traffic.run(dry_run=True, client=FakeSp(), pause_s=0) == 1
    monkeypatch.setattr(traffic, "days", lambda today=None: [dt.date(2026, 9, 20), dt.date(2026, 9, 21)])
    import pytest
    with pytest.raises(RuntimeError):
        traffic.run(dry_run=True, client=FakeSp(), pause_s=0)


def test_settlements_transform_labels_summary_row_and_fills_ids():
    text = ("settlement-id\tsettlement-start-date\tsettlement-end-date\tdeposit-date\ttotal-amount\tcurrency\t"
            "transaction-type\torder-id\tamount\tposted-date\tquantity-purchased\n"
            "38595\t2026-09-07\t2026-09-21\t2026-09-23\t1234.56\tUSD\t\t\t\t\t\n"
            "\t\t\t\t\t\tOrder\t111-1\t23.99\t2026-09-10\t1\n")
    df = settlements.transform([text])
    assert list(df["settlement_id"]) == ["38595", "38595"]
    assert df["transaction_type"].iloc[0] == "Settlement" and df["transaction_type"].iloc[1] == "Order"
    assert df["settlement_end_date"].iloc[1] == pd.Timestamp("2026-09-21")
    assert df["quantity_purchased"].iloc[1] == 1


def test_finances_flatten_covers_every_event_family():
    page = {
        "ShipmentEventList": [{"AmazonOrderId": "111-1", "PostedDate": "2026-09-20T01:00:00Z", "ShipmentItemList": [
            {"SellerSKU": "A", "ItemChargeList": [{"ChargeType": "Principal", "ChargeAmount": {"CurrencyAmount": 23.99, "CurrencyCode": "USD"}}],
             "ItemFeeList": [{"FeeType": "Commission", "FeeAmount": {"CurrencyAmount": -3.6, "CurrencyCode": "USD"}}]}]}],
        "RefundEventList": [{"AmazonOrderId": "111-2", "PostedDate": "2026-09-20T02:00:00Z", "ShipmentItemAdjustmentList": [
            {"SellerSKU": "B", "ItemChargeAdjustmentList": [{"ChargeType": "Principal", "ChargeAmount": {"CurrencyAmount": -9.99, "CurrencyCode": "USD"}}]}]}],
        "ServiceFeeEventList": [{"CreationDate": "2026-09-20T03:00:00Z", "FeeList": [{"FeeType": "FBAInboundTransportationFee", "FeeAmount": {"CurrencyAmount": -12.0, "CurrencyCode": "USD"}}]}],
    }
    rows = finances.flatten(page)
    kinds = sorted(r["transaction_type"] for r in rows)
    assert kinds == ["Fee", "Order", "Refund", "ServiceFee"]
    assert {r["amount"] for r in rows} == {23.99, -3.6, -9.99, -12.0}
    assert [r for r in rows if r["transaction_type"] == "ServiceFee"][0]["amazon_order_id"] == "Non-Order Fee"


def test_fba_ledger_transform_renames_and_flags_stock():
    text = ("Date\tMSKU\tTitle\tDisposition\tStarting Warehouse Balance\tCustomer Shipments\tWarehouse Transfer In/Out\tOther Events\tEnding Warehouse Balance\n"
            "2026-09-20\tKPOP-CPNCLS-FBA\tt\tSELLABLE\t10\t-2\t0\t0\t8\n2026-09-20\tX-Y-Z\tt\tSELLABLE\t0\t0\t0\t0\t0\n")
    df = fba_ledger.transform(text)
    assert {"Shipments", "WhseTransfers", "Adjustments", "Ending Warehouse Balance"} <= set(df.columns)
    assert "Customer Shipments" not in df.columns
    assert list(df["Inventory Binary"]) == [1, 0]
    assert df["Parent SKU"].iloc[0] == "KPOP-CPNCLS"
    assert fba_ledger.target_day(dt.date(2026, 9, 21)) == dt.date(2026, 9, 20)


def test_awd_transform_flattens_quantities_and_expiration():
    items = [{"sku": "KPOP-CPNCLS-FBA", "totalInboundQuantity": 5,
              "inventoryDetails": {"totalOnhandQuantity": 100, "availableDistributableQuantity": 90,
                                   "replenishmentQuantity": 0, "reservedDistributableQuantity": 10},
              "expirationDetails": [{"expiration": "2027-01-31T00:00:00Z", "onhandQuantity": 100}]}]
    df = awd_inventory.transform(items, dt.date(2026, 9, 20))
    row = df.iloc[0]
    assert row["totalOnhandQuantity"] == 100 and row["totalInboundQuantity"] == 5
    assert row["earliestExpirationDate"] == "2027-01-31" and row["Parent SKU"] == "KPOP-CPNCLS"
    assert awd_inventory.snapshot_date(dt.datetime(2026, 9, 21, 20, tzinfo=dt.timezone.utc)) == dt.date(2026, 9, 20)


def test_clients_retry_dropped_connections(monkeypatch):
    import requests
    from pipelines.lib.spapi import SpApiClient
    from pipelines.lib.ads_api import AdsApiClient

    class Flaky:
        def __init__(self): self.n = 0
        def request(self, *a, **k):
            self.n += 1
            if self.n == 1:
                raise requests.exceptions.ConnectionError("Remote end closed connection")
            r = requests.Response(); r.status_code = 200; r._content = b'{"ok": true}'; return r

    monkeypatch.setattr("time.sleep", lambda s: None)
    for c in (SpApiClient("i", "s", "r"), AdsApiClient("i", "s", "r", profile_id="1")):
        c.session = Flaky(); c._access_token = "t"; c._token_expires_at = 9e12
        assert c.request("GET", "/x").json() == {"ok": True} and c.session.n == 2
