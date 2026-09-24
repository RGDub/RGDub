"""Shopify Admin GraphQL API client.

Auth is the client credentials grant: the app (``punlabs-data``, created in
the Shopify Dev Dashboard and installed on the store) exchanges its client id
and secret for an Admin API access token that lasts 24 hours. There is no
static token to copy; the client mints one on construction. The credentials
live in Secret Manager as ``shopify-client-id`` and ``shopify-client-secret``.
The store is identified by its myshopify subdomain (``popcolors``).

    from pipelines.lib.shopify import shopify_client_from_secrets
    shop = shopify_client_from_secrets()
    for order in shop.orders(updated_at_min="2026-09-01T00:00:00Z"):
        ...

Two ways of reading:

* ``query`` runs one GraphQL document. Shopify meters queries by "cost" (about
  one point per object, connections cost 2 + first x child) against a bucket
  that refills at a fixed rate; the client reads the throttle status returned
  with every response, waits when a query is throttled, and paces itself when
  the bucket runs low. Dropped connections, 429s and 5xx are retried.
* ``bulk`` runs a bulk operation: Shopify executes the query server-side with
  no cost limit and hands back a JSONL file. Nested connections come back as
  separate lines tagged ``__parentId``; ``rebuild`` folds them back under
  their parents so each order (or product) is one nested dict again. Only one
  bulk operation runs per shop at a time; the client waits for any in flight.

Orders and products are read with bulk operations (a whole order with its
line items, refunds and transactions costs far more than the 1,000-point
per-query ceiling allows at any useful page size). Payouts are paginated.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Iterator

import requests

log = logging.getLogger(__name__)

API_VERSION = "2026-04"
SECRET_IDS = {"client_id": "shopify-client-id", "client_secret": "shopify-client-secret"}
DEFAULT_SHOP = os.environ.get("SHOPIFY_SHOP", "popcolors")
BULK_POLL_S = 5
BULK_TIMEOUT_S = 1800


class ShopifyApiError(RuntimeError):
    def __init__(self, message: str, errors: list | None = None):
        super().__init__(message)
        self.errors = errors or []


class ShopifyAccessDenied(ShopifyApiError):
    """The token lacks a scope the query needs (Shopify error code ACCESS_DENIED)."""


def money(obj: dict | None, key: str = "shopMoney") -> float | None:
    """Amount from a MoneyBag ({shopMoney: {amount}}) or MoneyV2 ({amount})."""
    if not obj:
        return None
    inner = obj.get(key) if key in obj else obj
    amt = (inner or {}).get("amount")
    return float(amt) if amt is not None else None


# -------------------------------------------------------------------- queries
# Every connection node carries __typename so bulk result lines can be folded
# back under the right parent field (see PARENT_FIELDS / rebuild).
_M = "{ shopMoney { amount currencyCode } }"

ORDERS_BULK_QUERY = """
{
  orders(query: %(filter)s, sortKey: UPDATED_AT) {
    edges { node {
      __typename id name createdAt updatedAt processedAt cancelledAt closedAt cancelReason
      test confirmed currencyCode sourceName displayFinancialStatus displayFulfillmentStatus
      tags note poNumber
      app { name }
      shippingAddress { city provinceCode countryCodeV2 zip }
      subtotalPriceSet %(m)s
      totalDiscountsSet %(m)s
      totalShippingPriceSet %(m)s
      totalTaxSet %(m)s
      totalPriceSet %(m)s
      totalRefundedSet %(m)s
      totalRefundedShippingSet %(m)s
      totalTipReceivedSet %(m)s
      totalReceivedSet %(m)s
      netPaymentSet %(m)s
      currentSubtotalPriceSet %(m)s
      currentTotalDiscountsSet %(m)s
      currentTotalTaxSet %(m)s
      currentTotalPriceSet %(m)s
      currentTotalDutiesSet %(m)s
      currentTotalAdditionalFeesSet %(m)s
      discountCodes
      discountApplications { edges { node {
        __typename allocationMethod targetSelection targetType
        value { __typename ... on MoneyV2 { amount currencyCode } ... on PricingPercentageValue { percentage } }
        ... on DiscountCodeApplication { code }
        ... on AutomaticDiscountApplication { title }
        ... on ManualDiscountApplication { title description }
        ... on ScriptDiscountApplication { title }
      } } }
      shippingLines { edges { node {
        __typename id title code source
        originalPriceSet %(m)s
        discountedPriceSet %(m)s
      } } }
      lineItems { edges { node {
        __typename id name title variantTitle sku vendor quantity currentQuantity refundableQuantity
        requiresShipping taxable isGiftCard
        product { id } variant { id }
        originalUnitPriceSet %(m)s
        discountedUnitPriceSet %(m)s
        originalTotalSet %(m)s
        discountedTotalSet %(m)s
        totalDiscountSet %(m)s
        discountAllocations { allocatedAmountSet %(m)s discountApplication { __typename ... on DiscountCodeApplication { code } } }
        taxLines { title rate ratePercentage priceSet %(m)s }
      } } }
      refunds {
        __typename id createdAt note
        totalRefundedSet %(m)s
      }
      transactions {
        id kind status gateway processedAt test paymentId
        amountSet %(m)s
        fees { amount { amount currencyCode } flatFee { amount } rate rateName type }
      }
      fulfillments {
        id status createdAt updatedAt deliveredAt
        trackingInfo { company number }
        location { id name }
      }
    } }
  }
}
"""

# Bulk queries allow at most five connections and none inside a list field, so
# refund detail (a connection under the ``refunds`` list) is fetched per
# refunded order with this ordinary query and merged in.
REFUNDS_QUERY = """
query Refunds($id: ID!) {
  order(id: $id) {
    refunds {
      __typename id createdAt note
      totalRefundedSet %(m)s
      refundLineItems(first: 100) { nodes {
        __typename id quantity restockType
        lineItem { id sku }
        subtotalSet %(m)s
        totalTaxSet %(m)s
      } }
      refundShippingLines(first: 20) { nodes {
        __typename id
        subtotalAmountSet %(m)s
        taxAmountSet %(m)s
      } }
      transactions(first: 20) { nodes {
        __typename id kind status gateway processedAt
        amountSet %(m)s
      } }
    }
  }
}
""" % {"m": _M}

PRODUCTS_BULK_QUERY = """
{
  products {
    edges { node {
      __typename id title handle status vendor productType createdAt updatedAt publishedAt tags totalInventory
      variants { edges { node {
        __typename id title sku barcode price compareAtPrice position inventoryQuantity createdAt updatedAt
        selectedOptions { name value }
        inventoryItem {
          __typename id tracked
          unitCost { amount currencyCode }
          inventoryLevels { edges { node {
            __typename id
            location { id name }
            quantities(names: ["available", "on_hand", "committed", "incoming", "reserved"]) { name quantity }
          } } }
        }
      } } }
    } }
  }
}
"""

PAYOUTS_QUERY = """
query Payouts($cursor: String) {
  shopifyPaymentsAccount {
    payouts(first: 50, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id legacyResourceId issuedAt status transactionType
        net { amount currencyCode }
        summary {
          adjustmentsFee { amount } adjustmentsGross { amount }
          chargesFee { amount } chargesGross { amount }
          refundsFee { amount } refundsFeeGross { amount }
          reservedFundsFee { amount } reservedFundsGross { amount }
          retriedPayoutsFee { amount } retriedPayoutsGross { amount }
        }
      }
    }
  }
}
"""

SHOP_QUERY = "{ shop { name myshopifyDomain ianaTimezone currencyCode } }"

# Bulk result line typename -> the parent field it belongs under.
PARENT_FIELDS = {
    "LineItem": "lineItems",
    "ShippingLine": "shippingLines",
    "DiscountCodeApplication": "discountApplications",
    "AutomaticDiscountApplication": "discountApplications",
    "ManualDiscountApplication": "discountApplications",
    "ScriptDiscountApplication": "discountApplications",
    "RefundLineItem": "refundLineItems",
    "RefundShippingLine": "refundShippingLines",
    "OrderTransaction": "transactions",
    "ProductVariant": "variants",
    "InventoryLevel": "inventoryLevels",
}
# Connection fields to materialise as [] when a parent had no children.
EMPTY_FIELDS = {
    "Order": ("lineItems", "shippingLines", "discountApplications"),
    "Refund": ("refundLineItems", "refundShippingLines", "transactions"),
    "Product": ("variants",),
    "InventoryItem": ("inventoryLevels",),
}


def unwrap_nodes(obj: Any) -> Any:
    """Replace ``{"nodes": [...]}`` connection wrappers with plain lists, recursively."""
    if isinstance(obj, dict):
        if set(obj) <= {"nodes", "pageInfo"} and "nodes" in obj:
            return [unwrap_nodes(n) for n in obj["nodes"]]
        return {k: unwrap_nodes(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [unwrap_nodes(v) for v in obj]
    return obj


def _index(obj: Any, by_id: dict[str, dict]) -> None:
    """Register every dict with an ``id`` in ``obj`` (recursively) so children can find inlined parents."""
    if isinstance(obj, dict):
        if isinstance(obj.get("id"), str):
            by_id[obj["id"]] = obj
        for v in obj.values():
            _index(v, by_id)
    elif isinstance(obj, list):
        for v in obj:
            _index(v, by_id)


def rebuild(lines: Iterator[dict]) -> list[dict]:
    """Fold a bulk operation's JSONL lines back into nested root objects."""
    roots: list[dict] = []
    by_id: dict[str, dict] = {}
    for obj in lines:
        parent_id = obj.pop("__parentId", None)
        if parent_id is None:
            roots.append(obj)
        else:
            parent = by_id.get(parent_id)
            if parent is None:
                raise ShopifyApiError(f"bulk result line references unknown parent {parent_id}: {str(obj)[:200]}")
            fld = PARENT_FIELDS.get(obj.get("__typename", ""))
            if fld is None:
                raise ShopifyApiError(f"no parent field mapping for bulk line typename {obj.get('__typename')!r}")
            parent.setdefault(fld, []).append(obj)
        _index(obj, by_id)
    _fill_empty(roots)
    return roots


def _fill_empty(obj: Any) -> None:
    if isinstance(obj, dict):
        for fld in EMPTY_FIELDS.get(obj.get("__typename", ""), ()):
            obj.setdefault(fld, [])
        for v in obj.values():
            _fill_empty(v)
    elif isinstance(obj, list):
        for v in obj:
            _fill_empty(v)


@dataclass
class ShopifyClient:
    shop: str
    access_token: str
    api_version: str = API_VERSION
    session: requests.Session = field(default_factory=requests.Session)
    max_retries: int = 8

    @property
    def url(self) -> str:
        return f"https://{self.shop}.myshopify.com/admin/api/{self.api_version}/graphql.json"

    @property
    def headers(self) -> dict[str, str]:
        return {"X-Shopify-Access-Token": self.access_token, "Content-Type": "application/json",
                "Accept": "application/json"}

    # ------------------------------------------------------------ transport
    def query(self, query: str, variables: dict | None = None, timeout: int = 120) -> dict:
        """Run one GraphQL document and return its ``data``; raises on GraphQL errors."""
        delay = 2.0
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.post(self.url, headers=self.headers, timeout=timeout,
                                         json={"query": query, "variables": variables or {}})
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                if attempt == self.max_retries:
                    raise
                log.warning("Shopify: %s; retrying in %.0fs (%d/%d)", exc, delay, attempt, self.max_retries)
                time.sleep(delay)
                delay = min(delay * 2, 120)
                continue
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt == self.max_retries:
                    raise ShopifyApiError(f"HTTP {resp.status_code}: {resp.text[:500]}")
                wait = float(resp.headers.get("Retry-After") or delay)
                log.warning("Shopify HTTP %s; retrying in %.0fs (%d/%d)", resp.status_code, wait, attempt, self.max_retries)
                time.sleep(wait)
                delay = min(delay * 2, 120)
                continue
            if resp.status_code >= 400:
                raise ShopifyApiError(f"HTTP {resp.status_code}: {resp.text[:500]}")
            body = resp.json()
            errors = body.get("errors") or []
            codes = {(e.get("extensions") or {}).get("code") for e in errors}
            if "THROTTLED" in codes:
                cost = (body.get("extensions") or {}).get("cost") or {}
                status = cost.get("throttleStatus") or {}
                need = float(cost.get("requestedQueryCost") or 0) - float(status.get("currentlyAvailable") or 0)
                wait = max(1.0, need / max(float(status.get("restoreRate") or 50), 1.0)) + 0.5
                log.warning("Shopify throttled (need %.0f points); waiting %.1fs", need, wait)
                time.sleep(wait)
                continue
            if errors:
                msg = "; ".join(e.get("message", "?") for e in errors)
                if "ACCESS_DENIED" in codes:
                    raise ShopifyAccessDenied(f"access denied: {msg}", errors)
                raise ShopifyApiError(f"GraphQL errors: {msg}", errors)
            self._pace((body.get("extensions") or {}).get("cost") or {})
            return body.get("data") or {}
        raise AssertionError("unreachable")

    @staticmethod
    def _pace(cost: dict) -> None:
        """Sleep a little when the cost bucket is nearly empty, instead of getting throttled next call."""
        status = cost.get("throttleStatus") or {}
        actual = float(cost.get("actualQueryCost") or 0)
        available = float(status.get("currentlyAvailable") or 0)
        rate = max(float(status.get("restoreRate") or 50), 1.0)
        if actual and available < 2 * actual:
            time.sleep(min((2 * actual - available) / rate, 20))

    def paginate(self, query: str, path: str, variables: dict | None = None) -> Iterator[dict]:
        """Yield nodes of the connection at dotted ``path``; the query must take ``$cursor``."""
        cursor = None
        while True:
            data = self.query(query, {**(variables or {}), "cursor": cursor})
            conn: Any = data
            for key in path.split("."):
                conn = (conn or {}).get(key)
            if not conn:
                return
            yield from conn.get("nodes") or []
            info = conn.get("pageInfo") or {}
            if not info.get("hasNextPage"):
                return
            cursor = info.get("endCursor")

    # ------------------------------------------------------------ bulk operations
    def _current_bulk(self) -> dict | None:
        """The newest bulk query operation, if one exists."""
        data = self.query("{ bulkOperations(first: 5, sortKey: CREATED_AT, reverse: true) "
                          "{ nodes { id status type errorCode objectCount url } } }")
        ops = [o for o in ((data.get("bulkOperations") or {}).get("nodes") or []) if o.get("type") == "QUERY"]
        return ops[0] if ops else None

    def _wait_bulk(self, op_id: str, timeout_s: int = BULK_TIMEOUT_S) -> dict:
        deadline = time.monotonic() + timeout_s
        while True:
            data = self.query(
                "query($id: ID!) { node(id: $id) { ... on BulkOperation { id status errorCode objectCount url } } }",
                {"id": op_id})
            op = data.get("node") or {}
            status = op.get("status")
            if status == "COMPLETED":
                return op
            if status in ("FAILED", "CANCELED", "EXPIRED"):
                raise ShopifyApiError(f"bulk operation {op_id} {status}: {op.get('errorCode')}")
            if time.monotonic() > deadline:
                raise ShopifyApiError(f"bulk operation {op_id} still {status} after {timeout_s}s")
            time.sleep(BULK_POLL_S)

    def bulk(self, query: str, timeout_s: int = BULK_TIMEOUT_S) -> list[dict]:
        """Run ``query`` as a bulk operation and return the rebuilt root objects."""
        current = self._current_bulk()
        if current and current.get("status") in ("CREATED", "RUNNING", "CANCELING"):
            log.info("waiting for bulk operation %s already in flight", current["id"])
            self._wait_bulk(current["id"], timeout_s)
        data = self.query(
            "mutation($q: String!) { bulkOperationRunQuery(query: $q) { bulkOperation { id status } userErrors { field message } } }",
            {"q": query})
        result = data.get("bulkOperationRunQuery") or {}
        if result.get("userErrors"):
            raise ShopifyApiError("bulkOperationRunQuery: " + "; ".join(e["message"] for e in result["userErrors"]))
        op_id = result["bulkOperation"]["id"]
        t0 = time.monotonic()
        op = self._wait_bulk(op_id, timeout_s)
        log.info("bulk operation %s completed: %s objects in %.0fs", op_id, op.get("objectCount"), time.monotonic() - t0)
        if not op.get("url"):
            return []
        return rebuild(self._download_jsonl(op["url"]))

    def _download_jsonl(self, url: str) -> Iterator[dict]:
        for attempt in range(1, 4):
            try:
                resp = self.session.get(url, timeout=600, stream=True)
                resp.raise_for_status()
                break
            except (requests.exceptions.RequestException,) as exc:
                if attempt == 3:
                    raise
                log.warning("bulk download failed (%s); retrying", exc)
                time.sleep(5 * attempt)
        for line in resp.iter_lines():
            if line:
                yield json.loads(line)

    # ------------------------------------------------------------ resources
    def shop_info(self) -> dict:
        return self.query(SHOP_QUERY)["shop"]

    def orders(self, updated_at_min: str | None = None) -> list[dict]:
        """Every order (or those updated at/after ``updated_at_min``, ISO 8601 UTC), fully nested."""
        flt = f"updated_at:>={updated_at_min}" if updated_at_min else ""
        query = ORDERS_BULK_QUERY % {"filter": json.dumps(flt), "m": _M}
        orders = self.bulk(query)
        refunded = [o for o in orders if o.get("refunds")]
        for o in refunded:
            o["refunds"] = self.refunds(o["id"])
        log.info("orders: %d, refund detail fetched for %d", len(orders), len(refunded))
        return orders

    def refunds(self, order_id: str) -> list[dict]:
        """An order's refunds with their line items, shipping lines and transactions."""
        order = (self.query(REFUNDS_QUERY, {"id": order_id}) or {}).get("order") or {}
        return [unwrap_nodes(r) for r in order.get("refunds") or []]

    def products(self) -> list[dict]:
        """Every product with its variants, each variant with inventory by location."""
        return self.bulk(PRODUCTS_BULK_QUERY)

    def payouts(self) -> Iterator[dict]:
        """Shopify Payments payouts, newest first. Empty if the shop does not use Shopify Payments."""
        yield from self.paginate(PAYOUTS_QUERY, "shopifyPaymentsAccount.payouts")


def access_token_from_client_credentials(shop: str, client_id: str, client_secret: str,
                                         session: requests.Session | None = None) -> str:
    """Exchange the app's client id and secret for a 24-hour Admin API access token."""
    resp = (session or requests).post(
        f"https://{shop}.myshopify.com/admin/oauth/access_token",
        data={"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret},
        timeout=60)
    if resp.status_code != 200:
        raise ShopifyApiError(f"client credentials grant failed: HTTP {resp.status_code} {resp.text[:300]}")
    body = resp.json()
    token = body.get("access_token")
    if not token:
        raise ShopifyApiError(f"client credentials grant returned no access_token: {str(body)[:300]}")
    log.info("Shopify access token minted for %s (expires in %ss)", shop, body.get("expires_in"))
    return token


def shopify_client_from_secrets(shop: str = DEFAULT_SHOP, secret_ids: dict[str, str] | None = None) -> ShopifyClient:
    from pipelines.lib.secrets import get_secret, preflight

    ids = {**SECRET_IDS, **(secret_ids or {})}
    preflight(ids.values())
    session = requests.Session()
    token = access_token_from_client_credentials(shop, get_secret(ids["client_id"]).strip(),
                                                 get_secret(ids["client_secret"]).strip(), session)
    return ShopifyClient(shop, token, session=session)
