"""Daily pull of Shopify orders, products and payouts into ``punlabs.ShopifySales``.

Orders are stored as the Admin API returns them (one JSON payload with line
items, discounts, shipping lines, refunds, transactions and fulfillments
nested), one row per version: an order whose ``updatedAt`` moves gets a new
row, so refunds and status changes are kept as history. Products are
snapshotted daily with every variant and its inventory by location. Shopify
Payments payouts are appended as they appear (one row per payout per status).
The views in sql/shopify/ddl.sql flatten all of it and reproduce Shopify's
"Sales by day" report columns.

Incremental: each run asks for orders updated since the newest ``updated_at``
already stored, less a two-day overlap, and appends only versions it has not
seen. The first run pulls the store's whole history (needs the
``read_all_orders`` scope; without it Shopify returns only the last 60 days).

    python -m pipelines.shopify.load             # load
    python -m pipelines.shopify.load --dry-run   # pull and count, write nothing
    python -m pipelines.shopify.load --full      # ignore the watermark, re-pull every order
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import uuid

from pipelines.lib import bq as bqlib
from pipelines.lib.heartbeat import run_logged
from pipelines.lib.shopify import ShopifyAccessDenied, ShopifyClient, money, shopify_client_from_secrets

log = logging.getLogger(__name__)

DATASET = "punlabs.ShopifySales"
ORDERS_RAW = f"{DATASET}.shopify_orders_raw"
PRODUCTS_RAW = f"{DATASET}.shopify_products_raw"
PAYOUTS_RAW = f"{DATASET}.shopify_payouts_raw"
OVERLAP = dt.timedelta(days=2)


def _ts(value: str | None) -> dt.datetime | None:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def order_row(o: dict, pulled_at: str, run_id: str) -> dict:
    return {
        "order_id": o["id"],
        "order_number": o.get("name"),
        "created_at": o.get("createdAt"),
        "updated_at": o.get("updatedAt"),
        "processed_at": o.get("processedAt"),
        "financial_status": o.get("displayFinancialStatus"),
        "fulfillment_status": o.get("displayFulfillmentStatus"),
        "test": o.get("test"),
        "pulled_at": pulled_at,
        "run_id": run_id,
        "payload": o,
    }


def product_row(p: dict, pulled_at: str, run_id: str) -> dict:
    return {
        "product_id": p["id"],
        "title": p.get("title"),
        "status": p.get("status"),
        "updated_at": p.get("updatedAt"),
        "pulled_at": pulled_at,
        "run_id": run_id,
        "payload": p,
    }


def payout_row(p: dict, pulled_at: str, run_id: str) -> dict:
    return {
        "payout_id": p["id"],
        "issued_at": p.get("issuedAt"),
        "status": p.get("status"),
        "transaction_type": p.get("transactionType"),
        "net": money(p.get("net")),
        "currency": (p.get("net") or {}).get("currencyCode"),
        "pulled_at": pulled_at,
        "run_id": run_id,
        "payload": p,
    }


def watermark(bq) -> str | None:
    """Newest order updated_at already stored, less the overlap, as ISO 8601 UTC. None on first run."""
    rows = list(bq.query(f"SELECT MAX(updated_at) AS m FROM `{ORDERS_RAW}`").result())
    if not rows or rows[0].m is None:
        return None
    return (rows[0].m - OVERLAP).strftime("%Y-%m-%dT%H:%M:%SZ")


def known_versions(bq, since: str | None) -> set[tuple[str, dt.datetime]]:
    where = f"WHERE updated_at >= TIMESTAMP('{since}')" if since else ""
    return {(r.order_id, r.updated_at) for r in bq.query(f"SELECT order_id, updated_at FROM `{ORDERS_RAW}` {where}").result()}


def known_payouts(bq) -> set[tuple[str, str]]:
    return {(r.payout_id, r.status) for r in bq.query(f"SELECT DISTINCT payout_id, status FROM `{PAYOUTS_RAW}`").result()}


def run(*, dry_run: bool = False, full: bool = False, client: ShopifyClient | None = None) -> int:
    with run_logged("shopify_daily", enabled=not dry_run) as ctx:
        shop = client or shopify_client_from_secrets()
        info = shop.shop_info()
        log.info("shop %s (%s), timezone %s, currency %s", info.get("name"), info.get("myshopifyDomain"),
                 info.get("ianaTimezone"), info.get("currencyCode"))
        bq = bqlib.client()
        pulled_at = dt.datetime.now(dt.timezone.utc).isoformat()
        run_id = str(uuid.uuid4())

        since = None if full else watermark(bq)
        orders = shop.orders(updated_at_min=since)
        seen = known_versions(bq, since) if not full else set()
        new_orders = [o for o in orders if (o["id"], _ts(o.get("updatedAt"))) not in seen]
        log.info("orders since %s: %d returned, %d new versions", since or "the beginning", len(orders), len(new_orders))

        products = shop.products()
        log.info("products: %d (%d variants)", len(products), sum(len(p.get("variants") or []) for p in products))

        try:
            payouts = list(shop.payouts())
        except ShopifyAccessDenied as exc:
            log.warning("payouts skipped, token lacks read_shopify_payments_payouts: %s", exc)
            payouts = []
        seen_payouts = known_payouts(bq)
        new_payouts = [p for p in payouts if (p["id"], p.get("status")) not in seen_payouts]
        log.info("payouts: %d returned, %d new", len(payouts), len(new_payouts))

        if dry_run:
            return len(new_orders) + len(products) + len(new_payouts)

        written = 0
        if new_orders:
            written += bqlib.load_json_rows(bq, [order_row(o, pulled_at, run_id) for o in new_orders], ORDERS_RAW)
        if products:
            written += bqlib.load_json_rows(bq, [product_row(p, pulled_at, run_id) for p in products], PRODUCTS_RAW)
        if new_payouts:
            written += bqlib.load_json_rows(bq, [payout_row(p, pulled_at, run_id) for p in new_payouts], PAYOUTS_RAW)
        ctx.rows_written = written
        return written


def _cli() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--full", action="store_true", help="ignore the watermark and re-pull every order")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run(dry_run=args.dry_run, full=args.full))


if __name__ == "__main__":
    _cli()
