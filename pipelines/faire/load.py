"""Daily pull of Faire orders and products into ``punlabs.FaireSales``.

Orders are stored as Faire returns them (JSON payload), one row per version:
an order whose ``updated_at`` moves gets a new row, so state changes and
payout restatements are kept. Products are stored the same way. The views in
sql/faire/ddl.sql flatten this into orders, order items, payouts and products.

Incremental: each run asks Faire for orders updated since the newest
``updated_at`` already stored, less a two-day overlap, and only appends versions
it has not seen. The first run pulls the brand's whole history (a few hundred
orders) the same way.

    python -m pipelines.faire.load             # load
    python -m pipelines.faire.load --dry-run   # pull and count, write nothing
    python -m pipelines.faire.load --full      # ignore the watermark, re-pull everything
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import uuid

from pipelines.lib import bq as bqlib
from pipelines.lib.faire import FaireClient, faire_client_from_secrets
from pipelines.lib.heartbeat import run_logged

log = logging.getLogger(__name__)

DATASET = "punlabs.FaireSales"
ORDERS_RAW = f"{DATASET}.faire_orders_raw"
PRODUCTS_RAW = f"{DATASET}.faire_products_raw"
OVERLAP = dt.timedelta(days=2)


def order_row(order: dict, pulled_at: str, run_id: str) -> dict:
    return {
        "order_id": order["id"],
        "display_id": order.get("display_id"),
        "state": order.get("state"),
        "created_at": order.get("created_at"),
        "updated_at": order.get("updated_at"),
        "payment_initiated_at": order.get("payment_initiated_at"),
        "retailer_id": order.get("retailer_id"),
        "source": order.get("source"),
        "pulled_at": pulled_at,
        "run_id": run_id,
        "payload": order,
    }


def product_row(product: dict, pulled_at: str, run_id: str) -> dict:
    return {
        "product_id": product["id"],
        "name": product.get("name"),
        "state": product.get("sale_state") or product.get("lifecycle_state"),
        "updated_at": product.get("updated_at"),
        "pulled_at": pulled_at,
        "run_id": run_id,
        "payload": product,
    }


def watermark(bq) -> str | None:
    """Newest order updated_at already stored, less the overlap, as ISO 8601. None on first run."""
    rows = list(bq.query(f"SELECT MAX(updated_at) AS m FROM `{ORDERS_RAW}`").result())
    if not rows or rows[0].m is None:
        return None
    return (rows[0].m - OVERLAP).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def known_versions(bq, since: str | None) -> set[tuple[str, str]]:
    where = f"WHERE updated_at >= TIMESTAMP('{since}')" if since else ""
    return {(r.order_id, r.updated_at.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
            for r in bq.query(f"SELECT order_id, updated_at FROM `{ORDERS_RAW}` {where}").result()}


def run(*, dry_run: bool = False, full: bool = False, client: FaireClient | None = None) -> int:
    with run_logged("faire_daily", enabled=not dry_run) as ctx:
        faire = client or faire_client_from_secrets()
        bq = bqlib.client()
        pulled_at = dt.datetime.now(dt.timezone.utc).isoformat()
        run_id = str(uuid.uuid4())

        since = None if full else watermark(bq)
        orders = list(faire.orders(updated_at_min=since))
        seen = known_versions(bq, since) if not full else set()
        new_orders = [o for o in orders if (o["id"], o.get("updated_at")) not in seen]
        log.info("orders since %s: %d returned, %d new versions", since or "the beginning", len(orders), len(new_orders))

        products = list(faire.products())
        log.info("products: %d", len(products))

        if dry_run:
            return len(new_orders) + len(products)

        written = 0
        if new_orders:
            written += bqlib.load_json_rows(bq, [order_row(o, pulled_at, run_id) for o in new_orders], ORDERS_RAW)
        if products:
            written += bqlib.load_json_rows(bq, [product_row(p, pulled_at, run_id) for p in products], PRODUCTS_RAW)
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
