"""Unsettled financial events -> punlabs.AMZSales.PL-AMZSales-PendingFinances.

Replaces notebook task ``PunData-DailyAMZPendingFinance``. Finds the last closed
settlement date, pulls every financial event posted since then, flattens order
charges, fees, refunds and service fees to one row per line, and fully replaces
the pending table. The SQL step ``UpdateAMZFinances`` rebuilds AMZFinances from
settlements plus this table.

Fix over the notebook: the Finances API pages at 100 events and the notebook
read only the first page, so the pending table was silently capped. This
follows NextToken to the end.

    from pipelines.spapi.finances import run
    run()
"""

from __future__ import annotations

import datetime as dt
import logging

import pandas as pd

from pipelines.lib import bq as bqlib
from pipelines.lib.heartbeat import run_logged
from pipelines.lib.spapi import SpApiClient, spapi_client_from_secrets

log = logging.getLogger(__name__)

SETTLEMENTS_TABLE = "punlabs.AMZSales.PL-AMZSales-AMZSettlements"
TABLE = "punlabs.AMZSales.PL-AMZSales-PendingFinances"
COLUMNS = ["amazon_order_id", "posted_date", "sku", "transaction_type", "description", "amount", "currency"]


def _money(obj: dict | None) -> tuple[float, str]:
    obj = obj or {}
    return obj.get("CurrencyAmount", 0.0), obj.get("CurrencyCode", "USD")


def flatten(events: dict) -> list[dict]:
    """One FinancialEvents page -> flat rows (same shape the notebook produced)."""
    rows: list[dict] = []

    def add(order_id, posted, sku, kind, description, money):
        amount, currency = _money(money)
        rows.append({"amazon_order_id": order_id, "posted_date": posted, "sku": sku, "transaction_type": kind,
                     "description": description, "amount": amount, "currency": currency})

    for ev in events.get("ShipmentEventList", []):
        for item in ev.get("ShipmentItemList", []):
            sku = item.get("SellerSKU", "Unknown")
            for c in item.get("ItemChargeList", []):
                add(ev.get("AmazonOrderId"), ev.get("PostedDate"), sku, "Order", c.get("ChargeType"), c.get("ChargeAmount"))
            for f in item.get("ItemFeeList", []):
                add(ev.get("AmazonOrderId"), ev.get("PostedDate"), sku, "Fee", f.get("FeeType"), f.get("FeeAmount"))
    for ev in events.get("RefundEventList", []):
        for item in ev.get("ShipmentItemAdjustmentList", []):
            sku = item.get("SellerSKU", "Unknown")
            for c in item.get("ItemChargeAdjustmentList", []):
                add(ev.get("AmazonOrderId"), ev.get("PostedDate"), sku, "Refund", c.get("ChargeType"), c.get("ChargeAmount"))
    for ev in events.get("ServiceFeeEventList", []):
        for f in ev.get("FeeList", []):
            add(ev.get("AmazonOrderId", "Non-Order Fee"), ev.get("CreationDate"), ev.get("SellerSKU", "N/A"),
                "ServiceFee", f.get("FeeType"), f.get("FeeAmount"))
    return rows


def gap(bq, now: dt.datetime | None = None) -> tuple[dt.datetime, dt.datetime]:
    """From the last closed settlement (or 3 days ago) to five minutes ago."""
    now = now or dt.datetime.now(dt.timezone.utc)
    safe_now = now - dt.timedelta(minutes=5)
    row = list(bq.query(
        f"SELECT MAX(settlement_end_date) AS last_settled FROM `{SETTLEMENTS_TABLE}` "
        "WHERE settlement_end_date < CURRENT_TIMESTAMP()"
    ).result())[0]
    last = row.last_settled
    if last is None or pd.isnull(last):
        last = now - dt.timedelta(days=3)
    if last.tzinfo is None:
        last = last.replace(tzinfo=dt.timezone.utc)
    if last >= safe_now:
        last = safe_now - dt.timedelta(days=3)
    return last, safe_now


def run(*, dry_run: bool = False, client: SpApiClient | None = None) -> int:
    with run_logged("sp_pending_finances_daily", enabled=not dry_run) as ctx:
        sp = client or spapi_client_from_secrets()
        bq = bqlib.client()
        start, end = gap(bq)
        log.info("financial events %s .. %s", start, end)
        rows: list[dict] = []
        pages = 0
        for page in sp.financial_events(start, end):
            pages += 1
            rows.extend(flatten(page))
        df = pd.DataFrame(rows, columns=COLUMNS)
        if not df.empty:
            df["posted_date"] = pd.to_datetime(df["posted_date"], utc=True)
        log.info("%d pending rows from %d page(s)", len(df), pages)
        if dry_run:
            return len(df)
        ctx.rows_written = bqlib.load_frame(bq, df, TABLE, write_disposition="WRITE_TRUNCATE")
        return ctx.rows_written


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run(dry_run="--dry-run" in sys.argv))
