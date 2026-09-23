"""FBA stock by fulfillment center -> punlabs.AMZSales.fba_inventory_by_fc.

Same report as the country-level ledger (GET_LEDGER_SUMMARY_VIEW_DATA), with
``aggregateByLocation=FC``: one row per day, SKU, disposition and fulfillment
center, carrying that FC's starting balance, movements and ending balance.

Probed 2026-09-22:
* about 1,100 rows a day across ~140 FCs holding stock;
* per-SKU FC totals reconcile to the unit with the country report (09-12);
* FC-level days arrive about 10 days late. A request for a recent day returns
  an empty document, not an error, while the country report is next-day.

So each daily run requests a trailing window (LOOKBACK_DAYS) and replaces only
the dates Amazon actually returned. Days that are not published yet are left
alone and picked up by a later run; a short or empty report can never delete
history.

Runs from the Daily Inventory pipeline via ``pipelines.spapi.fba_ledger.run``
(so no notebook change was needed). Standalone and backfill:

    python -m pipelines.spapi.fba_inventory_by_fc                  # trailing window
    python -m pipelines.spapi.fba_inventory_by_fc --dry-run
    python -m pipelines.spapi.fba_inventory_by_fc --backfill --start 2025-03-01 --end 2026-09-21
"""

from __future__ import annotations

import datetime as dt
import io
import logging
import re
import time

import pandas as pd

from pipelines.lib import bq as bqlib
from pipelines.lib.heartbeat import run_logged
from pipelines.lib.sku import parent_sku_legacy
from pipelines.lib.spapi import SpApiClient, spapi_client_from_secrets

log = logging.getLogger(__name__)

TABLE = "punlabs.AMZSales.fba_inventory_by_fc"
REPORT_TYPE = "GET_LEDGER_SUMMARY_VIEW_DATA"
REPORT_OPTIONS = {"aggregateByLocation": "FC", "aggregatedByTimePeriod": "DAILY"}
LOOKBACK_DAYS = 21
KEY = ["date", "fnsku", "msku", "disposition", "fc"]

# Report headers, normalized (lowercase, non-alphanumerics -> "_"), to table columns.
# Normalizing first means both the spaced headers Amazon emits today and the
# underscored ones it used before map to the same place.
COLUMNS = {
    "date": "date",
    "fnsku": "fnsku",
    "asin": "asin",
    "msku": "msku",
    "title": "title",
    "disposition": "disposition",
    "location": "fc",
    "store": "store",
    "starting_warehouse_balance": "starting_balance",
    "in_transit_between_warehouses": "in_transit_between_warehouses",
    "receipts": "receipts",
    "customer_shipments": "customer_shipments",
    "customer_returns": "customer_returns",
    "vendor_returns": "vendor_returns",
    "warehouse_transfer_in_out": "warehouse_transfers",
    "found": "found",
    "lost": "lost",
    "damaged": "damaged",
    "disposed": "disposed",
    "other_events": "other_events",
    "ending_warehouse_balance": "ending_balance",
    "unknown_events": "unknown_events",
}
QUANTITIES = ["starting_balance", "in_transit_between_warehouses", "receipts", "customer_shipments",
              "customer_returns", "vendor_returns", "warehouse_transfers", "found", "lost", "damaged",
              "disposed", "other_events", "ending_balance", "unknown_events"]


def _norm(header: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", header.strip().lower()).strip("_")


def transform(tsv: str) -> pd.DataFrame:
    """Report text -> table-shaped frame. Empty text gives an empty frame."""
    if not tsv or not tsv.strip():
        return pd.DataFrame(columns=list(COLUMNS.values()) + ["parent_sku", "loaded_at"])
    raw = pd.read_csv(io.StringIO(tsv), sep="\t", dtype=str, keep_default_na=False)
    df = raw.rename(columns={c: COLUMNS[_norm(c)] for c in raw.columns if _norm(c) in COLUMNS})
    unknown = [c for c in raw.columns if _norm(c) not in COLUMNS]
    if unknown:
        log.warning("FC ledger has columns this loader does not map (ignored): %s", unknown)
    if "fc" not in df.columns:
        raise ValueError("FC ledger has no Location column; was aggregateByLocation=FC honored?")
    df["date"] = pd.to_datetime(df["date"], format="%m/%d/%Y", errors="coerce").dt.date
    if df["date"].isna().any():
        raise ValueError(f"FC ledger has {int(df['date'].isna().sum())} rows with an unreadable Date")
    for col in QUANTITIES:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int) if col in df else 0
    for col in ("store", "title", "asin"):
        if col in df:
            df[col] = df[col].mask(df[col] == "")  # blank -> missing, loaded as NULL
    df["parent_sku"] = df["msku"].map(parent_sku_legacy)
    df["loaded_at"] = pd.Timestamp.now(tz="UTC")
    dupes = int(df.duplicated(KEY).sum())
    if dupes:
        raise ValueError(f"FC ledger has {dupes} duplicate rows on {KEY}")
    return df


def window(today: dt.date | None = None, days: int = LOOKBACK_DAYS) -> tuple[dt.date, dt.date]:
    """The trailing ``days`` complete UTC days ending yesterday."""
    end = (today or dt.datetime.now(dt.timezone.utc).date()) - dt.timedelta(days=1)
    return end - dt.timedelta(days=days - 1), end


def fetch(sp: SpApiClient, start: dt.date, end: dt.date) -> pd.DataFrame:
    a = dt.datetime.combine(start, dt.time.min, tzinfo=dt.timezone.utc)
    b = dt.datetime.combine(end, dt.time(23, 59, 59), tzinfo=dt.timezone.utc)
    return transform(sp.run_report(REPORT_TYPE, a, b, timeout_s=1200, report_options=REPORT_OPTIONS))


def load(bq, df: pd.DataFrame, schema=None) -> int:
    """Replace exactly the dates present in ``df``. Nothing is deleted for dates Amazon did not return."""
    if df.empty:
        return 0
    dates = sorted(df["date"].unique())
    where = f"date IN ({', '.join(f'DATE({d.isoformat()!r})' for d in dates)})"
    return bqlib.replace_window(bq, df, TABLE, where=where, schema=schema)


def run(*, dry_run: bool = False, client: SpApiClient | None = None, today: dt.date | None = None) -> int:
    with run_logged("fba_inventory_by_fc", enabled=not dry_run) as ctx:
        sp = client or spapi_client_from_secrets()
        start, end = window(today)
        df = fetch(sp, start, end)
        if df.empty:
            log.warning("FC ledger %s..%s: Amazon returned no rows; nothing loaded", start, end)
            return 0
        log.info("FC ledger %s..%s: %d rows, days %s..%s, %d FCs", start, end, len(df),
                 min(df["date"]), max(df["date"]), df["fc"].nunique())
        if dry_run:
            return len(df)
        ctx.rows_written = load(bqlib.client(), df)
        return ctx.rows_written


def month_chunks(start: dt.date, end: dt.date, months: int = 1) -> list[tuple[dt.date, dt.date]]:
    """Split start..end on calendar-month boundaries, ``months`` months per chunk."""
    out, cur = [], start
    while cur <= end:
        nxt = cur.replace(day=1)
        for _ in range(months):
            nxt = (nxt + dt.timedelta(days=32)).replace(day=1)
        out.append((cur, min(end, nxt - dt.timedelta(days=1))))
        cur = nxt
    return out


def backfill(start: dt.date, end: dt.date, *, client: SpApiClient | None = None, pause_s: int = 60,
             months_per_report: int = 3, dry_run: bool = False) -> dict[str, list]:
    """Load ``start``..``end``, one report per ``months_per_report`` calendar months.

    Amazon keeps ~18 months. Ledger reports have their own rolling daily cap
    (on 2026-09-22 about 10 requests in 24 hours before HTTP 429 persisted for
    hours), so chunks are quarters by default and a failed chunk is reported,
    not retried in a loop. Re-run later with the failed range.
    """
    with run_logged("fba_inventory_by_fc_backfill", enabled=not dry_run) as ctx:
        sp = client or spapi_client_from_secrets()
        bq = None if dry_run else bqlib.client()
        schema = None if dry_run else bqlib.table_schema(bq, TABLE)
        loaded, empty, failed = [], [], []
        for a, b in month_chunks(start, end, months_per_report):
            try:
                df = fetch(sp, a, b)
                if df.empty:
                    log.warning("%s..%s: no rows at Amazon", a, b)
                    empty.append((a, b))
                else:
                    n = len(df) if dry_run else load(bq, df, schema=schema)
                    ctx.rows_written += n
                    loaded.append((a, b))
                    log.info("%s..%s: %d rows, %d days", a, b, n, df["date"].nunique())
            except Exception as exc:  # noqa: BLE001 - one bad month must not stop the rest
                log.error("%s..%s: %s", a, b, str(exc)[:200])
                failed.append((a, b))
            time.sleep(pause_s)
        log.info("backfill finished: %d months loaded, %d empty, %d failed", len(loaded), len(empty), len(failed))
        return {"loaded": loaded, "empty": empty, "failed": failed}


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--backfill", action="store_true")
    p.add_argument("--start", type=dt.date.fromisoformat)
    p.add_argument("--end", type=dt.date.fromisoformat)
    p.add_argument("--months-per-report", type=int, default=3)
    args = p.parse_args()
    if args.backfill:
        if not (args.start and args.end):
            p.error("--backfill needs --start and --end")
        print(backfill(args.start, args.end, months_per_report=args.months_per_report, dry_run=args.dry_run))
    else:
        print(run(dry_run=args.dry_run))
