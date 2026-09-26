"""FBA inventory ledger summary -> punlabs.AMZSales.PL-AMZSales-INVLedger.

Replaces notebook task ``AMZSales-DailyINV-FBAINVLedger``. Requests yesterday's
GET_LEDGER_SUMMARY_VIEW_DATA report (daily grain, by country), renames the
report's underscored headers to the table's spaced ones, derives Parent SKU and
an in-stock flag, then deletes that Date and appends.

Fix over the notebook: it appended without deleting, so every manual re-run
since the August outage wrote the day twice.

The scheduled run also refreshes stock by fulfillment center
(``pipelines.spapi.fba_inventory_by_fc``) after the country ledger has loaded,
under its own heartbeat. Riding on this step means no notebook change was
needed. If the FC step fails, the country rows are already written and the
error is re-raised so the pipeline run shows red.

    from pipelines.spapi.fba_ledger import run
    run()
"""

from __future__ import annotations

import datetime as dt
import io
import logging

import numpy as np
import pandas as pd

from pipelines.lib import bq as bqlib
from pipelines.lib.heartbeat import run_logged
from pipelines.lib.sku import parent_sku_legacy
from pipelines.lib.spapi import SpApiClient, spapi_client_from_secrets

log = logging.getLogger(__name__)

TABLE = "punlabs.AMZSales.PL-AMZSales-INVLedger"
REPORT_TYPE = "GET_LEDGER_SUMMARY_VIEW_DATA"
RENAME = {
    # report header (as Amazon emits it today) -> table column
    "Customer Shipments": "Shipments",
    "Customer Returns": "CustomerReturns",
    "Vendor Returns": "VendorReturns",
    "Warehouse Transfer In/Out": "WhseTransfers",
    "Other Events": "Adjustments",
    # underscore variants the original notebook expected, kept in case Amazon flips back
    "Starting_Warehouse_Balance": "Starting Warehouse Balance",
    "In_Transit_Between_Warehouses": "In Transit Between Warehouses",
    "Customer_Shipments": "Shipments",
    "Customer_Returns": "CustomerReturns",
    "Vendor_Returns": "VendorReturns",
    "Warehouse_Transfer_In_Out": "WhseTransfers",
    "Other_Events": "Adjustments",
    "Ending_Warehouse_Balance": "Ending Warehouse Balance",
    "Unknown_Events": "Unknown Events",
}


def target_day(today: dt.date | None = None) -> dt.date:
    return (today or dt.datetime.now(dt.timezone.utc).date()) - dt.timedelta(days=1)


def transform(tsv: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(tsv), sep="\t").rename(columns=RENAME)
    df["Parent SKU"] = df["MSKU"].map(parent_sku_legacy) if "MSKU" in df.columns else "UNKNOWN"
    if "Ending Warehouse Balance" in df.columns:
        df["Inventory Binary"] = np.where(pd.to_numeric(df["Ending Warehouse Balance"], errors="coerce") > 0, 1, 0)
    else:
        df["Inventory Binary"] = 0
    return df


def run(*, dry_run: bool = False, client: SpApiClient | None = None, day: dt.date | None = None,
        include_fc: bool | None = None) -> int:
    """Load the country ledger for ``day`` (default yesterday).

    ``include_fc`` defaults to True for the scheduled run and False when a
    specific ``day`` is being re-run by hand.
    """
    if include_fc is None:
        include_fc = day is None
    rows, sp = _run_country(dry_run=dry_run, client=client, day=day)
    if include_fc:
        from pipelines.spapi import fba_inventory_by_fc

        fba_inventory_by_fc.run(dry_run=dry_run, client=sp)
    return rows


def _run_country(*, dry_run: bool, client: SpApiClient | None, day: dt.date | None) -> tuple[int, SpApiClient]:
    with run_logged("amz_fba_inv_ledger", enabled=not dry_run) as ctx:
        sp = client or spapi_client_from_secrets()
        day = day or target_day()
        start = dt.datetime.combine(day, dt.time.min, tzinfo=dt.timezone.utc)
        end = dt.datetime.combine(day, dt.time(23, 59, 59), tzinfo=dt.timezone.utc)
        df = transform(sp.run_report(REPORT_TYPE, start, end, timeout_s=600,
                                     report_options={"aggregateByLocation": "COUNTRY",
                                                     "aggregatedByTimePeriod": "DAILY"}))
        log.info("ledger for %s: %d rows", day, len(df))
        if dry_run:
            return len(df), sp
        bq = bqlib.client()
        ctx.rows_written = bqlib.replace_window(bq, df, TABLE, where=f"Date = DATE('{day}')")
        return ctx.rows_written, sp


GRAIN = ["Date", "FNSKU", "MSKU", "Disposition", "Location"]


def check_range(df: pd.DataFrame, start: dt.date, end: dt.date) -> tuple[dt.date, dt.date]:
    """Refuse a range pull with duplicate grain rows or missing days; return the span it covers."""
    days = pd.to_datetime(df["Date"]).dt.date
    dupes = int(df.assign(Date=days).duplicated(subset=[c for c in GRAIN if c in df.columns]).sum())
    if dupes:
        raise RuntimeError(f"range report has {dupes} duplicate grain rows; not replacing")
    first, last = days.min(), days.max()
    if first > start + dt.timedelta(days=7):
        raise RuntimeError(f"range report starts {first}, asked for {start}; not replacing")
    missing = sorted(set(pd.date_range(first, last).date) - set(days))
    if missing:
        raise RuntimeError(f"range report is missing {len(missing)} days ({missing[:5]}...); not replacing")
    return first, last


def repull_range(start: dt.date, end: dt.date, *, dry_run: bool = False, client: SpApiClient | None = None) -> int:
    """Re-pull the country ledger for ``start``..``end`` in ONE report and replace exactly the days it returns.

    One ledger request instead of one per day (the ledger report has a rolling
    ~10-per-24h cap). Used 2026-09-26 to clear the notebook's doubled days and
    the 2026-08-25..09-14 gap.
    """
    with run_logged("amz_fba_inv_ledger_repull", enabled=not dry_run) as ctx:
        sp = client or spapi_client_from_secrets()
        df = transform(sp.run_report(
            REPORT_TYPE,
            dt.datetime.combine(start, dt.time.min, tzinfo=dt.timezone.utc),
            dt.datetime.combine(end, dt.time(23, 59, 59), tzinfo=dt.timezone.utc),
            timeout_s=1800,
            report_options={"aggregateByLocation": "COUNTRY", "aggregatedByTimePeriod": "DAILY"}))
        first, last = check_range(df, start, end)
        log.info("ledger range %s..%s: %d rows over %d days", first, last, len(df), (last - first).days + 1)
        if dry_run:
            return len(df)
        ctx.rows_written = bqlib.replace_window(
            bqlib.client(), df, TABLE, where=f"Date BETWEEN DATE('{first}') AND DATE('{last}')")
        return ctx.rows_written


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--start", type=dt.date.fromisoformat, help="with --end: re-pull a date range in one report")
    ap.add_argument("--end", type=dt.date.fromisoformat)
    args = ap.parse_args()
    if (args.start is None) != (args.end is None):
        ap.error("--start and --end must be given together")
    if args.start:
        print(repull_range(args.start, args.end, dry_run=args.dry_run))
    else:
        print(run(dry_run=args.dry_run))
