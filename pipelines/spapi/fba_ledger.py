"""FBA inventory ledger summary -> punlabs.AMZSales.PL-AMZSales-INVLedger.

Replaces notebook task ``AMZSales-DailyINV-FBAINVLedger``. Requests yesterday's
GET_LEDGER_SUMMARY_VIEW_DATA report (daily grain, by country), renames the
report's underscored headers to the table's spaced ones, derives Parent SKU and
an in-stock flag, then deletes that Date and appends.

Fix over the notebook: it appended without deleting, so every manual re-run
since the August outage wrote the day twice.

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
    "Starting_Warehouse_Balance": "Starting Warehouse Balance",
    "In_Transit_Between_Warehouses": "In Transit Between Warehouses",
    "Customer_Shipments": "Shipments",
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


def run(*, dry_run: bool = False, client: SpApiClient | None = None, day: dt.date | None = None) -> int:
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
            return len(df)
        bq = bqlib.client()
        ctx.rows_written = bqlib.replace_window(bq, df, TABLE, where=f"Date = DATE('{day}')")
        return ctx.rows_written


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run(dry_run="--dry-run" in sys.argv))
