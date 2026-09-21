"""All Orders report -> punlabs.AMZSales.PL-AMZSales-AMZTransactions.

Replaces notebook task ``PunDataPipe-AMZTrans``. Same behaviour: request the
GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL report for the trailing 30
days (UTC day boundaries, as before), derive Parent SKU, delete that purchase
date window from the table and append. The SQL step ``UpdateDailySales`` that
follows it in the pipeline is unchanged.

    from pipelines.spapi.orders import run
    run()
"""

from __future__ import annotations

import datetime as dt
import io
import logging

import pandas as pd

from pipelines.lib import bq as bqlib
from pipelines.lib.heartbeat import run_logged
from pipelines.lib.sku import parent_sku_legacy
from pipelines.lib.spapi import SpApiClient, spapi_client_from_secrets

log = logging.getLogger(__name__)

TABLE = "punlabs.AMZSales.PL-AMZSales-AMZTransactions"
REPORT_TYPE = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL"
LOOKBACK_DAYS = 30


def window(today: dt.datetime | None = None) -> tuple[dt.datetime, dt.datetime]:
    now = today or dt.datetime.now(dt.timezone.utc)
    start = (now - dt.timedelta(days=LOOKBACK_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = (now - dt.timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
    return start, end


def transform(tsv: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(tsv), sep="\t", dtype=str)
    if "sku" in df.columns:
        df["Parent SKU"] = df["sku"].map(parent_sku_legacy)
    return df


def run(*, dry_run: bool = False, client: SpApiClient | None = None) -> int:
    with run_logged("sp_orders_daily") as ctx:
        sp = client or spapi_client_from_secrets()
        start, end = window()
        df = transform(sp.run_report(REPORT_TYPE, start, end))
        log.info("orders report: %d rows for %s..%s", len(df), start.date(), end.date())
        if dry_run:
            return len(df)
        bq = bqlib.client()
        ctx.rows_written = bqlib.replace_window(
            bq, df, TABLE,
            where=f"DATE(`purchase-date`) BETWEEN '{start.date()}' AND '{end.date()}'",
        )
        return ctx.rows_written


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run(dry_run="--dry-run" in sys.argv))
