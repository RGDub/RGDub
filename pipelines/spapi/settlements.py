"""Settlement reports -> punlabs.AMZSales.PL-AMZSales-AMZSettlements.

Replaces notebook task ``PunData-DailyAMZSettlementCheck``. Amazon generates
settlement reports itself; we list the ones created in the last 15 days,
download each (cp1252, as Amazon emits them), label the summary row, forward
fill the settlement id and its dates, then delete those settlement ids and
append. Settlements never change once closed, so this is idempotent.

    from pipelines.spapi.settlements import run
    run()
"""

from __future__ import annotations

import datetime as dt
import io
import logging

import pandas as pd

from pipelines.lib import bq as bqlib
from pipelines.lib.heartbeat import run_logged
from pipelines.lib.spapi import SpApiClient, spapi_client_from_secrets

log = logging.getLogger(__name__)

TABLE = "punlabs.AMZSales.PL-AMZSales-AMZSettlements"
REPORT_TYPE = "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2"
LOOKBACK_DAYS = 15
DATE_COLUMNS = ("settlement_start_date", "settlement_end_date", "deposit_date", "posted_date", "posted_date_time")


def transform(texts: list[str]) -> pd.DataFrame:
    frames = [pd.read_csv(io.StringIO(t), sep="\t", dtype=str) for t in texts]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df.columns = df.columns.str.replace("-", "_")
    for col in ("amount", "total_amount"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "quantity_purchased" in df.columns:
        df["quantity_purchased"] = pd.to_numeric(df["quantity_purchased"], errors="coerce").astype("Int64")
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    if "total_amount" in df.columns:
        if "transaction_type" not in df.columns:
            df["transaction_type"] = None
        df.loc[df["total_amount"].notna(), "transaction_type"] = "Settlement"
    if "settlement_id" in df.columns:
        df["settlement_id"] = df["settlement_id"].ffill()
        for col in ("settlement_start_date", "settlement_end_date", "deposit_date"):
            if col in df.columns:
                df[col] = df.groupby("settlement_id")[col].transform(lambda s: s.ffill().bfill())
    return df


def run(*, dry_run: bool = False, client: SpApiClient | None = None) -> int:
    with run_logged("sp_settlements_daily") as ctx:
        sp = client or spapi_client_from_secrets()
        now = dt.datetime.now(dt.timezone.utc)
        reports = sp.list_reports([REPORT_TYPE], now - dt.timedelta(days=LOOKBACK_DAYS), now)
        log.info("%d settlement reports created in the last %d days", len(reports), LOOKBACK_DAYS)
        if not reports:
            return 0
        df = transform([sp.download_document(r["reportDocumentId"], encoding="cp1252") for r in reports])
        if df.empty:
            log.info("settlement reports contained no rows")
            return 0
        ids = df["settlement_id"].dropna().unique().tolist()
        log.info("%d rows across settlements %s", len(df), ids)
        if dry_run:
            return len(df)
        bq = bqlib.client()
        ctx.rows_written = bqlib.replace_window(bq, df, TABLE, where=f"settlement_id IN ({bqlib.sql_list(ids)})")
        return ctx.rows_written


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run(dry_run="--dry-run" in sys.argv))
