"""Sales and Traffic report by SKU -> punlabs.AMZSales.DailyTraffic.

Replaces notebook task ``PundData-DailyTraffic``. One report per day for the
trailing 7 days, because the ``salesAndTrafficByAsin`` section of this report
is aggregated over the requested range and carries no date of its own. Each day
is deleted and re-appended. Unlike the notebook, a day that fails is a failure
of the run, not a printed line: a missing day must never look like a quiet day.

    from pipelines.spapi.traffic import run
    run()
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import time
import zoneinfo

import pandas as pd

from pipelines.lib import bq as bqlib
from pipelines.lib.heartbeat import run_logged
from pipelines.lib.sku import parent_sku_legacy
from pipelines.lib.spapi import SpApiClient, spapi_client_from_secrets

log = logging.getLogger(__name__)

TABLE = "punlabs.AMZSales.DailyTraffic"
REPORT_TYPE = "GET_SALES_AND_TRAFFIC_REPORT"
LOOKBACK_DAYS = 7
KEY_COLUMNS = ("date", "parentAsin", "childAsin", "sku", "Parent SKU")
PACIFIC = zoneinfo.ZoneInfo("America/Los_Angeles")   # the marketplace's business day


def days(today: dt.date | None = None) -> list[dt.date]:
    """The 7 complete Pacific days before today (Pacific)."""
    today = today or dt.datetime.now(PACIFIC).date()
    return [today - dt.timedelta(days=i) for i in range(LOOKBACK_DAYS, 0, -1)]


def transform(report_json: str | dict, day: dt.date) -> pd.DataFrame:
    data = json.loads(report_json) if isinstance(report_json, str) else report_json
    rows = data.get("salesAndTrafficByAsin", [])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    parts = [df.drop(["salesByAsin", "trafficByAsin"], axis=1, errors="ignore")]
    for nested in ("salesByAsin", "trafficByAsin"):
        if nested in df.columns:
            parts.append(df[nested].apply(pd.Series))
    df = pd.concat(parts, axis=1)
    for col in df.columns:  # {"amount": 12.3, "currencyCode": "USD"} -> 12.3
        if df[col].map(lambda v: isinstance(v, dict)).any():
            df[col] = df[col].map(lambda v: v.get("amount") if isinstance(v, dict) else v)
    df.insert(0, "date", day)
    if "sku" in df.columns:
        df["Parent SKU"] = df["sku"].map(parent_sku_legacy)
    for col in df.columns:
        if col not in KEY_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def run(*, dry_run: bool = False, client: SpApiClient | None = None, pause_s: int = 45,
        dates: list[dt.date] | None = None) -> int:
    """Load the trailing 7 Pacific days, or exactly ``dates`` (backfill)."""
    with run_logged("sp_traffic_daily", enabled=not dry_run) as ctx:
        sp = client or spapi_client_from_secrets()
        bq = None if dry_run else bqlib.client()
        schema = None if dry_run else bqlib.table_schema(bq, TABLE)
        total = 0
        window = dates or days()
        for i, day in enumerate(window):
            start = dt.datetime.combine(day, dt.time.min, tzinfo=dt.timezone.utc)
            end = dt.datetime.combine(day, dt.time(23, 59, 59), tzinfo=dt.timezone.utc)
            text = sp.run_report(REPORT_TYPE, start, end,
                                 report_options={"dateGranularity": "DAY", "asinGranularity": "SKU"})
            df = transform(text, day)
            log.info("%s: %d SKU rows", day, len(df))
            if df.empty:
                if day == window[-1]:
                    # Amazon publishes a day's sales & traffic some hours after it
                    # closes; the most recent day may legitimately not exist yet.
                    # It is inside tomorrow's window, so skip it loudly rather
                    # than fail the whole run.
                    log.warning("%s: no ASIN rows yet (most recent day); will be picked up tomorrow", day)
                    continue
                raise RuntimeError(f"sales and traffic report for {day} had no ASIN rows; refusing to continue")
            if not dry_run:
                total += bqlib.replace_window(bq, df, TABLE, where=f"date = DATE('{day}')", schema=schema)
            else:
                total += len(df)
            if i < LOOKBACK_DAYS - 1:
                time.sleep(pause_s)
        ctx.rows_written = total
        return total


def missing_days(bq) -> list[dt.date]:
    """Dates between the table's first and last day that have no rows."""
    sql = f"""
    WITH span AS (SELECT MIN(date) mn, MAX(date) mx FROM `{TABLE}`),
    have AS (SELECT DISTINCT date FROM `{TABLE}`)
    SELECT d FROM span, UNNEST(GENERATE_DATE_ARRAY(span.mn, span.mx)) d
    LEFT JOIN have ON have.date = d WHERE have.date IS NULL ORDER BY d"""
    return [r.d for r in bq.query(sql).result()]


def backfill(dates: list[dt.date], *, client: SpApiClient | None = None, pause_s: int = 45,
             retries: int = 2) -> dict[str, list[dt.date]]:
    """Load exactly ``dates``, one report each. A failed day is logged and retried at
    the end; only days that still fail after ``retries`` passes are reported. Replaces
    the hand-edited PunData-HistDailyTrafficETL notebook.

        python -m pipelines.spapi.traffic --backfill --start 2026-06-17 --end 2026-07-20
        python -m pipelines.spapi.traffic --backfill --missing      # every gap in the table
    """
    with run_logged("sp_traffic_backfill") as ctx:
        sp = client or spapi_client_from_secrets()
        bq = bqlib.client()
        schema = bqlib.table_schema(bq, TABLE)
        done, empty, failed = [], [], list(dates)
        for attempt in range(1, retries + 2):
            todo, failed = failed, []
            if not todo:
                break
            log.info("backfill pass %d: %d day(s)", attempt, len(todo))
            for day in todo:
                try:
                    start = dt.datetime.combine(day, dt.time.min, tzinfo=dt.timezone.utc)
                    end = dt.datetime.combine(day, dt.time(23, 59, 59), tzinfo=dt.timezone.utc)
                    df = transform(sp.run_report(REPORT_TYPE, start, end, timeout_s=900,
                                                 report_options={"dateGranularity": "DAY", "asinGranularity": "SKU"}), day)
                    if df.empty:
                        log.warning("%s: Amazon returned no ASIN rows", day)
                        empty.append(day)
                    else:
                        ctx.rows_written += bqlib.replace_window(bq, df, TABLE, where=f"date = DATE('{day}')", schema=schema)
                        done.append(day)
                        log.info("%s: %d rows", day, len(df))
                except Exception as exc:  # noqa: BLE001 - one bad day must not stop the rest
                    log.error("%s: %s", day, str(exc)[:200])
                    failed.append(day)
                time.sleep(pause_s)
        log.info("backfill finished: %d loaded, %d empty at Amazon, %d failed", len(done), len(empty), len(failed))
        if failed:
            log.error("still missing after retries: %s", ", ".join(map(str, failed)))
        return {"loaded": done, "empty": empty, "failed": failed}


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--backfill", action="store_true", help="load explicit days instead of the trailing window")
    ap.add_argument("--start", type=dt.date.fromisoformat)
    ap.add_argument("--end", type=dt.date.fromisoformat)
    ap.add_argument("--missing", action="store_true", help="with --backfill: every day the table is missing")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.backfill:
        if args.missing:
            days_to_load = missing_days(bqlib.client())
        elif args.start and args.end:
            days_to_load = [args.start + dt.timedelta(days=i) for i in range((args.end - args.start).days + 1)]
        else:
            ap.error("--backfill needs --missing or --start/--end")
        print(backfill(days_to_load))
    else:
        print(run(dry_run=args.dry_run))
