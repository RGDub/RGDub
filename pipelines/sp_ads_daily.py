"""Daily Sponsored Products loader for punlabs.AMZSales.sp_performance_master.

Replaces the hand-run notebook cells that have fed the table since May 2026.
Same shape as what runs today (LOAD into ``_sp_load_staging``, then a
transactional DELETE + INSERT over a trailing window) with three fixes:

* the report carries ``adGroupId`` / ``adGroupName`` / ``adId`` so the table
  finally records its true grain (see docs/RUNBOOK.md §2);
* the trailing window is 31 days, matching the longest attribution column
  (``sales30d``) so restated conversions are always picked up;
* the run is guarded: credentials are preflighted, the staging load is
  sanity-checked before it touches the master table, and every run writes a
  heartbeat row.

Notebook usage (first cell)::

    from pipelines.sp_ads_daily import run
    run()

CLI::

    python -m pipelines.sp_ads_daily                 # trailing 31 days
    python -m pipelines.sp_ads_daily --start 2026-08-01 --end 2026-08-31
    python -m pipelines.sp_ads_daily --dry-run       # download + validate only

Run ``sql/migrations/2026-09-18_sp_performance_master_add_adgroup.sql`` once
before the first run.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import re
from collections import Counter
from typing import Iterable

from pipelines.lib.ads_api import AdsApiClient, ads_client_from_secrets, chunk_date_range
from pipelines.lib.heartbeat import run_logged

log = logging.getLogger(__name__)

PROJECT = "punlabs"
DATASET = "AMZSales"
MASTER_TABLE = f"{PROJECT}.{DATASET}.sp_performance_master"
STAGING_TABLE = f"{PROJECT}.{DATASET}._sp_load_staging"

WINDOW_DAYS = 31          # longest attribution window in the table is 30d
REPORT_TIMEOUT_S = 3600   # per 31-day report chunk
REPORT_LAG_DAYS = 1       # Amazon's daily report for D is complete on D+1

# Everything the existing table stores, plus the three grain columns.
ATTRIBUTION_WINDOWS = ("1d", "7d", "14d", "30d")
METRIC_FAMILIES = (
    "sales", "purchases", "unitsSoldClicks",
    "attributedSalesSameSku", "purchasesSameSku", "unitsSoldSameSku",
)
REPORT_COLUMNS = [
    "date",
    "campaignName", "campaignId", "campaignStatus",
    "campaignBudgetAmount", "campaignBudgetType", "campaignBudgetCurrencyCode",
    "adGroupName", "adGroupId", "adId",
    "advertisedAsin", "advertisedSku",
    "impressions", "clicks", "cost", "spend",
    *[f"{m}{w}" for m in METRIC_FAMILIES for w in ATTRIBUTION_WINDOWS],
]

# BigQuery types for the staging load. Mirrors the master table exactly so the
# INSERT ... SELECT below can list columns by name.
INT_COLS = {"campaignId", "adGroupId", "adId", "impressions", "clicks",
            *[f"{m}{w}" for m in ("purchases", "unitsSoldClicks", "purchasesSameSku", "unitsSoldSameSku")
              for w in ATTRIBUTION_WINDOWS]}
FLOAT_COLS = {"campaignBudgetAmount", "cost", "spend",
              *[f"{m}{w}" for m in ("sales", "attributedSalesSameSku") for w in ATTRIBUTION_WINDOWS]}
TABLE_COLUMNS = [*REPORT_COLUMNS, "Parent SKU"]

GRAIN = ("date", "campaignId", "adGroupId", "advertisedSku")

_SKU_SUFFIXES = re.compile(r"-(FBA|FBM|UPC|CORR)(?=-|$)")


def parent_sku(sku: str | None) -> str | None:
    """Strip fulfilment / listing suffixes: ``FAIRYTL-CPNCLS-FBA-UPC`` -> ``FAIRYTL-CPNCLS``.

    Same rule the May 2026 backfill used, applied to every position rather than
    only the end so multi-suffix SKUs collapse correctly.
    """
    if sku is None:
        return None
    return _SKU_SUFFIXES.sub("", sku)


def report_body(start: dt.date, end: dt.date) -> dict:
    return {
        "name": f"sp_performance_master {start}..{end}",
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "reportTypeId": "spAdvertisedProduct",
            "groupBy": ["advertiser"],
            "columns": REPORT_COLUMNS,
            "timeUnit": "DAILY",
            "format": "GZIP_JSON",
        },
    }


def _coerce(col: str, value):
    if value is None or value == "":
        return None
    if col in INT_COLS:
        return int(value)
    if col in FLOAT_COLS:
        return float(value)
    return str(value)


def transform(rows: Iterable[dict]) -> list[dict]:
    """Report rows -> staging rows with the table's exact column set and types."""
    out = []
    for raw in rows:
        row = {col: _coerce(col, raw.get(col)) for col in REPORT_COLUMNS}
        row["Parent SKU"] = parent_sku(row["advertisedSku"])
        out.append(row)
    return out


def check_grain(rows: list[dict]) -> None:
    """Fail if the report repeats the full key. That would mean the grain is still wrong."""
    counts = Counter(tuple(r[k] for k in GRAIN) for r in rows)
    dupes = [(k, n) for k, n in counts.items() if n > 1]
    if dupes:
        sample = ", ".join(f"{k} x{n}" for k, n in dupes[:5])
        raise RuntimeError(
            f"{len(dupes)} repeated {GRAIN} keys in the report; refusing to load. First: {sample}"
        )


def fetch(client: AdsApiClient, start: dt.date, end: dt.date) -> list[dict]:
    rows: list[dict] = []
    for chunk_start, chunk_end in chunk_date_range(start, end, max_days=31):
        # Amazon's generation time for a 31-day report swings from 8 to 40+
        # minutes depending on time of day; the 07:00 ET run has seen >30.
        rows.extend(client.run_report(report_body(chunk_start, chunk_end), timeout_s=REPORT_TIMEOUT_S))
    return transform(rows)


# ----------------------------------------------------------------- BigQuery
def _bq_schema():
    from google.cloud import bigquery

    def typ(col):
        if col in INT_COLS:
            return "INT64"
        if col in FLOAT_COLS:
            return "FLOAT64"
        return "STRING"

    return [bigquery.SchemaField(col, typ(col)) for col in TABLE_COLUMNS]


def load_staging(bq, rows: list[dict]) -> int:
    from google.cloud import bigquery

    job = bq.load_table_from_json(
        rows,
        STAGING_TABLE,
        job_config=bigquery.LoadJobConfig(
            schema=_bq_schema(),
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    job.result()
    return job.output_rows or 0


def _quoted(cols: Iterable[str]) -> str:
    return ", ".join(f"`{c}`" for c in cols)


def guard_against_partial_load(bq, start: dt.date, end: dt.date, staged_rows: int) -> None:
    """Refuse to replace a window with something much smaller than what is there.

    A truncated or empty report must never wipe 31 days of history.
    """
    existing = list(bq.query(
        f"SELECT COUNT(*) AS n FROM `{MASTER_TABLE}` WHERE date BETWEEN '{start}' AND '{end}'"
    ).result())[0].n
    if existing and staged_rows < 0.5 * existing:
        raise RuntimeError(
            f"Staging has {staged_rows} rows but master already has {existing} for {start}..{end}; "
            "report looks truncated. Not replacing the window."
        )


def replace_window(bq, start: dt.date, end: dt.date) -> None:
    cols = _quoted(TABLE_COLUMNS)
    sql = f"""
    BEGIN TRANSACTION;
      DELETE FROM `{MASTER_TABLE}` WHERE date BETWEEN '{start}' AND '{end}';
      INSERT INTO `{MASTER_TABLE}` ({cols})
      SELECT {cols} FROM `{STAGING_TABLE}`;
    COMMIT TRANSACTION;
    """
    bq.query(sql).result()


def default_window(today: dt.date | None = None) -> tuple[dt.date, dt.date]:
    today = today or dt.date.today()
    end = today - dt.timedelta(days=REPORT_LAG_DAYS)
    return end - dt.timedelta(days=WINDOW_DAYS - 1), end


def run(start: dt.date | None = None, end: dt.date | None = None, *, dry_run: bool = False,
        client: AdsApiClient | None = None) -> int:
    """Pull the window from the Ads API and replace it in the master table. Returns rows loaded."""
    if start is None or end is None:
        start, end = default_window()
    with run_logged("sp_ads_daily", enabled=not dry_run) as ctx:
        client = client or ads_client_from_secrets()
        rows = fetch(client, start, end)
        if not rows:
            raise RuntimeError(f"Report for {start}..{end} returned zero rows; not touching the table.")
        check_grain(rows)
        days = {r["date"] for r in rows}
        log.info("fetched %d rows across %d days (%s..%s)", len(rows), len(days), min(days), max(days))
        if dry_run:
            return len(rows)

        from google.cloud import bigquery

        bq = bigquery.Client(project=PROJECT)
        staged = load_staging(bq, rows)
        guard_against_partial_load(bq, start, end, staged)
        replace_window(bq, start, end)
        ctx.rows_written = staged
        log.info("replaced %s..%s in %s with %d rows", start, end, MASTER_TABLE, staged)
        return staged


def _cli() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=dt.date.fromisoformat)
    parser.add_argument("--end", type=dt.date.fromisoformat)
    parser.add_argument("--dry-run", action="store_true", help="fetch and validate, do not write")
    parser.add_argument("--region", default="NA")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if (args.start is None) != (args.end is None):
        parser.error("--start and --end must be given together")
    run(args.start, args.end, dry_run=args.dry_run, client=ads_client_from_secrets(region=args.region))


if __name__ == "__main__":
    _cli()
