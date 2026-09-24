"""Sponsored Products search-term and placement reports -> BigQuery, daily.

Same mechanics as sp_ads_daily: one v3 report per 31-day chunk, a trailing
window re-pulled every run (attribution restates for ~14 days), the window
replaced in a transaction, guarded against a truncated report, heartbeat per
run. Column names were confirmed against live reports on 2026-09-24.

    python -m pipelines.ads_reports                        # both, trailing 14 days
    python -m pipelines.ads_reports --start 2026-06-21 --end 2026-09-22   # backfill (95-day lookback)
    python -m pipelines.ads_reports --only search_terms | placements
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
from collections import Counter

from pipelines.lib.ads_api import AdsApiClient, ads_client_from_secrets, chunk_date_range
from pipelines.lib.heartbeat import run_logged

log = logging.getLogger(__name__)

WINDOW_DAYS = 14
REPORT_LAG_DAYS = 1
COMMON_METRICS = ["impressions", "clicks", "cost", "purchases7d", "sales7d", "purchases14d", "sales14d"]

REPORTS = {
    "search_terms": {
        "table": "punlabs.AMZSales.ads_sp_search_term_daily",
        "pipeline": "ads_sp_search_terms_daily",
        "config": {"adProduct": "SPONSORED_PRODUCTS", "reportTypeId": "spSearchTerm", "groupBy": ["searchTerm"],
                   "timeUnit": "DAILY", "format": "GZIP_JSON",
                   "columns": ["date", "campaignId", "campaignName", "adGroupId", "adGroupName", "keywordId", "keyword",
                               "matchType", "searchTerm", *COMMON_METRICS]},
        "columns": {"date": "date", "campaignId": "campaign_id", "campaignName": "campaign_name", "adGroupId": "ad_group_id",
                    "adGroupName": "ad_group_name", "keywordId": "keyword_id", "keyword": "keyword", "matchType": "match_type",
                    "searchTerm": "search_term", "impressions": "impressions", "clicks": "clicks", "cost": "cost",
                    "purchases7d": "purchases_7d", "sales7d": "sales_7d", "purchases14d": "purchases_14d", "sales14d": "sales_14d"},
        "grain": ("date", "campaign_id", "ad_group_id", "keyword_id", "search_term"),
    },
    "placements": {
        "table": "punlabs.AMZSales.ads_sp_placement_daily",
        "pipeline": "ads_sp_placements_daily",
        "config": {"adProduct": "SPONSORED_PRODUCTS", "reportTypeId": "spCampaigns", "groupBy": ["campaign", "campaignPlacement"],
                   "timeUnit": "DAILY", "format": "GZIP_JSON",
                   "columns": ["date", "campaignId", "campaignName", "placementClassification", *COMMON_METRICS]},
        "columns": {"date": "date", "campaignId": "campaign_id", "campaignName": "campaign_name",
                    "placementClassification": "placement", "impressions": "impressions", "clicks": "clicks", "cost": "cost",
                    "purchases7d": "purchases_7d", "sales7d": "sales_7d", "purchases14d": "purchases_14d", "sales14d": "sales_14d"},
        "grain": ("date", "campaign_id", "placement"),
    },
}
INT_COLS = {"campaign_id", "ad_group_id", "keyword_id", "impressions", "clicks", "purchases_7d", "purchases_14d"}
FLOAT_COLS = {"cost", "sales_7d", "sales_14d"}


def default_window(today: dt.date | None = None) -> tuple[dt.date, dt.date]:
    today = today or dt.date.today()
    end = today - dt.timedelta(days=REPORT_LAG_DAYS)
    return end - dt.timedelta(days=WINDOW_DAYS - 1), end


def _coerce(col: str, value):
    if value is None or value == "":
        return None
    if col in INT_COLS:
        return int(value)
    if col in FLOAT_COLS:
        return float(value)
    return str(value)


def transform(kind: str, rows: list[dict], loaded_at: dt.datetime) -> list[dict]:
    mapping = REPORTS[kind]["columns"]
    out = []
    for raw in rows:
        row = {dst: _coerce(dst, raw.get(src)) for src, dst in mapping.items()}
        row["loaded_at"] = loaded_at.isoformat()
        out.append(row)
    return out


def check_grain(kind: str, rows: list[dict]) -> None:
    grain = REPORTS[kind]["grain"]
    dupes = [(k, n) for k, n in Counter(tuple(r[c] for c in grain) for r in rows).items() if n > 1]
    if dupes:
        raise RuntimeError(f"{kind}: {len(dupes)} repeated {grain} keys in the report; first: {dupes[:3]}")


def fetch(client: AdsApiClient, kind: str, start: dt.date, end: dt.date, loaded_at: dt.datetime) -> list[dict]:
    rows: list[dict] = []
    for a, b in chunk_date_range(start, end, max_days=31):
        body = {"name": f"{kind} {a}..{b}", "startDate": a.isoformat(), "endDate": b.isoformat(),
                "configuration": REPORTS[kind]["config"]}
        rows.extend(client.run_report(body, timeout_s=3600))
    return transform(kind, rows, loaded_at)


def replace_window(bq, kind: str, rows: list[dict], start: dt.date, end: dt.date) -> int:
    """Load to a temp table, then DELETE window + INSERT in one transaction; refuse if the
    report looks truncated against what the table already holds."""
    from google.cloud import bigquery

    table = REPORTS[kind]["table"]
    existing = list(bq.query(f"SELECT COUNT(*) AS n FROM `{table}` WHERE date BETWEEN '{start}' AND '{end}'").result())[0].n
    if existing and len(rows) < 0.5 * existing:
        raise RuntimeError(f"{kind}: report has {len(rows)} rows but the table holds {existing} for {start}..{end}; not replacing")
    staging = f"{table}__staging"
    job = bq.load_table_from_json(rows, staging, job_config=bigquery.LoadJobConfig(
        schema=bq.get_table(table).schema, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE))
    job.result()
    cols = ", ".join(f"`{f.name}`" for f in bq.get_table(table).schema)
    bq.query(f"""
    BEGIN TRANSACTION;
      DELETE FROM `{table}` WHERE date BETWEEN '{start}' AND '{end}';
      INSERT INTO `{table}` ({cols}) SELECT {cols} FROM `{staging}`;
    COMMIT TRANSACTION;""").result()
    return job.output_rows or 0


def run(kinds: list[str] | None = None, start: dt.date | None = None, end: dt.date | None = None, *,
        dry_run: bool = False, client: AdsApiClient | None = None) -> dict[str, int]:
    kinds = kinds or list(REPORTS)
    if start is None or end is None:
        start, end = default_window()
    client = client or ads_client_from_secrets()
    bq = None if dry_run else __import__("pipelines.lib.bq", fromlist=["client"]).client()
    totals = {}
    for kind in kinds:
        with run_logged(REPORTS[kind]["pipeline"], enabled=not dry_run) as ctx:
            loaded_at = dt.datetime.now(dt.timezone.utc)
            rows = fetch(client, kind, start, end, loaded_at)
            if not rows:
                raise RuntimeError(f"{kind}: report for {start}..{end} returned zero rows; not touching the table")
            check_grain(kind, rows)
            days = {r["date"] for r in rows}
            log.info("%s: %d rows across %d days (%s..%s)", kind, len(rows), len(days), min(days), max(days))
            totals[kind] = len(rows) if dry_run else replace_window(bq, kind, rows, start, end)
            ctx.rows_written = totals[kind]
    return totals


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", type=dt.date.fromisoformat)
    ap.add_argument("--end", type=dt.date.fromisoformat)
    ap.add_argument("--only", choices=list(REPORTS))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if (args.start is None) != (args.end is None):
        ap.error("--start and --end must be given together")
    print(run([args.only] if args.only else None, args.start, args.end, dry_run=args.dry_run))
