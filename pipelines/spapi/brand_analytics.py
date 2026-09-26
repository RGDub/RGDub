"""Brand Analytics weekly reports -> punlabs.AMZSales.ba_search_catalog_perf / ba_search_query_perf.

Search Catalog Performance (SCP): the search funnel per ASIN per week
(impressions, clicks, cart adds, purchases). Search Query Performance (SQP):
the same funnel per ASIN per customer search query, with this ASIN's share of
each step against the whole query.

Rules learned live on 2026-09-24:
* weeks must be Amazon's Sunday..Saturday; anything else ends FATAL;
* SQP needs an explicit ``asin`` report option (comma-separated); it is
  requested in batches, one report per batch;
* the latest complete week is usually available a few days after it ends, so
  the weekly run re-pulls the last ``LOOKBACK_WEEKS`` weeks and skips a week
  that is not published yet.

    python -m pipelines.spapi.brand_analytics                   # last 3 weeks, both reports (Tuesdays only)
    python -m pipelines.spapi.brand_analytics --backfill 52     # last 52 weeks
    python -m pipelines.spapi.brand_analytics --scp-only / --sqp-only
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import time

from pipelines.lib import bq as bqlib
from pipelines.lib.heartbeat import run_logged
from pipelines.lib.spapi import ReportFailed, SpApiClient, spapi_client_from_secrets

log = logging.getLogger(__name__)

SCP_TABLE = "punlabs.AMZSales.ba_search_catalog_perf"
SQP_TABLE = "punlabs.AMZSales.ba_search_query_perf"
SCP_REPORT = "GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT"
SQP_REPORT = "GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT"
LOOKBACK_WEEKS = 3
SQP_BATCH = 15          # ASINs per SQP request: the asin option is capped at 200 characters (15 x 10 + 14 commas = 164)
PUBLISH_LAG_DAYS = 3    # a week is requested only once it ended at least this long ago
RUN_WEEKDAY = 1         # Tuesday: the scheduled task fires daily but only loads on this day (0 = Monday)


# ------------------------------------------------------------------ weeks
def last_complete_weeks(n: int, today: dt.date | None = None) -> list[tuple[dt.date, dt.date]]:
    """The ``n`` most recent Sunday..Saturday weeks that ended at least PUBLISH_LAG_DAYS ago, oldest first."""
    today = today or dt.date.today()
    latest_end = today - dt.timedelta(days=PUBLISH_LAG_DAYS)
    sat = latest_end - dt.timedelta(days=(latest_end.weekday() - 5) % 7)   # most recent Saturday on/before latest_end
    weeks = []
    for i in range(n):
        end = sat - dt.timedelta(weeks=i)
        weeks.append((end - dt.timedelta(days=6), end))
    return list(reversed(weeks))


def _bounds(week: tuple[dt.date, dt.date]) -> tuple[dt.datetime, dt.datetime]:
    sun, sat = week
    assert sun.weekday() == 6 and sat.weekday() == 5 and (sat - sun).days == 6, week
    return (dt.datetime.combine(sun, dt.time.min, tzinfo=dt.timezone.utc),
            dt.datetime.combine(sat, dt.time(23, 59, 59), tzinfo=dt.timezone.utc))


# --------------------------------------------------------------- flatten
def _amt(obj) -> float | None:
    return None if not obj else obj.get("amount")


def flatten_scp(rec: dict, loaded_at: dt.datetime) -> dict:
    imp, clk, cart, pur = (rec.get(k) or {} for k in ("impressionData", "clickData", "cartAddData", "purchaseData"))
    return {
        "week_start": rec["startDate"], "week_end": rec["endDate"], "asin": rec["asin"],
        "impressions": imp.get("impressionCount"), "impression_median_price": _amt(imp.get("impressionMedianPrice")),
        "same_day_impressions": imp.get("sameDayShippingImpressionCount"),
        "one_day_impressions": imp.get("oneDayShippingImpressionCount"),
        "two_day_impressions": imp.get("twoDayShippingImpressionCount"),
        "clicks": clk.get("clickCount"), "click_rate": clk.get("clickRate"),
        "clicked_median_price": _amt(clk.get("clickedMedianPrice")),
        "same_day_clicks": clk.get("sameDayShippingClickCount"), "one_day_clicks": clk.get("oneDayShippingClickCount"),
        "two_day_clicks": clk.get("twoDayShippingClickCount"),
        "cart_adds": cart.get("cartAddCount"), "cart_added_median_price": _amt(cart.get("cartAddedMedianPrice")),
        "same_day_cart_adds": cart.get("sameDayShippingCartAddCount"),
        "one_day_cart_adds": cart.get("oneDayShippingCartAddCount"),
        "two_day_cart_adds": cart.get("twoDayShippingCartAddCount"),
        "purchases": pur.get("purchaseCount"), "search_traffic_sales": _amt(pur.get("searchTrafficSales")),
        "conversion_rate": pur.get("conversionRate"), "purchase_median_price": _amt(pur.get("purchaseMedianPrice")),
        "same_day_purchases": pur.get("sameDayShippingPurchaseCount"),
        "one_day_purchases": pur.get("oneDayShippingPurchaseCount"),
        "two_day_purchases": pur.get("twoDayShippingPurchaseCount"),
        "currency": (imp.get("impressionMedianPrice") or {}).get("currencyCode"),
        "loaded_at": loaded_at.isoformat(), "payload": rec,
    }


def flatten_sqp(rec: dict, loaded_at: dt.datetime) -> dict:
    """SQP record -> row. Amazon's shares and rates are percentages (0-100), stored as delivered."""
    q, imp, clk, cart, pur = (rec.get(k) or {} for k in ("searchQueryData", "impressionData", "clickData", "cartAddData", "purchaseData"))
    return {
        "week_start": rec["startDate"], "week_end": rec["endDate"], "asin": rec["asin"],
        "search_query": q.get("searchQuery"), "search_query_score": q.get("searchQueryScore"),
        "search_query_volume": q.get("searchQueryVolume"),
        "impressions_total": imp.get("totalQueryImpressionCount"), "impressions_asin": imp.get("asinImpressionCount"),
        "impressions_asin_share": imp.get("asinImpressionShare"),
        "clicks_total": clk.get("totalClickCount"), "clicks_asin": clk.get("asinClickCount"),
        "clicks_asin_share": clk.get("asinClickShare"), "click_rate_total": clk.get("totalClickRate"),
        "click_median_price_total": _amt(clk.get("totalMedianClickPrice")),
        "click_median_price_asin": _amt(clk.get("asinMedianClickPrice")),
        "cart_adds_total": cart.get("totalCartAddCount"), "cart_adds_asin": cart.get("asinCartAddCount"),
        "cart_adds_asin_share": cart.get("asinCartAddShare"), "cart_add_rate_total": cart.get("totalCartAddRate"),
        "purchases_total": pur.get("totalPurchaseCount"), "purchases_asin": pur.get("asinPurchaseCount"),
        "purchases_asin_share": pur.get("asinPurchaseShare"), "purchase_rate_total": pur.get("totalPurchaseRate"),
        "purchase_median_price_total": _amt(pur.get("totalMedianPurchasePrice")),
        "purchase_median_price_asin": _amt(pur.get("asinMedianPurchasePrice")),
        "loaded_at": loaded_at.isoformat(), "payload": rec,
    }


# ------------------------------------------------------------------ fetch
def fetch_scp(sp: SpApiClient, week: tuple[dt.date, dt.date]) -> list[dict]:
    start, end = _bounds(week)
    data = json.loads(sp.run_report(SCP_REPORT, start, end, report_options={"reportPeriod": "WEEK"}, timeout_s=1500))
    return data.get("dataByAsin", [])


def fetch_sqp(sp: SpApiClient, week: tuple[dt.date, dt.date], asins: list[str]) -> list[dict]:
    start, end = _bounds(week)
    out: list[dict] = []
    for i in range(0, len(asins), SQP_BATCH):
        batch = asins[i:i + SQP_BATCH]
        data = json.loads(sp.run_report(SQP_REPORT, start, end,
                                        report_options={"reportPeriod": "WEEK", "asin": ",".join(batch)}, timeout_s=1500))
        out.extend(data.get("dataByAsin", []))
    return out


# ------------------------------------------------------------------- load
def _load(bq, table: str, rows: list[dict], week: tuple[dt.date, dt.date]) -> int:
    """Replace one week: DELETE its partition, then load. Skips both on empty."""
    from google.cloud import bigquery

    if not rows:
        return 0
    bqlib.delete_where(bq, table, f"week_start = DATE('{week[0]}')")
    job = bq.load_table_from_json(rows, table, job_config=bigquery.LoadJobConfig(
        schema=bq.get_table(table).schema, write_disposition=bigquery.WriteDisposition.WRITE_APPEND))
    job.result()
    return job.output_rows or 0


def run(*, weeks: list[tuple[dt.date, dt.date]] | None = None, scp: bool = True, sqp: bool = True,
        dry_run: bool = False, client: SpApiClient | None = None, pause_s: int = 20) -> dict[str, int]:
    """Load ``weeks`` (default: the last LOOKBACK_WEEKS complete weeks). Returns rows per table.

    The default (scheduled) run only does work on RUN_WEEKDAY. The Daily Activity
    task calls it every day, and pulling ~20 SQP reports daily alongside the other
    SP-API jobs was tripping the shared report quota (HTTP 429 on orders/traffic).
    """
    if weeks is None and dt.date.today().weekday() != RUN_WEEKDAY:
        log.info("Brand Analytics loads weekly (weekday %d); nothing to do today", RUN_WEEKDAY)
        return {"scp": 0, "sqp": 0}
    weeks = weeks or last_complete_weeks(LOOKBACK_WEEKS)
    totals = {"scp": 0, "sqp": 0}
    with run_logged("ba_weekly", enabled=not dry_run) as ctx:
        sp = client or spapi_client_from_secrets()
        bq = None if dry_run else bqlib.client()
        loaded_at = dt.datetime.now(dt.timezone.utc)
        for week in weeks:
            asins: list[str] = []
            if scp:
                try:
                    recs = fetch_scp(sp, week)
                except ReportFailed as exc:
                    log.warning("SCP %s..%s: %s (not published yet?)", week[0], week[1], exc)
                    continue
                rows = [flatten_scp(r, loaded_at) for r in recs]
                asins = sorted({r["asin"] for r in rows})
                log.info("SCP %s..%s: %d ASINs", week[0], week[1], len(rows))
                totals["scp"] += len(rows) if dry_run else _load(bq, SCP_TABLE, rows, week)
                time.sleep(pause_s)
            if sqp:
                if not asins:  # SQP only: take the ASINs already known for that week
                    asins = [r.asin for r in bq.query(
                        f"SELECT DISTINCT asin FROM `{SCP_TABLE}` WHERE week_start = DATE('{week[0]}')").result()] if bq else []
                if not asins:
                    log.warning("SQP %s..%s: no ASIN list; skipping", week[0], week[1])
                    continue
                try:
                    recs = fetch_sqp(sp, week, asins)
                except ReportFailed as exc:
                    log.warning("SQP %s..%s: %s", week[0], week[1], exc)
                    continue
                rows = [flatten_sqp(r, loaded_at) for r in recs]
                log.info("SQP %s..%s: %d ASIN x query rows", week[0], week[1], len(rows))
                totals["sqp"] += len(rows) if dry_run else _load(bq, SQP_TABLE, rows, week)
                time.sleep(pause_s)
        ctx.rows_written = totals["scp"] + totals["sqp"]
    return totals


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--backfill", type=int, metavar="WEEKS", help="load the last N complete weeks instead of the trailing window")
    ap.add_argument("--scp-only", action="store_true")
    ap.add_argument("--sqp-only", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    wk = last_complete_weeks(args.backfill) if args.backfill else None
    print(run(weeks=wk, scp=not args.sqp_only, sqp=not args.scp_only, dry_run=args.dry_run))
