"""Daily catalog snapshot -> punlabs.AMZSales.catalog_snapshot.

One getCatalogItem call per ASIN (images, summaries, sales ranks,
classifications, dimensions). The ASIN universe is everything that had
traffic, ad spend or search impressions recently, so a new listing appears the
day it starts getting seen. A change in main_image_url between two days is
how a listing-image change gets dated.

    python -m pipelines.spapi.catalog_snapshot
"""

from __future__ import annotations

import datetime as dt
import logging
import time

from pipelines.lib import bq as bqlib
from pipelines.lib.heartbeat import run_logged
from pipelines.lib.spapi import MARKETPLACE_US, SpApiClient, SpApiError, spapi_client_from_secrets

log = logging.getLogger(__name__)

TABLE = "punlabs.AMZSales.catalog_snapshot"
INCLUDED = "images,summaries,salesRanks,classifications,dimensions"
ASIN_UNIVERSE_SQL = """
SELECT DISTINCT asin FROM (
  SELECT childAsin AS asin FROM `punlabs.AMZSales.DailyTraffic` WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
  UNION ALL SELECT advertisedAsin FROM `punlabs.AMZSales.sp_performance_master` WHERE PARSE_DATE('%F', date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 95 DAY)
  UNION ALL SELECT asin FROM `punlabs.AMZSales.ba_search_catalog_perf`
) WHERE asin IS NOT NULL AND REGEXP_CONTAINS(asin, r'^B0[A-Z0-9]{8}$') ORDER BY asin"""


def asin_universe(bq) -> list[str]:
    return [r.asin for r in bq.query(ASIN_UNIVERSE_SQL).result()]


def _first(items: list | None, marketplace: str = MARKETPLACE_US) -> dict:
    for it in items or []:
        if it.get("marketplaceId") == marketplace:
            return it
    return (items or [{}])[0]


def flatten(item: dict, snapshot_date: dt.date, loaded_at: dt.datetime) -> dict:
    summary = _first(item.get("summaries"))
    images = _first(item.get("images")).get("images", [])
    main = next((i for i in images if i.get("variant") == "MAIN"), {})
    ranks = _first(item.get("salesRanks"))
    cls_rank = (ranks.get("classificationRanks") or [{}])[0]
    grp_rank = (ranks.get("displayGroupRanks") or [{}])[0]
    dims = (_first(item.get("dimensions")).get("item") or {})
    browse = summary.get("browseClassification") or {}
    return {
        "snapshot_date": snapshot_date.isoformat(), "asin": item["asin"],
        "title": summary.get("itemName"), "brand": summary.get("brand"),
        "browse_classification": browse.get("displayName"), "classification_id": browse.get("classificationId"),
        "main_image_url": main.get("link"), "main_image_width": main.get("width"), "main_image_height": main.get("height"),
        "image_count": len(images),
        "sales_rank_category": cls_rank.get("title"), "sales_rank": cls_rank.get("rank"),
        "display_group": grp_rank.get("title"), "display_group_rank": grp_rank.get("rank"),
        "item_length_in": (dims.get("length") or {}).get("value"), "item_width_in": (dims.get("width") or {}).get("value"),
        "item_height_in": (dims.get("height") or {}).get("value"),
        "loaded_at": loaded_at.isoformat(), "payload": item,
    }


def fetch(sp: SpApiClient, asins: list[str], snapshot_date: dt.date, loaded_at: dt.datetime, pause_s: float = 0.6) -> list[dict]:
    rows = []
    for asin in asins:
        try:
            item = sp.request("GET", f"/catalog/2022-04-01/items/{asin}",
                              params={"marketplaceIds": MARKETPLACE_US, "includedData": INCLUDED}).json()
        except SpApiError as exc:
            if exc.status == 404:
                log.warning("%s: not in the catalog any more (404); skipped", asin)
                continue
            raise
        rows.append(flatten(item, snapshot_date, loaded_at))
        time.sleep(pause_s)   # catalog items: 2 req/s
    return rows


def run(*, dry_run: bool = False, client: SpApiClient | None = None, asins: list[str] | None = None) -> int:
    with run_logged("catalog_snapshot_daily", enabled=not dry_run) as ctx:
        sp = client or spapi_client_from_secrets()
        bq = bqlib.client()
        asins = asins or asin_universe(bq)
        today = dt.datetime.now(dt.timezone.utc)
        rows = fetch(sp, asins, today.date(), today)
        log.info("catalog snapshot %s: %d of %d ASINs", today.date(), len(rows), len(asins))
        if dry_run or not rows:
            return len(rows)
        from google.cloud import bigquery

        bqlib.delete_where(bq, TABLE, f"snapshot_date = DATE('{today.date()}')")
        job = bq.load_table_from_json(rows, TABLE, job_config=bigquery.LoadJobConfig(
            schema=bq.get_table(TABLE).schema, write_disposition=bigquery.WriteDisposition.WRITE_APPEND))
        job.result()
        ctx.rows_written = job.output_rows or 0
        return ctx.rows_written


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run(dry_run="--dry-run" in sys.argv))
