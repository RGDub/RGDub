"""Daily price / Featured Offer snapshot -> punlabs.AMZSales.pricing_daily.

Uses the Product Pricing competitiveSummary batch endpoint (20 ASINs per
request) for the same ASIN universe as the catalog snapshot. Records the
featured offer (who holds the Buy Box and at what price), the lowest new
offer across all sellers, and our own lowest offer.

    python -m pipelines.spapi.pricing_snapshot
"""

from __future__ import annotations

import datetime as dt
import logging
import time

from pipelines.lib import bq as bqlib
from pipelines.lib.heartbeat import run_logged
from pipelines.lib.spapi import MARKETPLACE_US, SpApiClient, spapi_client_from_secrets
from pipelines.spapi.catalog_snapshot import asin_universe

log = logging.getLogger(__name__)

TABLE = "punlabs.AMZSales.pricing_daily"
SELLER_ID = "A1CKBWKC4RTGPE"     # Pun Labs
BATCH = 20                       # competitiveSummary batch limit


def _landed(offer: dict) -> float | None:
    price = (offer.get("listingPrice") or {}).get("amount")
    if price is None:
        return None
    ship = next(((s.get("price") or {}).get("amount", 0.0) for s in offer.get("shippingOptions") or []
                 if s.get("shippingOptionType") == "DEFAULT"), 0.0)
    return round(price + (ship or 0.0), 2)


def flatten(body: dict, snapshot_date: dt.date, loaded_at: dt.datetime) -> dict:
    featured = {}
    glance = None
    for opt in body.get("featuredBuyingOptions") or []:
        if opt.get("buyingOptionType") != "New":
            continue
        for seg in opt.get("segmentedFeaturedOffers") or []:
            segs = seg.get("featuredOfferSegments") or []
            default = next((s for s in segs if s.get("customerMembership") == "DEFAULT"), segs[0] if segs else {})
            featured = seg
            glance = (default.get("segmentDetails") or {}).get("glanceViewWeightPercentage")
            break
        if featured:
            break
    offers = []
    for block in body.get("lowestPricedOffers") or []:
        if (block.get("lowestPricedOffersInput") or {}).get("itemCondition") == "New":
            offers.extend(block.get("offers") or [])
    landed = [(o, _landed(o)) for o in offers if _landed(o) is not None]
    lowest = min(landed, key=lambda t: t[1], default=(None, None))
    ours = [p for o, p in landed if o.get("sellerId") == SELLER_ID]
    was = next(((r.get("price") or {}).get("amount") for r in body.get("referencePrices") or [] if r.get("name") == "WasPrice"), None)
    return {
        "snapshot_date": snapshot_date.isoformat(), "asin": body["asin"],
        "featured_price": (featured.get("listingPrice") or {}).get("amount"),
        "featured_shipping": next(((s.get("price") or {}).get("amount") for s in featured.get("shippingOptions") or []
                                   if s.get("shippingOptionType") == "DEFAULT"), None),
        "featured_seller_id": featured.get("sellerId"),
        "featured_is_ours": (featured.get("sellerId") == SELLER_ID) if featured else None,
        "featured_fulfillment": featured.get("fulfillmentType"),
        "featured_glance_view_pct": glance,
        "lowest_new_price": lowest[1], "lowest_new_seller_id": (lowest[0] or {}).get("sellerId"),
        "our_lowest_price": min(ours) if ours else None, "offer_count_new": len(offers),
        "was_price": was, "loaded_at": loaded_at.isoformat(), "payload": body,
    }


def fetch(sp: SpApiClient, asins: list[str], snapshot_date: dt.date, loaded_at: dt.datetime, pause_s: float = 31) -> list[dict]:
    rows = []
    for i in range(0, len(asins), BATCH):
        batch = asins[i:i + BATCH]
        resp = sp.request("POST", "/batches/products/pricing/2022-05-01/items/competitiveSummary", json_body={
            "requests": [{"asin": a, "marketplaceId": MARKETPLACE_US, "method": "GET",
                          "uri": "/products/pricing/2022-05-01/items/competitiveSummary",
                          "includedData": ["featuredBuyingOptions", "referencePrices", "lowestPricedOffers"]} for a in batch]}).json()
        for r in resp.get("responses", []):
            body = r.get("body") or {}
            if r.get("status", {}).get("statusCode", 200) != 200 or "asin" not in body:
                log.warning("pricing: no body for one ASIN (%s)", str(r.get("status"))[:120])
                continue
            rows.append(flatten(body, snapshot_date, loaded_at))
        if i + BATCH < len(asins):
            time.sleep(pause_s)   # competitiveSummary is heavily rate limited
    return rows


def run(*, dry_run: bool = False, client: SpApiClient | None = None, asins: list[str] | None = None) -> int:
    with run_logged("pricing_snapshot_daily", enabled=not dry_run) as ctx:
        sp = client or spapi_client_from_secrets()
        bq = bqlib.client()
        asins = asins or asin_universe(bq)
        today = dt.datetime.now(dt.timezone.utc)
        rows = fetch(sp, asins, today.date(), today)
        log.info("pricing snapshot %s: %d of %d ASINs; we hold the featured offer on %d",
                 today.date(), len(rows), len(asins), sum(1 for r in rows if r["featured_is_ours"]))
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
