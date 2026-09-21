"""Snapshot the full campaign structure into punlabs.AMZSales.ads_entity_log.

Marketing Stream's campaign-management datasets only deliver *changes*; there is
no initial state. This module pulls every campaign, ad group, ad and target for
the profile through the Ads API v1 query endpoints and appends them to the same
log the stream poller writes to, as ``source = 'snapshot'``. Run it once to seed
the log, and again whenever you want a known-good full state (a weekly run is
cheap: a few thousand rows).

Keys are converted from the API's camelCase to the stream's snake_case so the
views in sql/ads_stream/entity_log.sql read both sources identically.

    python -m pipelines.ads_entities            # snapshot -> BigQuery
    python -m pipelines.ads_entities --dry-run  # pull and count only
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import re
from typing import Any, Iterable

from pipelines.lib.ads_api import AdsApiClient, ads_client_from_secrets
from pipelines.lib.heartbeat import run_logged

log = logging.getLogger(__name__)

ENTITY_LOG_TABLE = "punlabs.AMZSales.ads_entity_log"

# v1 query kind -> (entity_type in the log, id field after snake_casing)
KINDS = {
    "campaigns": ("campaign", "campaign_id"),
    "adGroups": ("ad_group", "ad_group_id"),
    "ads": ("ad", "ad_id"),
    "targets": ("target", "target_id"),
}

_CAMEL = re.compile(r"(?<!^)(?=[A-Z])")


def to_snake(obj: Any) -> Any:
    """Recursively convert dict keys from camelCase to snake_case (values untouched)."""
    if isinstance(obj, dict):
        return {_CAMEL.sub("_", k).lower(): to_snake(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_snake(v) for v in obj]
    return obj


def entity_rows(kind: str, entities: Iterable[dict], observed_at: dt.datetime) -> list[dict]:
    entity_type, id_key = KINDS[kind]
    rows = []
    for raw in entities:
        rec = to_snake(raw)
        rows.append({
            "entity_type": entity_type,
            "entity_id": str(rec[id_key]),
            "campaign_id": str(rec["campaign_id"]) if rec.get("campaign_id") is not None else None,
            "ad_group_id": str(rec["ad_group_id"]) if entity_type != "campaign" and rec.get("ad_group_id") is not None else None,
            "ad_product": rec.get("ad_product"),
            "state": rec.get("state"),
            "observed_at": observed_at.isoformat(),
            "last_updated": rec.get("last_updated_date_time"),
            "source": "snapshot",
            "payload": rec,   # a dict, so the load job stores a JSON object, not a JSON string
        })
    return rows


def snapshot(client: AdsApiClient, ad_product: str = "SPONSORED_PRODUCTS") -> dict[str, list[dict]]:
    """Pull every entity kind. Returns {kind: rows}."""
    observed_at = dt.datetime.now(dt.timezone.utc)
    out = {}
    for kind in KINDS:
        entities = list(client.query_entities(kind, ad_product=ad_product))
        out[kind] = entity_rows(kind, entities, observed_at)
        log.info("%s: %d", kind, len(out[kind]))
    return out


def load(bq, rows: list[dict]) -> int:
    from google.cloud import bigquery

    job = bq.load_table_from_json(
        rows, ENTITY_LOG_TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND),
    )
    job.result()
    return job.output_rows or 0


def run(*, dry_run: bool = False, client: AdsApiClient | None = None, ad_product: str = "SPONSORED_PRODUCTS") -> int:
    with run_logged("ads_entities_snapshot", enabled=not dry_run) as ctx:
        client = client or ads_client_from_secrets()
        pulled = snapshot(client, ad_product=ad_product)
        rows = [r for kind_rows in pulled.values() for r in kind_rows]
        if not rows:
            raise RuntimeError("v1 query returned no entities at all; not writing an empty snapshot")
        if dry_run:
            return len(rows)
        from google.cloud import bigquery

        ctx.rows_written = load(bigquery.Client(project="punlabs"), rows)
        return ctx.rows_written


def _cli() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ad-product", default="SPONSORED_PRODUCTS")
    parser.add_argument("--region", default="NA")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    n = run(dry_run=args.dry_run, client=ads_client_from_secrets(region=args.region), ad_product=args.ad_product)
    print(f"{'would write' if args.dry_run else 'wrote'} {n} entity rows")


if __name__ == "__main__":
    _cli()
