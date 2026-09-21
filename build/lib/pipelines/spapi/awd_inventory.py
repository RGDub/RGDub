"""AWD inventory snapshot -> punlabs.AMZSales.AWDInventoryDaily.

Replaces notebook task ``AMZSales-DailyINV-AWDINVLedger``. Pulls every AWD
inventory item, flattens the quantity block, keeps the earliest expiration,
stamps the snapshot with yesterday's Pacific date, then deletes that date and
appends.

Fixes over the notebook: it read only the first page of the AWD endpoint, and
it appended without deleting, so re-runs duplicated the day.

    from pipelines.spapi.awd_inventory import run
    run()
"""

from __future__ import annotations

import datetime as dt
import logging
import zoneinfo

import pandas as pd

from pipelines.lib import bq as bqlib
from pipelines.lib.heartbeat import run_logged
from pipelines.lib.sku import parent_sku_legacy
from pipelines.lib.spapi import SpApiClient, spapi_client_from_secrets

log = logging.getLogger(__name__)

TABLE = "punlabs.AMZSales.AWDInventoryDaily"
QUANTITY_COLUMNS = ["totalInboundQuantity", "totalOnhandQuantity", "availableDistributableQuantity",
                    "replenishmentQuantity", "reservedDistributableQuantity"]
PACIFIC = zoneinfo.ZoneInfo("America/Los_Angeles")


def snapshot_date(now: dt.datetime | None = None) -> dt.date:
    now = now or dt.datetime.now(dt.timezone.utc)
    return (now.astimezone(PACIFIC) - dt.timedelta(days=1)).date()


def transform(items: list[dict], day: dt.date) -> pd.DataFrame:
    rows = []
    for it in items:
        details = it.get("inventoryDetails") or {}
        exp = it.get("expirationDetails") or []
        row = {"snapshot_date": day, "sku": it.get("sku")}
        for col in QUANTITY_COLUMNS:
            row[col] = int(pd.to_numeric(it.get(col, details.get(col, 0)), errors="coerce") or 0)
        row["earliestExpirationDate"] = (exp[0].get("expiration") or "")[:10] or None if exp else None
        row["Parent SKU"] = parent_sku_legacy(it.get("sku"))
        rows.append(row)
    return pd.DataFrame(rows)


def run(*, dry_run: bool = False, client: SpApiClient | None = None) -> int:
    with run_logged("awd_inventory_daily") as ctx:
        sp = client or spapi_client_from_secrets()
        day = snapshot_date()
        df = transform(list(sp.awd_inventory()), day)
        log.info("AWD snapshot %s: %d SKUs", day, len(df))
        if dry_run:
            return len(df)
        bq = bqlib.client()
        ctx.rows_written = bqlib.replace_window(bq, df, TABLE, where=f"snapshot_date = DATE('{day}')")
        return ctx.rows_written


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run(dry_run="--dry-run" in sys.argv))
