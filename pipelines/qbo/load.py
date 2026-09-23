"""Daily full pull of QuickBooks Online into ``punlabs.QBO.qbo_raw``.

Every transaction and list record is stored as QuickBooks returns it (a JSON
payload), one row per version: a record whose SyncToken changes gets a new
row, so edits are kept. Records that stop coming back are marked with
``deleted_at``. The views in sql/qbo/ddl.sql flatten this into transactions,
transaction lines, accounts, vendors, customers and a change log.

A full pull (not CDC) because the whole file is ~2,000 transactions, ~30 API
calls, and it is the only way to see deletions without a 30-day CDC window.

    python -m pipelines.qbo.load              # load
    python -m pipelines.qbo.load --dry-run    # pull and count, write nothing to BigQuery
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import uuid

from pipelines.lib import bq as bqlib
from pipelines.lib.heartbeat import run_logged
from pipelines.lib.qbo import QboClient

log = logging.getLogger(__name__)

DATASET = "punlabs.QBO"
RAW = f"{DATASET}.qbo_raw"
STAGE = f"{DATASET}._qbo_stage"

TRANSACTIONS = [
    "Bill", "BillPayment", "Purchase", "Deposit", "JournalEntry", "Transfer", "CreditCardPayment",
    "VendorCredit", "PurchaseOrder", "Invoice", "Payment", "SalesReceipt", "CreditMemo",
    "RefundReceipt", "Estimate",
]
# List entities only return active records unless asked for both.
LISTS = ["Account", "Vendor", "Customer", "Item", "Class", "Department", "Employee", "Term", "PaymentMethod"]

# Refuse to mark deletions when an entity suddenly returns far fewer records
# than BigQuery holds; that is an API problem, not a mass deletion.
DELETE_GUARD_MIN_ROWS = 20
DELETE_GUARD_RATIO = 0.5


def to_row(entity: str, rec: dict, pulled_at: str, run_id: str) -> dict:
    meta = rec.get("MetaData", {})
    return {
        "entity": entity,
        "id": str(rec["Id"]),
        "sync_token": str(rec.get("SyncToken", "0")),
        "txn_date": rec.get("TxnDate"),
        "created_at": meta.get("CreateTime"),
        "last_updated_at": meta.get("LastUpdatedTime"),
        "payload": json.dumps(rec, separators=(",", ":")),
        "pulled_at": pulled_at,
        "run_id": run_id,
    }


def pull(client: QboClient) -> tuple[list[dict], dict[str, int]]:
    pulled_at = dt.datetime.now(dt.timezone.utc).isoformat()
    run_id = str(uuid.uuid4())
    rows, counts = [], {}
    for entity in TRANSACTIONS + LISTS:
        where = "WHERE Active IN (true, false)" if entity in LISTS else ""
        recs = client.query_all(entity, where)
        counts[entity] = len(recs)
        rows += [to_row(entity, r, pulled_at, run_id) for r in recs]
        log.info("%-18s %6d", entity, len(recs))
    return rows, counts


def deletable_entities(counts: dict[str, int], live: dict[str, int]) -> tuple[list[str], list[str]]:
    """Split pulled entities into those safe to mark deletions for and those refused by the guard."""
    ok, refused = [], []
    for entity, n in counts.items():
        have = live.get(entity, 0)
        if have >= DELETE_GUARD_MIN_ROWS and n < have * DELETE_GUARD_RATIO:
            refused.append(f"{entity}: pulled {n}, BigQuery has {have} live")
        else:
            ok.append(entity)
    return ok, refused


STAGE_SCHEMA = [
    ("entity", "STRING"), ("id", "STRING"), ("sync_token", "STRING"), ("txn_date", "DATE"),
    ("created_at", "TIMESTAMP"), ("last_updated_at", "TIMESTAMP"), ("payload", "STRING"),
    ("pulled_at", "TIMESTAMP"), ("run_id", "STRING"),
]


def write(bq, rows: list[dict], counts: dict[str, int]) -> int:
    from google.cloud import bigquery

    job = bq.load_table_from_json(rows, STAGE, job_config=bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(n, t) for n, t in STAGE_SCHEMA], write_disposition="WRITE_TRUNCATE"))
    job.result()

    merge = bq.query(f"""
        MERGE `{RAW}` T
        USING `{STAGE}` S
        ON T.entity = S.entity AND T.id = S.id AND T.sync_token = S.sync_token
        WHEN MATCHED THEN UPDATE SET last_seen_at = S.pulled_at, deleted_at = NULL
        WHEN NOT MATCHED THEN INSERT
          (entity, id, sync_token, txn_date, created_at, last_updated_at, payload, first_seen_at, last_seen_at, deleted_at, run_id)
          VALUES (S.entity, S.id, S.sync_token, S.txn_date, S.created_at, S.last_updated_at,
                  PARSE_JSON(S.payload), S.pulled_at, S.pulled_at, NULL, S.run_id)""")
    merge.result()
    changed = merge.num_dml_affected_rows or 0

    live = {r.entity: r.n for r in bq.query(
        f"SELECT entity, COUNT(DISTINCT id) n FROM `{RAW}` WHERE deleted_at IS NULL GROUP BY entity").result()}
    ok, refused = deletable_entities(counts, live)
    n_deleted = 0
    if ok:
        deleted = bq.query(f"""
            UPDATE `{RAW}` T SET deleted_at = CURRENT_TIMESTAMP()
            WHERE T.deleted_at IS NULL AND T.entity IN ({bqlib.sql_list(ok)})
              AND NOT EXISTS (SELECT 1 FROM `{STAGE}` S WHERE S.entity = T.entity AND S.id = T.id)""")
        deleted.result()
        n_deleted = deleted.num_dml_affected_rows or 0
    log.info("merged %d rows (%d staged); marked %d rows deleted", changed, len(rows), n_deleted)
    if refused:
        raise RuntimeError("Deletion marking refused (possible partial pull): " + "; ".join(refused))
    return len(rows)


def run(dry_run: bool = False, client: QboClient | None = None) -> int:
    with run_logged("qbo_daily", enabled=not dry_run) as ctx:
        client = client or QboClient()
        rows, counts = pull(client)
        if dry_run:
            log.info("dry run: %d records pulled, nothing written", len(rows))
            return len(rows)
        ctx.rows_written = write(bqlib.client(), rows, counts)
        return ctx.rows_written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    print(f"qbo_daily: {run(dry_run=a.dry_run)} records")
