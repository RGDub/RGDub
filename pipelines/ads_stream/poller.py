"""Drain the Marketing Stream SQS queue into punlabs.AMZSales.ads_stream_raw.

Runs anywhere with AWS and GCP credentials: a Cloud Run job on a 5-minute
Cloud Scheduler cadence is the intended home. Each invocation long-polls the
queue for up to ``--max-seconds`` and exits, so overlapping runs are harmless
(SQS visibility timeouts stop two pollers processing one message).

    python -m pipelines.ads_stream.poller --queue-url https://sqs.us-east-1.amazonaws.com/123456789012/amazon-marketing-stream

AWS credentials: the default boto3 chain. Preferred is an IAM role assumed via
web identity from the Cloud Run service account (AWS trusts accounts.google.com
as an OIDC provider); an access key in Secret Manager works as a stopgap.

Message handling
----------------
* ``SubscriptionConfirmation`` -> GET the ``SubscribeURL``; nothing flows until
  this happens once per dataset subscription.
* ``Notification`` -> the ``Message`` field is one stream record. Stored as
  delivered (JSON) plus the handful of keys worth clustering on. Records are
  **deltas**: readers must SUM per ``time_window_start`` after de-duplicating
  on ``idempotency_id`` (see sql/ads_stream/ddl.sql).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import time
from typing import Any

import requests

from pipelines.lib.heartbeat import run_logged

log = logging.getLogger(__name__)

RAW_TABLE = "punlabs.AMZSales.ads_stream_raw"

# Keys we lift out of the payload into real columns. Everything else stays in ``payload``.
LIFTED = {
    "dataset_id": ("dataset_id",),
    "advertiser_id": ("advertiser_id",),
    "marketplace_id": ("marketplace_id",),
    "campaign_id": ("campaign_id",),
    "ad_group_id": ("ad_group_id",),
    "ad_id": ("ad_id",),
    "target_id": ("target_id", "keyword_id"),
    "placement": ("placement",),
}


def _first(record: dict, keys: tuple[str, ...]):
    for key in keys:
        if record.get(key) is not None:
            return str(record[key])
    return None


def record_to_row(record: dict, received_at: dt.datetime | None = None) -> dict[str, Any]:
    """One stream record -> one ads_stream_raw row."""
    received_at = received_at or dt.datetime.now(dt.timezone.utc)
    row = {name: _first(record, keys) for name, keys in LIFTED.items()}
    row["idempotency_id"] = str(record["idempotency_id"])
    row["time_window_start"] = record.get("time_window_start")
    row["received_at"] = received_at.isoformat()
    row["payload"] = json.dumps(record, separators=(",", ":"))
    if not row["dataset_id"]:
        raise ValueError(f"record without dataset_id: {row['idempotency_id']}")
    return row


def parse_sqs_message(body: str) -> tuple[str, Any]:
    """Return ('confirm', subscribe_url) | ('record', record_dict) | ('ignore', reason)."""
    envelope = json.loads(body)
    kind = envelope.get("Type")
    if kind == "SubscriptionConfirmation":
        return "confirm", envelope["SubscribeURL"]
    if kind == "Notification":
        message = envelope.get("Message")
        record = json.loads(message) if isinstance(message, str) else message
        return "record", record
    if "idempotency_id" in envelope:          # raw delivery without SNS envelope
        return "record", envelope
    return "ignore", f"unhandled message type {kind!r}"


def confirm_subscription(url: str) -> None:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    log.info("confirmed SNS subscription via %s", url.split("?")[0])


def insert_rows(bq, rows: list[dict]) -> None:
    """Streaming insert keyed on idempotency_id so SQS redeliveries collapse at the edge."""
    if not rows:
        return
    errors = bq.insert_rows_json(RAW_TABLE, rows, row_ids=[r["idempotency_id"] for r in rows])
    if errors:
        raise RuntimeError(f"BigQuery rejected {len(errors)} rows: {errors[:3]}")


def drain(sqs, queue_url: str, bq, max_seconds: int = 240, batch: int = 10) -> int:
    """Long-poll the queue until it is empty or ``max_seconds`` elapse. Returns rows written."""
    deadline = time.time() + max_seconds
    written = 0
    while time.time() < deadline:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=batch,
            WaitTimeSeconds=20,
            VisibilityTimeout=120,
        )
        messages = resp.get("Messages", [])
        if not messages:
            break
        rows, done = [], []
        for msg in messages:
            kind, value = parse_sqs_message(msg["Body"])
            if kind == "confirm":
                confirm_subscription(value)
            elif kind == "record":
                rows.append(record_to_row(value))
            else:
                log.warning("skipping message %s: %s", msg["MessageId"], value)
            done.append({"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]})
        insert_rows(bq, rows)           # raise before deleting -> message redelivers
        sqs.delete_message_batch(QueueUrl=queue_url, Entries=done)
        written += len(rows)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--queue-url", required=True)
    parser.add_argument("--aws-region", default="us-east-1")
    parser.add_argument("--max-seconds", type=int, default=240)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    import boto3
    from google.cloud import bigquery

    sqs = boto3.client("sqs", region_name=args.aws_region)
    bq = bigquery.Client(project="punlabs")
    with run_logged("ads_stream_poller") as ctx:
        ctx.rows_written = drain(sqs, args.queue_url, bq, max_seconds=args.max_seconds)
        log.info("wrote %d stream rows", ctx.rows_written)


if __name__ == "__main__":
    main()
