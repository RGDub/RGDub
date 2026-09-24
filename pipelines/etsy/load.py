"""Daily pull of Etsy receipts, ledger entries and listings into ``punlabs.EtsySales``.

Receipts (orders, with their transactions = line items, shipments and refunds
embedded) are stored as Etsy returns them, one row per version: a receipt whose
updated_timestamp moves gets a new row, so status changes and refunds are kept.
Ledger entries (sales, fees, deposits, refunds) are re-pulled for a trailing
window and replaced. Listings are snapshotted daily with their inventory
(SKUs and prices). Reviews are pulled incrementally by creation date. Payment
detail (Etsy's gross / fees / net per order) is fetched for every receipt that
changed in the run, one call each. The views in sql/etsy/ddl.sql flatten all of it and
reproduce the legacy CSV-export columns.

Incremental: each run asks for receipts modified since the newest
updated_timestamp already stored, less a two-day overlap, and appends only
versions it has not seen. The first run pulls the shop's whole history.

    python -m pipelines.etsy.load             # load
    python -m pipelines.etsy.load --dry-run   # pull and count, write nothing
    python -m pipelines.etsy.load --full      # ignore the watermark, re-pull every receipt
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import uuid

from pipelines.lib import bq as bqlib
from pipelines.lib.etsy import EtsyClient, etsy_client_from_secrets
from pipelines.lib.heartbeat import run_logged

log = logging.getLogger(__name__)

DATASET = "punlabs.EtsySales"
RECEIPTS_RAW = f"{DATASET}.etsy_receipts_raw"
LEDGER_RAW = f"{DATASET}.etsy_ledger_raw"
LISTINGS_RAW = f"{DATASET}.etsy_listings_raw"
REVIEWS_RAW = f"{DATASET}.etsy_reviews_raw"
PAYMENTS_RAW = f"{DATASET}.etsy_payments_raw"
OVERLAP = dt.timedelta(days=2)
LEDGER_WINDOW = dt.timedelta(days=45)
LISTING_STATES = ("active", "inactive", "sold_out", "draft", "expired")


def _ts(unix: int | None) -> str | None:
    return dt.datetime.fromtimestamp(unix, dt.timezone.utc).isoformat() if unix else None


def receipt_row(r: dict, pulled_at: str, run_id: str) -> dict:
    return {
        "receipt_id": r["receipt_id"],
        "status": r.get("status"),
        "is_paid": r.get("is_paid"),
        "is_shipped": r.get("is_shipped"),
        "created_at": _ts(r.get("created_timestamp") or r.get("create_timestamp")),
        "updated_at": _ts(r.get("updated_timestamp") or r.get("update_timestamp")),
        "buyer_user_id": r.get("buyer_user_id"),
        "pulled_at": pulled_at,
        "run_id": run_id,
        "payload": r,
    }


def ledger_row(e: dict, pulled_at: str, run_id: str) -> dict:
    return {
        "entry_id": e["entry_id"],
        "ledger_type": e.get("ledger_type"),
        "reference_type": e.get("reference_type"),
        "reference_id": e.get("reference_id"),
        "created_at": _ts(e.get("created_timestamp") or e.get("create_date")),
        "amount": (e.get("amount") or 0) / 100.0,
        "balance": (e.get("balance") or 0) / 100.0,
        "currency": e.get("currency"),
        "description": e.get("description"),
        "pulled_at": pulled_at,
        "run_id": run_id,
        "payload": e,
    }


def listing_row(l: dict, pulled_at: str, run_id: str) -> dict:
    return {
        "listing_id": l["listing_id"],
        "state": l.get("state"),
        "title": l.get("title"),
        "updated_at": _ts(l.get("updated_timestamp") or l.get("last_modified_timestamp")),
        "pulled_at": pulled_at,
        "run_id": run_id,
        "payload": l,
    }


def review_row(v: dict, pulled_at: str, run_id: str) -> dict:
    return {
        "transaction_id": v.get("transaction_id"),
        "listing_id": v.get("listing_id"),
        "buyer_user_id": v.get("buyer_user_id"),
        "rating": v.get("rating"),
        "created_at": _ts(v.get("created_timestamp") or v.get("create_timestamp")),
        "updated_at": _ts(v.get("updated_timestamp") or v.get("update_timestamp")),
        "pulled_at": pulled_at,
        "run_id": run_id,
        "payload": v,
    }


def _money(m: dict | None) -> float | None:
    return (m["amount"] / m["divisor"]) if m and m.get("divisor") else None


def payment_row(p: dict, pulled_at: str, run_id: str) -> dict:
    return {
        "payment_id": p["payment_id"],
        "receipt_id": p.get("receipt_id"),
        "status": p.get("status"),
        "gross": _money(p.get("amount_gross")),
        "fees": _money(p.get("amount_fees")),
        "net": _money(p.get("amount_net")),
        "adjusted_gross": _money(p.get("adjusted_gross")),
        "adjusted_fees": _money(p.get("adjusted_fees")),
        "adjusted_net": _money(p.get("adjusted_net")),
        "currency": p.get("currency"),
        "created_at": _ts(p.get("created_timestamp") or p.get("create_timestamp")),
        "updated_at": _ts(p.get("updated_timestamp") or p.get("update_timestamp")),
        "pulled_at": pulled_at,
        "run_id": run_id,
        "payload": p,
    }


def review_watermark(bq) -> int | None:
    rows = list(bq.query(f"SELECT MAX(created_at) AS m FROM `{REVIEWS_RAW}`").result())
    if not rows or rows[0].m is None:
        return None
    return int((rows[0].m - OVERLAP).timestamp())


def known_reviews(bq, since_unix: int | None) -> set[tuple[int, str]]:
    where = f"WHERE created_at >= TIMESTAMP_SECONDS({since_unix})" if since_unix else ""
    return {(r.transaction_id, r.updated_at.isoformat())
            for r in bq.query(f"SELECT transaction_id, updated_at FROM `{REVIEWS_RAW}` {where}").result()}


def known_payments(bq) -> set[tuple[int, str]]:
    return {(r.payment_id, r.updated_at.isoformat())
            for r in bq.query(f"SELECT payment_id, updated_at FROM `{PAYMENTS_RAW}`").result()}


def watermark(bq) -> int | None:
    """Newest receipt updated_at already stored, less the overlap, as unix seconds. None on first run."""
    rows = list(bq.query(f"SELECT MAX(updated_at) AS m FROM `{RECEIPTS_RAW}`").result())
    if not rows or rows[0].m is None:
        return None
    return int((rows[0].m - OVERLAP).timestamp())


def known_versions(bq, since_unix: int | None) -> set[tuple[int, str]]:
    where = f"WHERE updated_at >= TIMESTAMP_SECONDS({since_unix})" if since_unix else ""
    return {(r.receipt_id, r.updated_at.isoformat())
            for r in bq.query(f"SELECT receipt_id, updated_at FROM `{RECEIPTS_RAW}` {where}").result()}


def run(*, dry_run: bool = False, full: bool = False, client: EtsyClient | None = None) -> int:
    with run_logged("etsy_daily", enabled=not dry_run) as ctx:
        etsy = client or etsy_client_from_secrets()
        bq = bqlib.client()
        now = dt.datetime.now(dt.timezone.utc)
        pulled_at, run_id = now.isoformat(), str(uuid.uuid4())

        since = None if full else watermark(bq)
        receipts = list(etsy.receipts(min_last_modified=since))
        seen = known_versions(bq, since) if not full else set()
        new_receipts = [r for r in receipts
                        if (r["receipt_id"], receipt_row(r, pulled_at, run_id)["updated_at"]) not in seen]
        log.info("receipts modified since %s: %d returned, %d new versions",
                 _ts(since) or "the beginning", len(receipts), len(new_receipts))

        ledger_from = int((now - LEDGER_WINDOW).timestamp())
        ledger = list(etsy.ledger_entries(ledger_from, int(now.timestamp())))
        log.info("ledger entries in the last %d days: %d", LEDGER_WINDOW.days, len(ledger))

        listings = [l for state in LISTING_STATES for l in etsy.listings(state=state)]
        log.info("listings across %s: %d", ", ".join(LISTING_STATES), len(listings))

        r_since = None if full else review_watermark(bq)
        reviews = list(etsy.reviews(min_created=r_since))
        seen_reviews = known_reviews(bq, r_since) if not full else set()
        new_reviews = [v for v in reviews
                       if (v.get("transaction_id"), review_row(v, pulled_at, run_id)["updated_at"]) not in seen_reviews]
        log.info("reviews since %s: %d returned, %d new versions", _ts(r_since) or "the beginning", len(reviews), len(new_reviews))

        # Per-order payment detail only for receipts that changed this run (one call each).
        seen_payments = known_payments(bq) if not full else set()
        payments = [p for r in new_receipts for p in etsy.payments_for_receipt(r["receipt_id"])]
        new_payments = [p for p in payments
                        if (p["payment_id"], payment_row(p, pulled_at, run_id)["updated_at"]) not in seen_payments]
        log.info("payments for %d changed receipts: %d returned, %d new versions", len(new_receipts), len(payments), len(new_payments))

        if dry_run:
            return len(new_receipts) + len(ledger) + len(listings) + len(new_reviews) + len(new_payments)

        written = 0
        if new_receipts:
            written += bqlib.load_json_rows(bq, [receipt_row(r, pulled_at, run_id) for r in new_receipts], RECEIPTS_RAW)
        if ledger:
            # Replace the window so restated or late entries never duplicate.
            bqlib.delete_where(bq, LEDGER_RAW, f"created_at >= TIMESTAMP_SECONDS({ledger_from})")
            written += bqlib.load_json_rows(bq, [ledger_row(e, pulled_at, run_id) for e in ledger], LEDGER_RAW)
        if listings:
            written += bqlib.load_json_rows(bq, [listing_row(l, pulled_at, run_id) for l in listings], LISTINGS_RAW)
        if new_reviews:
            written += bqlib.load_json_rows(bq, [review_row(v, pulled_at, run_id) for v in new_reviews], REVIEWS_RAW)
        if new_payments:
            written += bqlib.load_json_rows(bq, [payment_row(p, pulled_at, run_id) for p in new_payments], PAYMENTS_RAW)
        ctx.rows_written = written
        return written


def backfill_payments(*, client: EtsyClient | None = None, pause_s: float = 0.15, batch: int = 500) -> int:
    """One-time: fetch payment detail for every receipt that has none yet.

    The daily run only looks up payments for receipts that changed, so the
    history loaded before this existed needs a pass. One call per receipt,
    throttled under Etsy's per-second limit; loads in batches so a stop
    midway keeps what was fetched.

        python -m pipelines.etsy.load --payments-backfill
    """
    import time

    with run_logged("etsy_payments_backfill") as ctx:
        etsy = client or etsy_client_from_secrets()
        bq = bqlib.client()
        todo = [r.receipt_id for r in bq.query(
            f"SELECT DISTINCT receipt_id FROM `{RECEIPTS_RAW}` WHERE receipt_id NOT IN "
            f"(SELECT receipt_id FROM `{PAYMENTS_RAW}` WHERE receipt_id IS NOT NULL) ORDER BY receipt_id DESC").result()]
        log.info("receipts without payment detail: %d", len(todo))
        pulled_at, run_id = dt.datetime.now(dt.timezone.utc).isoformat(), str(uuid.uuid4())
        rows: list[dict] = []
        for i, rid in enumerate(todo, 1):
            rows.extend(payment_row(p, pulled_at, run_id) for p in etsy.payments_for_receipt(rid))
            time.sleep(pause_s)
            if len(rows) >= batch or i == len(todo):
                if rows:
                    ctx.rows_written += bqlib.load_json_rows(bq, rows, PAYMENTS_RAW)
                    rows = []
                log.info("%d/%d receipts done, %d payment rows written", i, len(todo), ctx.rows_written)
        return ctx.rows_written


def _cli() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--full", action="store_true", help="ignore the watermark and re-pull every receipt")
    ap.add_argument("--payments-backfill", action="store_true", help="fetch payment detail for receipts that have none")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.payments_backfill:
        print(backfill_payments())
    else:
        print(run(dry_run=args.dry_run, full=args.full))


if __name__ == "__main__":
    _cli()
