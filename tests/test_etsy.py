import datetime as dt
import json

import requests

from pipelines.etsy import load
from pipelines.lib.etsy import EtsyClient, PAGE_LIMIT


def _resp(status, body):
    r = requests.Response()
    r.status_code = status
    r._content = json.dumps(body).encode()
    return r


def test_receipt_row_flattens_keys_and_keeps_payload():
    r = {"receipt_id": 1, "status": "paid", "is_paid": True, "is_shipped": False,
         "created_timestamp": 1758585600, "updated_timestamp": 1758672000, "buyer_user_id": 9,
         "grandtotal": {"amount": 1499, "divisor": 100, "currency_code": "USD"}, "transactions": []}
    row = load.receipt_row(r, "2026-09-23T00:00:00+00:00", "run")
    assert row["receipt_id"] == 1 and row["status"] == "paid"
    assert row["created_at"] == dt.datetime.fromtimestamp(1758585600, dt.timezone.utc).isoformat()
    assert row["updated_at"] == dt.datetime.fromtimestamp(1758672000, dt.timezone.utc).isoformat()
    assert row["payload"] is r


def test_ledger_row_converts_cents():
    e = {"entry_id": 5, "ledger_type": "sale", "amount": 1499, "balance": 25000, "currency": "USD",
         "created_timestamp": 1758585600, "reference_type": "receipt", "reference_id": "1"}
    row = load.ledger_row(e, "p", "r")
    assert row["amount"] == 14.99 and row["balance"] == 250.0 and row["reference_id"] == "1"


def test_pagination_stops_on_short_page(monkeypatch):
    pages = [{"count": 150, "results": [{"i": n} for n in range(PAGE_LIMIT)]},
             {"count": 150, "results": [{"i": n} for n in range(50)]}]
    calls = []

    class S:
        def request(self, method, url, headers=None, params=None, json=None, timeout=None):
            calls.append(params["offset"])
            return _resp(200, pages[len(calls) - 1])

    c = EtsyClient("k", "s", shop_id=1)
    c.session = S(); c._access_token = "t"; c._token_expires_at = 9e12
    items = list(c.receipts(min_last_modified=0))
    assert len(items) == 150 and calls == [0, PAGE_LIMIT]


def test_refresh_rotates_and_saves_token(monkeypatch):
    saved = {}
    monkeypatch.setattr("pipelines.lib.secrets.add_secret_version", lambda sid, val: saved.update({sid: val}))

    class S:
        def post(self, url, data=None, timeout=None):
            assert data["grant_type"] == "refresh_token" and data["refresh_token"] == "old"
            return _resp(200, {"access_token": "1.acc", "refresh_token": "1.new", "expires_in": 3600})

    c = EtsyClient("k", "s", refresh_token="old", shop_id=1, refresh_token_secret_id="etsy-oauth-refresh-token")
    c.session = S()
    assert c.access_token() == "1.acc"
    assert c.refresh_token == "1.new" and saved == {"etsy-oauth-refresh-token": "1.new"}


def test_401_refreshes_once_then_retries(monkeypatch):
    class S:
        def __init__(self): self.n = 0
        def request(self, method, url, headers=None, params=None, json=None, timeout=None):
            self.n += 1
            return _resp(401, {"error": "expired"}) if self.n == 1 else _resp(200, {"ok": True})
        def post(self, url, data=None, timeout=None):
            return _resp(200, {"access_token": "1.fresh", "refresh_token": "old", "expires_in": 3600})

    c = EtsyClient("k", "s", refresh_token="old", shop_id=1)
    c.session = S(); c._access_token = "stale"; c._token_expires_at = 9e12
    assert c.request("GET", "/x").json() == {"ok": True} and c.session.n == 2


def test_run_dry_counts_new_versions_only(monkeypatch):
    class FakeEtsy:
        shop_id = 1
        def receipts(self, min_last_modified=None, **k):
            return [{"receipt_id": 1, "updated_timestamp": 1758672000, "created_timestamp": 1758585600},
                    {"receipt_id": 2, "updated_timestamp": 1758672000, "created_timestamp": 1758585600}]
        def ledger_entries(self, a, b): return [{"entry_id": 1, "amount": 100, "balance": 100}]
        def listings(self, state="active"): return [{"listing_id": 7}] if state == "active" else []
        def reviews(self, min_created=None, max_created=None): return []
        def payments_for_receipt(self, rid): return []

    monkeypatch.setattr(load.bqlib, "client", lambda: object())
    monkeypatch.setattr(load, "watermark", lambda bq: 1758000000)
    monkeypatch.setattr(load, "known_versions", lambda bq, since: {(1, load._ts(1758672000))})
    monkeypatch.setattr(load, "review_watermark", lambda bq: None)
    monkeypatch.setattr(load, "known_reviews", lambda bq, since: set())
    monkeypatch.setattr(load, "known_payments", lambda bq: set())
    assert load.run(dry_run=True, client=FakeEtsy()) == 1 + 1 + 1   # one new receipt version, one ledger entry, one listing


def test_ledger_splits_into_31_day_windows():
    windows = []

    class S:
        def request(self, method, url, headers=None, params=None, json=None, timeout=None):
            windows.append((params["min_created"], params["max_created"]))
            return _resp(200, {"results": []})

    c = EtsyClient("k", "s", shop_id=1); c.session = S(); c._access_token = "t"; c._token_expires_at = 9e12
    list(c.ledger_entries(0, 45 * 86400))
    assert len(windows) == 2 and all(b - a <= 31 * 86400 for a, b in windows) and windows[-1][1] == 45 * 86400


def test_payment_row_converts_money_objects():
    p = {"payment_id": 3, "receipt_id": 1, "status": "settled",
         "amount_gross": {"amount": 1999, "divisor": 100, "currency_code": "USD"},
         "amount_fees": {"amount": 214, "divisor": 100}, "amount_net": {"amount": 1785, "divisor": 100},
         "adjusted_gross": {"amount": 1999, "divisor": 100}, "adjusted_fees": {"amount": 214, "divisor": 100},
         "adjusted_net": {"amount": 1785, "divisor": 100}, "currency": "USD",
         "created_timestamp": 1758585600, "updated_timestamp": 1758585600}
    row = load.payment_row(p, "p", "r")
    assert (row["gross"], row["fees"], row["net"], row["adjusted_net"]) == (19.99, 2.14, 17.85, 17.85)


def test_review_row_keeps_rating_and_payload():
    v = {"transaction_id": 5, "listing_id": 9, "rating": 5, "review": "great", "created_timestamp": 1758585600,
         "updated_timestamp": 1758585600, "buyer_user_id": 1}
    row = load.review_row(v, "p", "r")
    assert row["rating"] == 5 and row["listing_id"] == 9 and row["payload"]["review"] == "great"


def test_run_fetches_payments_only_for_changed_receipts(monkeypatch):
    asked = []

    class FakeEtsy:
        shop_id = 1
        def receipts(self, min_last_modified=None, **k):
            return [{"receipt_id": 1, "updated_timestamp": 1758672000, "created_timestamp": 1758585600},
                    {"receipt_id": 2, "updated_timestamp": 1758672000, "created_timestamp": 1758585600}]
        def ledger_entries(self, a, b): return []
        def listings(self, state="active"): return []
        def reviews(self, min_created=None, max_created=None): return [{"transaction_id": 7, "updated_timestamp": 1, "created_timestamp": 1}]
        def payments_for_receipt(self, rid):
            asked.append(rid)
            return [{"payment_id": rid * 10, "receipt_id": rid, "updated_timestamp": 1758672000, "created_timestamp": 1}]

    monkeypatch.setattr(load.bqlib, "client", lambda: object())
    monkeypatch.setattr(load, "watermark", lambda bq: 1758000000)
    monkeypatch.setattr(load, "known_versions", lambda bq, since: {(1, load._ts(1758672000))})   # receipt 1 unchanged
    monkeypatch.setattr(load, "review_watermark", lambda bq: None)
    monkeypatch.setattr(load, "known_reviews", lambda bq, since: set())
    monkeypatch.setattr(load, "known_payments", lambda bq: set())
    assert load.run(dry_run=True, client=FakeEtsy()) == 1 + 1 + 1   # receipt 2, its payment, one review
    assert asked == [2]
