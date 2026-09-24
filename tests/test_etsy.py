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

    class FakeBq:
        pass

    monkeypatch.setattr(load.bqlib, "client", lambda: FakeBq())
    monkeypatch.setattr(load, "watermark", lambda bq: 1758000000)
    monkeypatch.setattr(load, "known_versions", lambda bq, since: {(1, load._ts(1758672000))})
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
