import datetime as dt

import requests

from pipelines.faire import load
from pipelines.lib.faire import FaireClient


def test_order_row_lifts_keys_and_keeps_payload_as_dict():
    order = {"id": "bo_1", "display_id": "ABC", "state": "DELIVERED", "created_at": "2026-09-01T00:00:00.000Z",
             "updated_at": "2026-09-05T00:00:00.000Z", "payment_initiated_at": None, "retailer_id": "r_1",
             "source": "MARKETPLACE", "items": [{"sku": "KPOP-CPNCLS", "quantity": 10}]}
    row = load.order_row(order, "2026-09-23T00:00:00+00:00", "run")
    assert row["order_id"] == "bo_1" and row["state"] == "DELIVERED" and row["retailer_id"] == "r_1"
    assert isinstance(row["payload"], dict) and row["payload"]["items"][0]["sku"] == "KPOP-CPNCLS"


def test_pagination_follows_cursor_until_short_page(monkeypatch):
    calls = []

    class Fake:
        def request(self, method, url, headers=None, params=None, json=None, timeout=None):
            calls.append(dict(params))
            r = requests.Response(); r.status_code = 200
            if params.get("cursor") == "c1":
                r._content = b'{"orders": [{"id": "bo_3"}], "cursor": "c2"}'
            else:
                r._content = b'{"orders": [{"id": "bo_1"}, {"id": "bo_2"}], "cursor": "c1"}'
            return r

    c = FaireClient("a", "s", "t"); c.session = Fake()
    monkeypatch.setattr("pipelines.lib.faire.PAGE_LIMIT", 2)
    ids = [o["id"] for o in c.orders(updated_at_min="2026-09-01T00:00:00.000Z")]
    assert ids == ["bo_1", "bo_2", "bo_3"]
    assert calls[0]["updated_at_min"] == "2026-09-01T00:00:00.000Z" and "cursor" not in calls[0]
    assert calls[1]["cursor"] == "c1"


def test_headers_carry_both_credentials():
    h = FaireClient("app", "secret", "tok").headers
    assert h["X-FAIRE-OAUTH-ACCESS-TOKEN"] == "tok"
    assert h["X-FAIRE-APP-CREDENTIALS"] == "YXBwOnNlY3JldA=="  # base64("app:secret")


def test_client_retries_429_then_succeeds(monkeypatch):
    class Flaky:
        def __init__(self): self.n = 0
        def request(self, *a, **k):
            self.n += 1
            r = requests.Response()
            r.status_code = 429 if self.n == 1 else 200
            r._content = b'{"orders": [], "cursor": null}'
            return r

    monkeypatch.setattr("time.sleep", lambda s: None)
    c = FaireClient("a", "s", "t"); c.session = Flaky()
    assert list(c.orders()) == [] and c.session.n == 2
