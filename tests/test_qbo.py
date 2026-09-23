import json

import pytest

from pipelines.lib.qbo import QboClient
from pipelines.qbo import load


class FakeQbo(QboClient):
    """QboClient with the HTTP layer replaced by an in-memory table per entity."""

    def __init__(self, data, short_read=False):
        self.data = data
        self.short_read = short_read
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        entity = sql.split(" FROM ")[1].split()[0]
        recs = self.data.get(entity, [])
        if "COUNT(*)" in sql:
            return {"totalCount": len(recs)}
        start = int(sql.split("STARTPOSITION ")[1].split()[0])
        size = int(sql.split("MAXRESULTS ")[1].split()[0])
        page = recs[start - 1:start - 1 + size]
        if self.short_read:
            page = page[:-1]
        return {entity: page} if page else {}


def test_query_all_pages_past_1000():
    recs = [{"Id": str(i)} for i in range(2345)]
    client = FakeQbo({"Purchase": recs})
    assert len(client.query_all("Purchase")) == 2345
    assert sum("STARTPOSITION" in q for q in client.queries) == 3


def test_query_all_fails_on_short_read():
    client = FakeQbo({"Bill": [{"Id": str(i)} for i in range(5)]}, short_read=True)
    with pytest.raises(RuntimeError, match="COUNT"):
        client.query_all("Bill")


def test_query_all_reads_renamed_response_key():
    class Renamed(FakeQbo):
        def query(self, sql):
            resp = super().query(sql)
            return {"CreditCardPaymentTxn": resp.pop("CreditCardPayment"), "startPosition": 1} \
                if "CreditCardPayment" in resp else resp
    client = Renamed({"CreditCardPayment": [{"Id": "1"}, {"Id": "2"}, {"Id": "3"}]})
    assert len(client.query_all("CreditCardPayment")) == 3


def test_list_entities_include_inactive():
    client = FakeQbo({})
    load.pull(client)
    account_queries = [q for q in client.queries if " FROM Account " in q + " "]
    assert account_queries and all("Active IN (true, false)" in q for q in account_queries)
    assert not any("Active" in q for q in client.queries if " FROM Bill" in q)


def test_to_row_keeps_full_payload_and_version():
    rec = {"Id": "1967", "SyncToken": "0", "TxnDate": "2026-08-22",
           "MetaData": {"CreateTime": "2026-09-03T11:14:50-07:00", "LastUpdatedTime": "2026-09-03T11:15:15-07:00"},
           "Line": [{"Amount": 2200.0}]}
    row = load.to_row("Bill", rec, "2026-09-23T20:00:00+00:00", "run-1")
    assert (row["entity"], row["id"], row["sync_token"], row["txn_date"]) == ("Bill", "1967", "0", "2026-08-22")
    assert json.loads(row["payload"]) == rec


def test_delete_guard_refuses_sudden_drop():
    ok, refused = load.deletable_entities(
        counts={"Purchase": 10, "Bill": 124, "Transfer": 0},
        live={"Purchase": 1152, "Bill": 125, "Transfer": 0})
    assert "Purchase" not in ok and refused and refused[0].startswith("Purchase")
    assert "Bill" in ok and "Transfer" in ok


def test_delete_guard_allows_small_entities_to_empty():
    ok, refused = load.deletable_entities(counts={"Invoice": 0}, live={"Invoice": 1})
    assert ok == ["Invoice"] and not refused
