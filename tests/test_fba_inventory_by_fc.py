import datetime as dt

import pandas as pd
import pytest

from pipelines.spapi import fba_inventory_by_fc as fc
from pipelines.spapi import fba_ledger

HEADER = ("Date\tFNSKU\tASIN\tMSKU\tTitle\tDisposition\tStarting Warehouse Balance\tIn Transit Between Warehouses\t"
          "Receipts\tCustomer Shipments\tCustomer Returns\tVendor Returns\tWarehouse Transfer In/Out\tFound\tLost\t"
          "Damaged\tDisposed\tOther Events\tEnding Warehouse Balance\tUnknown Events\tLocation\tStore\n")


def report(*rows):
    return HEADER + "".join("\t".join(map(str, r)) + "\n" for r in rows)


ROW = ["09/12/2026", "X004FYSAZ5", "B09JZWZX5B", "KPOP-CPNCLS-FBA", "t", "SELLABLE",
       10, 0, 2, -3, 1, 0, -1, 0, 0, 0, 0, 0, 9, 0, "DET3", ""]


def test_transform_maps_headers_and_types():
    df = fc.transform(report(ROW, ROW[:20] + ["OMA2", ""]))
    assert list(df["fc"]) == ["DET3", "OMA2"]
    r = df.iloc[0]
    assert r["date"] == dt.date(2026, 9, 12)
    assert (r["starting_balance"], r["receipts"], r["customer_shipments"], r["warehouse_transfers"], r["ending_balance"]) == (10, 2, -3, -1, 9)
    assert r["parent_sku"] == "KPOP-CPNCLS" and pd.isna(r["store"])
    assert "Location" not in df.columns and "Ending Warehouse Balance" not in df.columns


def test_transform_accepts_underscored_headers():
    text = report(ROW).replace("Ending Warehouse Balance", "Ending_Warehouse_Balance").replace("Warehouse Transfer In/Out", "Warehouse_Transfer_In_Out")
    df = fc.transform(text)
    assert df.iloc[0]["ending_balance"] == 9 and df.iloc[0]["warehouse_transfers"] == -1


def test_transform_empty_document_gives_empty_frame():
    assert fc.transform("").empty and fc.transform("  \n").empty


def test_transform_refuses_country_level_report():
    with pytest.raises(ValueError, match="Location"):
        fc.transform(report(ROW).replace("\tLocation", "\tCountry"))


def test_transform_refuses_duplicate_keys():
    with pytest.raises(ValueError, match="duplicate"):
        fc.transform(report(ROW, ROW))


def test_window_is_trailing_days_ending_yesterday():
    assert fc.window(dt.date(2026, 9, 22)) == (dt.date(2026, 9, 1), dt.date(2026, 9, 21))


def test_month_chunks_split_on_calendar_months():
    assert fc.month_chunks(dt.date(2026, 1, 15), dt.date(2026, 3, 10)) == [
        (dt.date(2026, 1, 15), dt.date(2026, 1, 31)),
        (dt.date(2026, 2, 1), dt.date(2026, 2, 28)),
        (dt.date(2026, 3, 1), dt.date(2026, 3, 10))]
    q = fc.month_chunks(dt.date(2025, 3, 1), dt.date(2026, 9, 21), 3)
    assert len(q) == 7 and q[0] == (dt.date(2025, 3, 1), dt.date(2025, 5, 31)) and q[-1] == (dt.date(2026, 9, 1), dt.date(2026, 9, 21))


def test_load_replaces_only_dates_amazon_returned(monkeypatch):
    seen = {}
    monkeypatch.setattr(fc.bqlib, "replace_window", lambda bq, df, table, where, schema=None: seen.update(where=where) or len(df))
    df = fc.transform(report(ROW, ["09/10/2026"] + ROW[1:]))
    assert fc.load(object(), df) == 2
    assert seen["where"] == "date IN (DATE('2026-09-10'), DATE('2026-09-12'))"
    assert fc.load(object(), fc.transform("")) == 0


def test_ledger_scheduled_run_also_refreshes_fc(monkeypatch):
    calls = []
    monkeypatch.setattr(fba_ledger, "_run_country", lambda **k: (61, "client"))
    monkeypatch.setattr(fc, "run", lambda **k: calls.append(k))
    assert fba_ledger.run() == 61
    assert calls == [{"dry_run": False, "client": "client"}]
    calls.clear()
    fba_ledger.run(day=dt.date(2026, 9, 1))          # a hand re-run of one day skips the FC refresh
    assert calls == []


def test_awd_inventory_requests_details_on_every_page():
    import requests
    from pipelines.lib.spapi import SpApiClient

    class Pages:
        def __init__(self): self.params = []
        def request(self, method, url, params=None, **k):
            self.params.append(dict(params))
            body = b'{"inventory": [{"sku": "A"}], "nextToken": "n"}' if len(self.params) == 1 else b'{"inventory": [{"sku": "B"}]}'
            r = requests.Response(); r.status_code = 200; r._content = body; return r

    c = SpApiClient("i", "s", "r"); c.session = Pages(); c._access_token = "t"; c._token_expires_at = 9e12
    assert [i["sku"] for i in c.awd_inventory()] == ["A", "B"]
    assert all(p.get("details") == "SHOW" for p in c.session.params) and c.session.params[1]["nextToken"] == "n"
