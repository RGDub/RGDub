import datetime as dt
import gzip
import io
import json
from unittest import mock

import pytest

from pipelines import sp_ads_daily as job
from pipelines.lib.ads_api import AdsApiClient, chunk_date_range


def test_parent_sku_strips_every_suffix_position():
    assert job.parent_sku("STRANGR-CPNCLS-FBA") == "STRANGR-CPNCLS"
    assert job.parent_sku("FAIRYTL-CPNCLS-FBA-UPC") == "FAIRYTL-CPNCLS"
    assert job.parent_sku("RICHMD-CPNCLS-FBM-CORR") == "RICHMD-CPNCLS"
    assert job.parent_sku("POTTR-CPNCLS") == "POTTR-CPNCLS"
    assert job.parent_sku("MARIO-BNDL") == "MARIO-BNDL"
    assert job.parent_sku(None) is None


def test_default_window_is_31_days_ending_yesterday():
    start, end = job.default_window(dt.date(2026, 9, 17))
    assert end == dt.date(2026, 9, 16)
    assert start == dt.date(2026, 8, 17)
    assert (end - start).days + 1 == 31


def test_report_body_requests_ad_group_grain():
    body = job.report_body(dt.date(2026, 8, 17), dt.date(2026, 9, 16))
    cfg = body["configuration"]
    assert cfg["reportTypeId"] == "spAdvertisedProduct"
    assert cfg["timeUnit"] == "DAILY"
    assert cfg["format"] == "GZIP_JSON"
    for col in ("adGroupId", "adGroupName", "adId", "advertisedSku", "campaignId", "sales30d"):
        assert col in cfg["columns"]
    assert len(cfg["columns"]) == len(set(cfg["columns"]))


def test_transform_types_and_parent_sku():
    raw = [{
        "date": "2026-09-16", "campaignId": "123", "campaignName": "c", "campaignStatus": "ENABLED",
        "campaignBudgetAmount": "25", "campaignBudgetType": "DAILY_BUDGET", "campaignBudgetCurrencyCode": "USD",
        "adGroupId": 456, "adGroupName": "g", "adId": "789",
        "advertisedAsin": "B0X", "advertisedSku": "COLOURS-BNDL-FBA",
        "impressions": 10, "clicks": 1, "cost": "0.42", "spend": 0.42,
        "sales14d": "9.99", "purchases14d": 1,
    }]
    rows = job.transform(raw)
    assert len(rows) == 1
    row = rows[0]
    assert set(row) == set(job.TABLE_COLUMNS)
    assert row["campaignId"] == 123 and isinstance(row["campaignId"], int)
    assert row["adGroupId"] == 456 and row["adId"] == 789
    assert row["cost"] == pytest.approx(0.42)
    assert row["campaignBudgetAmount"] == pytest.approx(25.0)
    assert row["sales30d"] is None            # absent columns become NULL, not 0
    assert row["Parent SKU"] == "COLOURS-BNDL"


def test_check_grain_rejects_repeated_full_key():
    base = {"date": "2026-09-16", "campaignId": 1, "adGroupId": 2, "advertisedSku": "A"}
    job.check_grain([base, {**base, "adGroupId": 3}])          # different ad groups: fine
    with pytest.raises(RuntimeError, match="repeated"):
        job.check_grain([base, dict(base)])


def test_chunk_date_range_respects_31_day_limit():
    chunks = list(chunk_date_range(dt.date(2026, 6, 1), dt.date(2026, 8, 15), max_days=31))
    assert chunks[0] == (dt.date(2026, 6, 1), dt.date(2026, 7, 1))
    assert chunks[-1][1] == dt.date(2026, 8, 15)
    assert all((e - s).days + 1 <= 31 for s, e in chunks)
    # contiguous, no gaps or overlaps
    for (_, e1), (s2, _) in zip(chunks, chunks[1:]):
        assert s2 == e1 + dt.timedelta(days=1)


def _gz(rows):
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as fh:
        fh.write(json.dumps(rows).encode())
    return buf.getvalue()


def test_run_report_end_to_end_with_mocked_http():
    client = AdsApiClient("cid", "sec", "rt", profile_id="p1")
    client._access_token, client._token_expires_at = "tok", 9e12

    calls = []

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        calls.append((method, url, headers))
        resp = mock.Mock()
        resp.headers = {}
        if url.endswith("/reporting/reports") and method == "POST":
            resp.status_code, resp.json = 200, lambda: {"reportId": "r1"}
        elif url.endswith("/reporting/reports/r1"):
            state = "PROCESSING" if len([c for c in calls if c[1].endswith("/r1")]) == 1 else "COMPLETED"
            resp.status_code, resp.json = 200, lambda: {"status": state, "url": "https://dl/x.gz"}
        else:
            raise AssertionError(url)
        return resp

    client.session.request = fake_request
    client.session.get = lambda url, timeout=None: mock.Mock(content=_gz([{"a": 1}]), raise_for_status=lambda: None)

    with mock.patch("pipelines.lib.ads_api.time.sleep") as sleep:
        rows = client.run_report(job.report_body(dt.date(2026, 9, 1), dt.date(2026, 9, 2)))

    assert rows == [{"a": 1}]
    assert sleep.called                      # waited between polls
    post = calls[0]
    assert post[2]["Amazon-Advertising-API-Scope"] == "p1"
    assert post[2]["Content-Type"].startswith("application/vnd.createasyncreportrequest.v3")


def test_request_retries_on_429_then_succeeds():
    client = AdsApiClient("cid", "sec", "rt", profile_id="p1")
    client._access_token, client._token_expires_at = "tok", 9e12
    responses = iter([
        mock.Mock(status_code=429, headers={"Retry-After": "1"}, text="slow down"),
        mock.Mock(status_code=200, headers={}),
    ])
    client.session.request = lambda *a, **k: next(responses)
    with mock.patch("pipelines.lib.ads_api.time.sleep") as sleep:
        resp = client.request("GET", "/v2/profiles", scoped=False)
    assert resp.status_code == 200
    sleep.assert_called_once_with(1.0)
