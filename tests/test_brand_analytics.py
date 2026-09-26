import datetime as dt

from pipelines.spapi import brand_analytics as ba


def test_weeks_are_sunday_to_saturday_and_respect_publish_lag():
    # Thursday 2026-09-24: the week ending Sat 09-19 ended 5 days ago -> included; 09-26 has not ended.
    weeks = ba.last_complete_weeks(3, today=dt.date(2026, 9, 24))
    assert weeks[-1] == (dt.date(2026, 9, 13), dt.date(2026, 9, 19))
    assert weeks[0] == (dt.date(2026, 8, 30), dt.date(2026, 9, 5))
    for sun, sat in weeks:
        assert sun.weekday() == 6 and sat.weekday() == 5
    # Monday 2026-09-21: the week ending 09-19 is only 2 days old -> not yet
    assert ba.last_complete_weeks(1, today=dt.date(2026, 9, 21))[0] == (dt.date(2026, 9, 6), dt.date(2026, 9, 12))


def test_flatten_scp_pulls_the_funnel_out_of_nested_blocks():
    rec = {"startDate": "2026-09-13", "endDate": "2026-09-19", "asin": "B0FSH8VPSF",
           "impressionData": {"impressionCount": 15986, "impressionMedianPrice": {"amount": 15.99, "currencyCode": "USD"},
                              "sameDayShippingImpressionCount": 631, "oneDayShippingImpressionCount": 7437, "twoDayShippingImpressionCount": 3837},
           "clickData": {"clickCount": 113, "clickRate": 0.0071, "clickedMedianPrice": {"amount": 15.99, "currencyCode": "USD"}},
           "cartAddData": {"cartAddCount": 48, "cartAddedMedianPrice": {"amount": 15.99, "currencyCode": "USD"}},
           "purchaseData": {"purchaseCount": 13, "searchTrafficSales": {"amount": 223.86, "currencyCode": "USD"}, "conversionRate": 0.115}}
    row = ba.flatten_scp(rec, dt.datetime(2026, 9, 24, tzinfo=dt.timezone.utc))
    assert row["impressions"] == 15986 and row["clicks"] == 113 and row["cart_adds"] == 48 and row["purchases"] == 13
    assert row["click_rate"] == 0.0071 and row["search_traffic_sales"] == 223.86 and row["currency"] == "USD"
    assert row["one_day_impressions"] == 7437 and row["same_day_clicks"] is None
    assert row["payload"] is rec and row["week_start"] == "2026-09-13"


def test_bounds_reject_misaligned_weeks():
    import pytest
    with pytest.raises(AssertionError):
        ba._bounds((dt.date(2026, 9, 12), dt.date(2026, 9, 18)))


def test_flatten_sqp_matches_amazons_field_names():
    rec = {"startDate": "2026-09-13", "endDate": "2026-09-19", "asin": "B09K2BJG8K",
           "searchQueryData": {"searchQuery": "the beatles merch", "searchQueryScore": 11, "searchQueryVolume": 555},
           "impressionData": {"totalQueryImpressionCount": 16083, "asinImpressionCount": 52, "asinImpressionShare": 0.32},
           "clickData": {"totalClickCount": 206, "totalClickRate": 37.12, "asinClickCount": 1, "asinClickShare": 0.49,
                         "totalMedianClickPrice": {"amount": 14.99, "currencyCode": "USD"}, "asinMedianClickPrice": {"amount": 23.99, "currencyCode": "USD"}},
           "cartAddData": {"totalCartAddCount": 74, "totalCartAddRate": 13.33, "asinCartAddCount": 0, "asinCartAddShare": 0.0, "asinMedianCartAddPrice": None},
           "purchaseData": {"totalPurchaseCount": 14, "totalPurchaseRate": 2.52, "asinPurchaseCount": 0, "asinPurchaseShare": 0.0,
                            "totalMedianPurchasePrice": {"amount": 12.95, "currencyCode": "USD"}, "asinMedianPurchasePrice": None}}
    row = ba.flatten_sqp(rec, dt.datetime(2026, 9, 24, tzinfo=dt.timezone.utc))
    assert row["search_query"] == "the beatles merch" and row["search_query_volume"] == 555
    assert row["impressions_total"] == 16083 and row["impressions_asin"] == 52 and row["impressions_asin_share"] == 0.32
    assert row["click_median_price_asin"] == 23.99 and row["purchase_median_price_asin"] is None
    assert row["purchase_rate_total"] == 2.52 and row["payload"] is rec


def test_scheduled_run_only_loads_on_run_weekday(monkeypatch):
    class Saturday(dt.date):
        @classmethod
        def today(cls):
            return cls(2026, 9, 26)

    monkeypatch.setattr(ba.dt, "date", Saturday)
    # no client passed: reaching the SP-API would fail, so returning proves the skip
    assert ba.run() == {"scp": 0, "sqp": 0}
