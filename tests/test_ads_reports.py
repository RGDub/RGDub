import datetime as dt

import pytest

from pipelines import ads_reports as ar


def test_window_is_trailing_14_days_ending_yesterday():
    assert ar.default_window(dt.date(2026, 9, 24)) == (dt.date(2026, 9, 10), dt.date(2026, 9, 23))


def test_search_term_transform_maps_and_types_the_live_column_names():
    raw = {"date": "2026-09-19", "purchases7d": 0, "keywordId": 350308015777117, "cost": 0.48, "adGroupName": "B09K2BJG8K",
           "matchType": "TARGETING_EXPRESSION_PREDEFINED", "sales14d": 0, "campaignId": 90436314277743, "impressions": 1,
           "sales7d": 0, "purchases14d": 0, "adGroupId": 370277098411634, "searchTerm": "048682280x", "clicks": 1,
           "keyword": "complements", "campaignName": "SP - Auto - The Colours"}
    (row,) = ar.transform("search_terms", [raw], dt.datetime(2026, 9, 24, tzinfo=dt.timezone.utc))
    assert row["search_term"] == "048682280x" and row["keyword_id"] == 350308015777117 and row["cost"] == 0.48
    assert isinstance(row["campaign_id"], int) and isinstance(row["sales_14d"], float)
    assert row["loaded_at"].startswith("2026-09-24")


def test_placement_transform_and_grain_check():
    raw = {"date": "2026-09-16", "purchases7d": 0, "cost": 0.84, "sales14d": 0, "placementClassification": "Detail Page on-Amazon",
           "campaignId": 240960446577073, "clicks": 2, "impressions": 332, "sales7d": 0, "campaignName": "SP - Auto", "purchases14d": 0}
    rows = ar.transform("placements", [raw, raw], dt.datetime(2026, 9, 24, tzinfo=dt.timezone.utc))
    assert rows[0]["placement"] == "Detail Page on-Amazon"
    with pytest.raises(RuntimeError):
        ar.check_grain("placements", rows)
    ar.check_grain("placements", rows[:1])
