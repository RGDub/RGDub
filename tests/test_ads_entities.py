import datetime as dt
import json

from pipelines.ads_entities import entity_rows, to_snake


def test_to_snake_recurses_into_lists_and_dicts():
    src = {"campaignId": "1", "budgets": [{"budgetType": "DAILY", "budgetValue": {"monetaryBudgetValue": {"monetaryBudget": {"value": 5.0}}}}],
           "lastUpdatedDateTime": "2026-09-21T00:00:00Z"}
    out = to_snake(src)
    assert out["campaign_id"] == "1"
    assert out["budgets"][0]["budget_value"]["monetary_budget_value"]["monetary_budget"]["value"] == 5.0
    assert out["last_updated_date_time"].endswith("Z")


def test_entity_rows_target_shape_matches_stream_rows():
    ts = dt.datetime(2026, 9, 21, 13, tzinfo=dt.timezone.utc)
    rows = entity_rows("targets", [{"targetId": "9", "adGroupId": "8", "campaignId": "7", "adProduct": "SPONSORED_PRODUCTS",
                                    "state": "ENABLED", "bid": {"bid": 0.75, "currencyCode": "USD"},
                                    "targetDetails": {"keywordTarget": {"keyword": "gifts", "matchType": "EXACT"}}}], ts)
    (row,) = rows
    assert row["entity_type"] == "target" and row["entity_id"] == "9"
    assert row["campaign_id"] == "7" and row["ad_group_id"] == "8" and row["source"] == "snapshot"
    payload = row["payload"]
    assert isinstance(payload, dict)
    assert payload["target_details"]["keyword_target"]["match_type"] == "EXACT"
    assert payload["bid"]["currency_code"] == "USD"


def test_campaign_rows_have_no_ad_group():
    ts = dt.datetime(2026, 9, 21, 13, tzinfo=dt.timezone.utc)
    (row,) = entity_rows("campaigns", [{"campaignId": "7", "name": "x", "state": "PAUSED"}], ts)
    assert row["ad_group_id"] is None and row["campaign_id"] == "7" and row["entity_type"] == "campaign"
