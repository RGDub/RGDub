import json
from unittest import mock

import pytest

from pipelines.ads_stream import poller


def test_parse_confirmation():
    body = json.dumps({"Type": "SubscriptionConfirmation", "SubscribeURL": "https://sns/confirm?x=1"})
    assert poller.parse_sqs_message(body) == ("confirm", "https://sns/confirm?x=1")


def test_parse_notification_unwraps_record():
    record = {"dataset_id": "sp-traffic", "idempotency_id": "abc", "clicks": 2}
    body = json.dumps({"Type": "Notification", "Message": json.dumps(record)})
    assert poller.parse_sqs_message(body) == ("record", record)


def test_record_to_row_lifts_keys_and_keeps_payload():
    record = {
        "dataset_id": "sp-traffic", "idempotency_id": "id-1", "advertiser_id": "adv",
        "marketplace_id": "ATVPDKIKX0DER", "time_window_start": "2026-09-17T14:00:00Z",
        "campaign_id": 111, "ad_group_id": 222, "ad_id": 333, "keyword_id": 444,
        "placement": "TOP_OF_SEARCH", "impressions": 5, "clicks": 1, "cost": 0.5,
    }
    row = poller.record_to_row(record)
    assert row["target_id"] == "444"                  # keyword_id falls into target_id
    assert row["campaign_id"] == "111"
    assert row["time_window_start"] == "2026-09-17T14:00:00Z"
    assert json.loads(row["payload"]) == record
    assert row["received_at"].endswith("+00:00")


def test_record_without_dataset_is_rejected():
    with pytest.raises(ValueError):
        poller.record_to_row({"idempotency_id": "x"})


def test_drain_confirms_inserts_and_deletes():
    record = {"dataset_id": "sp-traffic", "idempotency_id": "id-9", "clicks": 1}
    sqs = mock.Mock()
    sqs.receive_message.side_effect = [
        {"Messages": [
            {"MessageId": "m1", "ReceiptHandle": "h1",
             "Body": json.dumps({"Type": "SubscriptionConfirmation", "SubscribeURL": "https://sns/c"})},
            {"MessageId": "m2", "ReceiptHandle": "h2",
             "Body": json.dumps({"Type": "Notification", "Message": json.dumps(record)})},
        ]},
        {},
    ]
    bq = mock.Mock()
    bq.insert_rows_json.return_value = []
    with mock.patch.object(poller, "confirm_subscription") as confirm:
        written = poller.drain(sqs, "https://sqs/q", bq, max_seconds=60)

    confirm.assert_called_once_with("https://sns/c")
    assert written == 1
    rows = bq.insert_rows_json.call_args.args[1]
    assert rows[0]["idempotency_id"] == "id-9"
    assert bq.insert_rows_json.call_args.kwargs["row_ids"] == ["id-9"]
    deleted = sqs.delete_message_batch.call_args.kwargs["Entries"]
    assert {e["Id"] for e in deleted} == {"m1", "m2"}


def test_drain_does_not_delete_when_bigquery_rejects():
    record = {"dataset_id": "sp-traffic", "idempotency_id": "id-9"}
    sqs = mock.Mock()
    sqs.receive_message.return_value = {"Messages": [
        {"MessageId": "m2", "ReceiptHandle": "h2",
         "Body": json.dumps({"Type": "Notification", "Message": json.dumps(record)})},
    ]}
    bq = mock.Mock()
    bq.insert_rows_json.return_value = [{"index": 0, "errors": ["bad"]}]
    with pytest.raises(RuntimeError):
        poller.drain(sqs, "https://sqs/q", bq, max_seconds=60)
    sqs.delete_message_batch.assert_not_called()


def test_record_without_idempotency_id_gets_stable_content_hash():
    from pipelines.ads_stream.poller import record_to_row

    rec = {"dataset_id": "budget-usage", "advertiser_id": "1", "budget_scope_id": "42",
           "budget_usage_percentage": 55.0, "usage_updated_timestamp": "2026-09-21T13:00:00Z"}
    a = record_to_row(dict(rec))
    b = record_to_row(dict(rec))
    assert a["idempotency_id"] == b["idempotency_id"]
    assert a["idempotency_id"].startswith("sha256:")
    assert record_to_row({**rec, "budget_usage_percentage": 60.0})["idempotency_id"] != a["idempotency_id"]


def test_entity_row_for_target_change_and_none_for_traffic():
    from pipelines.ads_stream.poller import entity_row

    rec = {"dataset_id": "ads-campaign-management-targets", "target_id": "9", "ad_group_id": "8",
           "campaign_id": "7", "ad_product": "SPONSORED_PRODUCTS", "state": "ENABLED",
           "last_updated_date_time": "2026-09-21T13:00:00Z",
           "bid": {"bid": 0.75, "currency_code": "USD"},
           "target_details": {"keyword_target": {"keyword": "gifts", "match_type": "EXACT"}}}
    row = entity_row(rec)
    assert row["entity_type"] == "target" and row["entity_id"] == "9"
    assert row["campaign_id"] == "7" and row["ad_group_id"] == "8"
    assert row["source"] == "stream" and row["state"] == "ENABLED"
    assert '"keyword":"gifts"' in row["payload"]
    assert entity_row({"dataset_id": "sp-traffic", "idempotency_id": "x"}) is None


def test_raw_budget_usage_message_without_envelope_is_a_record():
    from pipelines.ads_stream.poller import parse_sqs_message, record_to_row

    body = '{"dataset_id": "budget-usage", "budget_scope_id": "75799706059736", "budget_scope_type": "CAMPAIGN", "budget": 5.0, "budget_usage_percentage": 5.79, "usage_updated_timestamp": "2026-09-21T13:04:36Z"}'
    kind, rec = parse_sqs_message(body)
    assert kind == "record" and rec["dataset_id"] == "budget-usage"
    row = record_to_row(rec)
    assert row["dataset_id"] == "budget-usage" and row["idempotency_id"].startswith("sha256:")
