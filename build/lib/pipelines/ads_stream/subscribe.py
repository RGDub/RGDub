"""Create / list Amazon Marketing Stream subscriptions for the punlabs Ads profile.

One subscription per dataset per profile. Amazon pushes to an SQS queue in
your AWS account; ``poller.py`` drains that queue into BigQuery.

    python -m pipelines.ads_stream.subscribe list
    python -m pipelines.ads_stream.subscribe create \
        --queue-arn arn:aws:sqs:us-east-1:123456789012:amazon-marketing-stream \
        --datasets sp-traffic sp-conversion budget-usage ads-campaign-management-campaigns

The queue must already allow Amazon's SNS topics to ``sqs:SendMessage`` (see
docs/ADS_STREAM.md). After ``create`` run the poller: it confirms the SNS
subscription automatically when the confirmation message arrives.
"""

from __future__ import annotations

import argparse
import json
import logging

from pipelines.lib.ads_api import ads_client_from_secrets

DEFAULT_DATASETS = [
    "sp-traffic", "sp-conversion", "budget-usage",
    "ads-campaign-management-campaigns", "ads-campaign-management-adgroups",
    "ads-campaign-management-ads", "ads-campaign-management-targets",
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    create = sub.add_parser("create")
    create.add_argument("--queue-arn", required=True)
    create.add_argument("--datasets", nargs="+", default=DEFAULT_DATASETS)
    create.add_argument("--notes", default="punlabs BigQuery ingestion")
    parser.add_argument("--region", default="NA")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    client = ads_client_from_secrets(region=args.region)
    if args.cmd == "list":
        print(json.dumps(client.list_stream_subscriptions(), indent=2))
        return

    existing = {
        (s.get("dataSetId"), s.get("destinationArn"))
        for s in client.list_stream_subscriptions()
        if s.get("status") not in ("ARCHIVED",)
    }
    for dataset in args.datasets:
        if (dataset, args.queue_arn) in existing:
            print(f"{dataset}: already subscribed")
            continue
        result = client.create_stream_subscription(dataset, args.queue_arn, notes=args.notes)
        print(f"{dataset}: {json.dumps(result)}")


if __name__ == "__main__":
    main()
