# Continuous Amazon Ads data — design note

Written 2026-09-18. Question: how do we download Amazon Ads data continuously
rather than as a once-a-day report pull?

Short answer: there are two different things "continuous" can mean, and they
need different plumbing. Both are laid out below, with a recommendation at the
end.

| | A. Daily reports, re-pulled with a lookback | B. Amazon Marketing Stream |
|---|---|---|
| Granularity | Daily | Hourly (traffic/conversions), event-driven (entities, budgets) |
| Latency | Impressions/clicks ~12 h; conversions ~24 h | Minutes |
| Transport | Pull: Ads API `POST /reporting/reports`, poll, download gzip JSON | Push: Amazon → SNS → your AWS SQS queue or Data Firehose |
| Infra outside GCP | None | An AWS account (one SQS queue + an IAM principal is enough) |
| Restatements | You re-download the trailing window every day | Delta messages arrive for the affected hour; you sum them |
| Grain | (date, campaign, ad group, advertised SKU) | (hour, campaign, ad group, **ad**, target, placement) — SKU comes from a join |
| What we have today | This, minus `adGroupId` (see RUNBOOK §2) | Nothing |

## What is already in place

`sp_performance_master` is fed from the Sponsored Products advertised-product
report (v3 `spAdvertisedProduct`, `timeUnit=DAILY`). `_sp_load_staging` holds
6,184 rows against the master's 5,884 rows for the trailing 30 days, so the
current notebook already re-pulls roughly a 30-day window each run and merges
it. That is the right shape for option A; it is just missing `adGroupId`
in the key.

Reference tables `DailySPAdsConsAPI` / `DailySPAdsBySKU` stopped on
2026-03-11 and are only useful for reconciliation.

## Option A — daily reports with a rolling lookback (no new infrastructure)

This is what "continuous" means for reporting: the table is always complete
and always reflects Amazon's latest restatements.

1. **Key on the true grain.** Add `adGroupId` (and `adGroupName`) to the
   report's `columns` and to the table. Key becomes
   `(date, campaignId, adGroupId, advertisedSku)`. This is the fix the runbook
   already calls for and it is a prerequisite for everything below, because a
   MERGE on the current key would collapse the ad-group rows.
2. **Re-pull a trailing window daily.** Amazon restates conversion columns for
   up to 30 days after the click (the `sales30d` / `purchases30d` columns are
   literally attribution windows). Pull `[today-31, today-1]` every run and
   `MERGE` on the full key. Impressions and clicks settle within a day or two;
   conversions keep moving. A 31-day window covers the longest attribution
   column in the table.
3. **Migrate `date` to `DATE`** while touching the schema, so the MERGE join
   doesn't need `PARSE_DATE` on both sides and partition pruning works. Partition
   the table by `date`, cluster by `campaignId, adGroupId`.
4. **Wrap the run** in `run_logged()` and `preflight()` from `pipelines/lib` so
   a silent failure is visible.

Sketch of the merge:

```sql
MERGE `punlabs.AMZSales.sp_performance_master` m
USING `punlabs.AMZSales._sp_load_staging` s
ON  m.date = s.date
AND m.campaignId = s.campaignId
AND m.adGroupId = s.adGroupId
AND m.advertisedSku = s.advertisedSku
WHEN MATCHED THEN UPDATE SET
  impressions = s.impressions, clicks = s.clicks, cost = s.cost, spend = s.spend,
  sales1d = s.sales1d, sales7d = s.sales7d, sales14d = s.sales14d, sales30d = s.sales30d,
  purchases1d = s.purchases1d, purchases7d = s.purchases7d,
  purchases14d = s.purchases14d, purchases30d = s.purchases30d
  -- ...and the remaining attributed columns
WHEN NOT MATCHED THEN INSERT ROW;
```

Ads API limits that matter: one report request covers at most 31 days; the SP
lookback is 95 days (SB/SD are longer); report generation is asynchronous and
usually takes a few minutes, so poll `GET /reporting/reports/{id}` with backoff
(30 s → 5 min) rather than a tight loop, and honour `429` `Retry-After`.

Cost: nothing new. This runs inside the existing BigQuery Data Pipelines
schedule.

## Option B — Amazon Marketing Stream (hourly push)

Marketing Stream is Amazon's push feed. It delivers Sponsored Products /
Brands / Display traffic and conversion records at hourly grain within minutes
of the hour closing, plus event-driven datasets: `campaigns`, `adgroups`, `ads`,
`targets` (entity changes), `budget-usage` (budget consumed per campaign,
published as it changes), and `sp-budget-recommendations`. There are ~45
dataset variants across the NA/EU/FE regions; the ones we'd want are
`sp-traffic`, `sp-conversion`, `budget-usage`, and the entity datasets.

### Non-negotiables

- **Destinations are AWS-only**: an SQS queue or an Amazon Data Firehose
  delivery stream in your AWS account, in the region that matches the Ads
  profile (NA → `us-east-1`). There is no GCP or webhook destination. So an AWS
  account is unavoidable, but it can be a nearly empty one.
- **Ads API app must be enabled for Marketing Stream.** Same LWA client
  ID/secret and refresh token the report pulls use; check in the Ads API
  console that the app has Marketing Stream access, and request it if not.
- **Subscriptions are per profile and per dataset.** One
  `POST /streams/subscriptions` call per dataset with the queue/stream ARN.
  Amazon then sends an SNS `SubscriptionConfirmation` message to the queue and
  nothing else flows until a consumer fetches its `SubscribeURL`.
- **Queue policy must allow Amazon's SNS topics** to `sqs:SendMessage`. The
  per-dataset Amazon AWS account IDs are listed in the onboarding docs; copy
  them from there rather than from memory, they differ by dataset and region.
- **Messages are deltas, not snapshots.** Each record carries an
  `idempotency_id` and a `time_window_start`. The total for an hour is the
  `SUM` of all records for that hour after de-duplicating on `idempotency_id`.
  Negative values are legitimate (a restatement). Never "upsert latest".
- **No SKU in the stream.** `sp-traffic`/`sp-conversion` are keyed by
  `ad_id`. Advertised SKU/ASIN comes from the `ads` entity dataset or a nightly
  pull of product ads. Keep `dim_sp_ads(ad_id, ad_group_id, campaign_id, sku,
  asin, valid_from, valid_to)` and join at query time.

### Getting it into BigQuery: three bridges

1. **SQS long-polled from GCP (recommended).** AWS holds only the SQS queue and
   an IAM role. A Cloud Run service (or a Cloud Run job on a 1–5 min Cloud
   Scheduler cadence) calls `ReceiveMessage` with 20 s long polling, writes
   batches to a raw BigQuery table via the Storage Write API, then deletes the
   messages. Authenticate to AWS with **web identity federation**: AWS trusts
   `accounts.google.com` as an OIDC provider, so the Cloud Run service account's
   ID token can `AssumeRoleWithWebIdentity` with no long-lived AWS keys. If that
   is more setup than wanted on day one, an IAM user access key in Secret
   Manager (read via `pipelines/lib/secrets.py`) works and can be swapped later.
   Handles the `SubscriptionConfirmation` message in the same loop.
2. **Firehose → S3 → BigQuery Data Transfer Service.** No consumer code at all:
   Firehose lands newline-delimited JSON in S3 every 60–900 s, and a scheduled
   S3 transfer loads it into BigQuery. Simplest to operate, slowest (hours, since
   DTS S3 transfers run at most every 24 h unless triggered by API… check the
   current minimum) and needs S3 lifecycle cleanup. Firehose subscriptions skip
   the SNS confirmation step but require two IAM roles (subscriber and
   subscription roles) that Amazon's principal assumes.
3. **SQS → Lambda → BigQuery.** The pattern in Amazon's reference
   implementation (amzn/amazon-marketing-stream-examples, CDK). Puts compute
   and a GCP service-account key in AWS. More to run and to secure than option 1
   for no gain, given everything else is on GCP.

### Tables

Raw, append-only, partitioned by hour, keeps the message envelope:

```sql
CREATE TABLE `punlabs.AMZSales.ads_stream_raw` (
  dataset_id        STRING NOT NULL,   -- sp-traffic, sp-conversion, budget-usage, ...
  idempotency_id    STRING NOT NULL,
  advertiser_id     STRING,
  marketplace_id    STRING,
  time_window_start TIMESTAMP,         -- hourly datasets; NULL for entity events
  received_at       TIMESTAMP NOT NULL,
  campaign_id       STRING,
  ad_group_id       STRING,
  ad_id             STRING,
  target_id         STRING,
  placement         STRING,
  payload           JSON NOT NULL      -- the full record as delivered
)
PARTITION BY TIMESTAMP_TRUNC(received_at, DAY)
CLUSTER BY dataset_id, time_window_start;
```

Hourly view, de-duplicated then summed (the only correct way to read deltas):

```sql
CREATE OR REPLACE VIEW `punlabs.AMZSales.v_sp_traffic_hourly` AS
WITH dedup AS (
  SELECT * EXCEPT(rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY idempotency_id ORDER BY received_at) AS rn
    FROM `punlabs.AMZSales.ads_stream_raw`
    WHERE dataset_id = 'sp-traffic'
  ) WHERE rn = 1
)
SELECT
  time_window_start, campaign_id, ad_group_id, ad_id, target_id, placement,
  SUM(INT64(payload.impressions)) AS impressions,
  SUM(INT64(payload.clicks))      AS clicks,
  SUM(FLOAT64(payload.cost))      AS cost
FROM dedup
GROUP BY 1, 2, 3, 4, 5, 6;
```

Do the same for `sp-conversion` (its `time_window_start` is the **click**
hour; conversions arrive later and add to it, which is why summing works).

### Reconcile against the daily report

Stream and report should agree on impressions/clicks/cost per day per campaign
once the day is a couple of days old. Add a scheduled query like
`sql/validation/ads_grain_reconciliation.sql` that compares the hourly view
rolled up to day against `sp_performance_master`; alert on divergence > $0.01.
Conversions will differ by attribution timing for up to 30 days and should be
compared only on closed windows.

### Freshness

Add rows to `sql/monitoring/freshness_check.sql` for `ads_stream_raw`. The
tolerance is hours, not days, so use a separate query on
`MAX(received_at)` per `dataset_id` (e.g. `sp-traffic` older than 3 h → alert).
Also log the poller through `pipelines/lib/heartbeat.py`.

### Rough cost

Amazon charges nothing for the stream. AWS: SQS is ~\$0.40 per million
requests; at our volume (162 campaigns, 49 SKUs, ~200 rows/day at daily grain,
so a few thousand hourly delta records/day) it rounds to a dollar a month.
Cloud Run polling every minute is a few dollars a month. BigQuery storage is
negligible.

## Recommendation

1. **Do option A now.** It is a small change to the notebook we already run
   (add `adGroupId`, MERGE on the full key, 31-day lookback, `DATE` column) and
   it removes the grain problem the runbook warns about. This alone gives a
   continuously correct daily table.
2. **Do option B only if something needs intraday data**: budget pacing,
   bid changes reacting to the current day, or dayparting analysis. If the goal
   is dashboards and forecasting off `PunDataDaily`, option A is sufficient and
   B adds an AWS account to look after for no benefit.
3. If B is wanted, use bridge 1 (SQS long-polled from Cloud Run with web
   identity federation), subscribe to `sp-traffic`, `sp-conversion`,
   `budget-usage`, and the `ads`/`adgroups`/`campaigns` entity datasets, and keep
   option A running as the source of truth for daily totals.

## Open questions to settle before building B

- Which Ads profile(s)/marketplaces? One NA profile means one `us-east-1`
  queue; EU marketplaces need a second queue in `eu-west-1`.
- Is there an existing AWS account for the business, or does one need creating
  under the company's billing?
- Does the Ads API app already show Marketing Stream access in the console?
