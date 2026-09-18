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

## What is already in place (verified 2026-09-18)

- **Ads API access exists.** Pun Labs LLC's Amazon Ads API registration was
  approved on 2026-03-10 (email "Amazon Ads API Registration Request
  Approved"). The LWA client id/secret and refresh token live in the Drive doc
  "Amazon Developer Credentials"; they need to move into Secret Manager (below).
- **The Make.com scenario is history.** The May 2026 handoff describes a
  Make.com scenario calling the *v2* `sp/productAds/report` endpoint. v2
  reporting is gone; everything below uses v3 `spAdvertisedProduct`.
- **The current loader is a daily 31-day reload, run by hand.** BigQuery job
  history shows the same sequence every day under `grant.weatherford@`:
  a LOAD into `_sp_load_staging`, then
  `BEGIN TRANSACTION; DELETE ... WHERE date BETWEEN today-31 AND today-1;
  INSERT ... SELECT * FROM _sp_load_staging; COMMIT`, then a duplicate check.
  That is already option A's shape; it just lacks `adGroupId` and any guard
  against a truncated report wiping the window.
- **Volume is small.** ~220 rows/day, ~140 campaigns, ~49 SKUs, $60–120/day.
- `pipeline_run_log` is live: `amz_fba_inv_ledger` and `awd_inventory_daily`
  already write heartbeats. `sp_ads_daily` and `ads_stream_poller` join them.
- Reference tables `DailySPAdsConsAPI` / `DailySPAdsBySKU` stopped on
  2026-03-11 and are only useful for reconciliation.

## Option A — daily reports with a rolling lookback (no new infrastructure)

This is what "continuous" means for reporting: the table is always complete
and always reflects Amazon's latest restatements. It is implemented in
`pipelines/sp_ads_daily.py` on top of `pipelines/lib/ads_api.py`.

### What the loader does

1. Requests one v3 `spAdvertisedProduct` report, `timeUnit=DAILY`,
   `groupBy=["advertiser"]`, for `[today-31, today-1]` — the longest attribution
   column in the table is `sales30d`, so a 31-day window catches every
   restatement. Requests are chunked at Amazon's 31-day per-report limit, so a
   backfill of any range works the same way.
2. Adds `adGroupName`, `adGroupId`, `adId` to the columns. This is the runbook's
   remedy for the "duplicates" in §2: the table can now represent its grain.
3. Transforms rows to the table's exact types, derives `Parent SKU` with the
   same suffix rule as before (`-FBA`, `-FBM`, `-UPC`, `-CORR`, any position).
4. **Refuses to load** if the report repeats the full key
   `(date, campaignId, adGroupId, advertisedSku)`, if it is empty, or if it has
   fewer than half the rows the master already holds for the window (a
   truncated report must never delete 31 days of history).
5. Loads `_sp_load_staging` (WRITE_TRUNCATE, explicit schema) and runs the same
   `DELETE window + INSERT` transaction the manual process uses, with named
   columns instead of `SELECT *`.
6. Writes a `pipeline_run_log` heartbeat either way.

### Setup, once

```bash
# 1. Credentials into Secret Manager (values from the "Amazon Developer
#    Credentials" Drive doc; then delete them from the doc).
for s in amazon-ads-client-id amazon-ads-client-secret amazon-ads-refresh-token amazon-ads-profile-id; do
  printf '%s' "$VALUE" | gcloud secrets create $s --project punlabs --data-file=-
  gcloud secrets add-iam-policy-binding $s --project punlabs \
    --member serviceAccount:amzsales@punlabs.iam.gserviceaccount.com \
    --role roles/secretmanager.secretAccessor
done
# Profile id, if unknown (pick the US seller profile's profileId):
#   python - <<'PY'
#   from pipelines.lib.secrets import get_secret
#   from pipelines.lib.ads_api import AdsApiClient
#   c = AdsApiClient(get_secret("amazon-ads-client-id"), get_secret("amazon-ads-client-secret"), get_secret("amazon-ads-refresh-token"))
#   print(c.list_profiles())
#   PY

# 2. Schema migration (adds the three grain columns; safe to re-run)
bq query --use_legacy_sql=false < sql/migrations/2026-09-18_sp_performance_master_add_adgroup.sql

# 3. Dry run: downloads, validates, writes nothing
python -m pipelines.sp_ads_daily --dry-run

# 4. Real run, then backfill adGroupId for the rest of the 95-day lookback
python -m pipelines.sp_ads_daily
python -m pipelines.sp_ads_daily --start 2026-06-16 --end 2026-08-16
```

### Schedule it

Put the module in the same BigQuery Data Pipelines notebook that runs the
other extracts, first cell:

```python
from pipelines.sp_ads_daily import run
run()
```

Then add `sql/validation/sp_ads_grain_check.sql` as a scheduled query after
the load and alert on any row, and keep `sql/monitoring/freshness_check.sql`
on `sp_performance_master` (tolerance 3 days).

Ads API limits that matter: 31 days per report request; 95-day lookback for
Sponsored Products; report generation is asynchronous and usually takes a few
minutes. The client polls with backoff (30 s → 5 min) and honours `429`
`Retry-After`.

Cost: nothing new. This runs inside the existing schedule.

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

### What is scaffolded in this repo

- `pipelines/ads_stream/subscribe.py` — creates one subscription per dataset
  against your SQS queue ARN, skipping ones that already exist.
- `pipelines/ads_stream/poller.py` — long-polls the queue, confirms the SNS
  subscription when that message arrives, streams records into
  `ads_stream_raw` keyed on `idempotency_id`, deletes messages only after
  BigQuery accepts them, and writes a heartbeat.
- `sql/ads_stream/ddl.sql` — the raw table, hourly traffic/conversion views
  that de-dup then SUM, and an `ads` entity view for the ad → SKU join.
- `sql/validation/ads_stream_vs_report.sql` — stream-vs-report reconciliation.

Neither script has run against a live queue yet (no AWS account is attached
to this project); the parsing and delete-after-insert logic is unit-tested in
`tests/test_ads_stream_poller.py`.

### AWS side, once

1. Create an SQS standard queue in `us-east-1` (NA profile), e.g.
   `amazon-marketing-stream`, with a dead-letter queue and a 14-day retention.
2. Queue policy: allow `sqs:SendMessage` from the Amazon Marketing Stream SNS
   principals for each dataset. The per-dataset Amazon account IDs are in the
   onboarding guide's "Amazon Marketing Stream datasets" table; copy them from
   there.
3. An IAM role with `sqs:ReceiveMessage`, `sqs:DeleteMessage`,
   `sqs:GetQueueAttributes` on that queue, trusted by
   `accounts.google.com` with an audience condition on the Cloud Run service
   account's unique id (web identity federation). No long-lived keys.
4. `python -m pipelines.ads_stream.subscribe create --queue-arn <arn>`
5. Deploy `poller.py` as a Cloud Run job, Cloud Scheduler every 5 minutes,
   `--max-seconds 240`.

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
