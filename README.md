# RGDub — Amazon data pipeline tooling

Support code for the Pop Colors / punlabs Amazon pipelines feeding
`punlabs.AMZSales` in BigQuery.

**Start with [`docs/RUNBOOK.md`](docs/RUNBOOK.md)** — it carries the current
diagnosis of the extraction outage, the fix, and two findings that contradict
earlier handoff notes.

## Layout

| Path | Purpose |
|---|---|
| `pipelines/lib/secrets.py` | Secret Manager access that survives Colab Enterprise base-image changes. Fixes the outage that stopped scheduled runs on 2026-08-25. |
| `pipelines/lib/heartbeat.py` | Per-run SUCCESS/FAILED logging so a dead pipeline is detectable. |
| `pipelines/lib/ads_api.py` | Amazon Ads API client: LWA auth, v3 async reports, Marketing Stream subscriptions. |
| `pipelines/sp_ads_daily.py` | Daily Sponsored Products loader for `sp_performance_master` (31-day reload, ad-group grain, guarded). Replaces the hand-run cells. |
| `pipelines/ads_stream/` | Amazon Marketing Stream: subscription CLI and SQS → BigQuery poller. |
| `sql/migrations/` | One-off schema changes. Run `2026-09-18_sp_performance_master_add_adgroup.sql` before the first `sp_ads_daily` run. |
| `sql/ads_stream/ddl.sql` | Landing table and hourly views for Marketing Stream. |
| `sql/monitoring/freshness_check.sql` | Per-table staleness alerting. Schedule it; alert on any returned row. |
| `sql/monitoring/pipeline_run_log.sql` | DDL for the heartbeat table, plus its alert query. |
| `sql/validation/ads_grain_reconciliation.sql` | Ad spend reconciliation. **Read before touching `sp_performance_master`.** |
| `sql/validation/sp_ads_grain_check.sql` | Post-load uniqueness check on the full ad-group grain. |
| `docs/ADS_STREAM.md` | How to get Amazon Ads data continuously: daily reports with a rolling lookback vs. Amazon Marketing Stream (hourly push), and how each lands in BigQuery. |

## Two things to know before changing anything

1. **Do not deduplicate `sp_performance_master` on
   `(date, campaignId, advertisedSku)`.** The repeated keys are real
   ad-group-level rows, not duplicates; collapsing them understates ad spend.
   Evidence is in `sql/validation/ads_grain_reconciliation.sql`.
2. **`punlabs.AMZSalesbyTransaction` does not exist.** Forecasting work
   referencing it should point at `PL-AMZSales-AMZTransactions`.

## Running the tests

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python -m pytest -q
```
