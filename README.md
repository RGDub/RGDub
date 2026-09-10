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
| `sql/monitoring/freshness_check.sql` | Per-table staleness alerting. Schedule it; alert on any returned row. |
| `sql/monitoring/pipeline_run_log.sql` | DDL for the heartbeat table, plus its alert query. |
| `sql/validation/ads_grain_reconciliation.sql` | Ad spend reconciliation. **Read before touching `sp_performance_master`.** |

## Two things to know before changing anything

1. **Do not deduplicate `sp_performance_master` on
   `(date, campaignId, advertisedSku)`.** The repeated keys are real
   ad-group-level rows, not duplicates; collapsing them understates ad spend.
   Evidence is in `sql/validation/ads_grain_reconciliation.sql`.
2. **`punlabs.AMZSalesbyTransaction` does not exist.** Forecasting work
   referencing it should point at `PL-AMZSales-AMZTransactions`.
