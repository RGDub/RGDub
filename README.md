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
| `pipelines/lib/spapi_reports.py` | SP-API report request/poll/download. Treats an empty report as no data instead of crashing, times out a stuck poll, caches the LWA token. |
| `pipelines/notebooks/AMZ_FBA_INVLedger.ipynb` | Inventory ledger load, rewritten. Idempotent (delete-then-append by date), so re-running is safe. |
| `sql/monitoring/freshness_check.sql` | Per-table staleness alerting. Schedule it; alert on any returned row. |
| `sql/monitoring/pipeline_run_log.sql` | DDL for the heartbeat table, plus its alert query. |
| `sql/validation/ads_grain_reconciliation.sql` | Ad spend reconciliation. **Read before touching `sp_performance_master`.** |
| `sql/validation/invledger_duplicate_audit.sql` | Inventory ledger duplicate audit + remediation (not run). |

## Three things to know before changing anything

1. **Do not deduplicate `sp_performance_master` on
   `(date, campaignId, advertisedSku)`.** The repeated keys are real
   ad-group-level rows, not duplicates; collapsing them understates ad spend.
   Evidence is in `sql/validation/ads_grain_reconciliation.sql`.
2. **`punlabs.AMZSalesbyTransaction` does not exist.** Forecasting work
   referencing it should point at `PL-AMZSales-AMZTransactions`.
3. **The ledger table *is* safe to deduplicate — rule 1 does not generalise.**
   Its 1,374 surplus rows carry byte-identical measures, unlike the ads rows.
   Confirm with `sql/validation/invledger_duplicate_audit.sql` query 1 first;
   `surplus_with_differing_measures` must be `0`. See `docs/RUNBOOK.md` §6.
