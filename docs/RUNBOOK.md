# Amazon pipeline runbook — punlabs.AMZSales

Investigation date: **2026-09-10**. All findings verified directly against
BigQuery, not inherited from prior handoff notes.

---

## 1. Live incident: scheduled extraction stopped on 2026-08-25/26

### Symptom

Six tables stopped receiving data, and the daily run has had to be triggered by
hand ever since.

| Table | Last data | Last write |
|---|---|---|
| `PL-AMZSales-AdsInvoices` | 2026-08-22 | 2026-08-26 |
| `DailySales` | 2026-08-24 | 2026-08-25 |
| `PL-AMZSales-INVLedger` | 2026-08-24 | 2026-08-26 |
| `AWDInventoryDaily` | 2026-08-25 | 2026-08-26 |
| `PL-AMZSales-PendingFinances` | 2026-08-26 | 2026-08-26 |
| `AMZFinances` (derived CTAS) | — | 2026-08-25 |

Still current, because they are being run manually: `PL-AMZSales-AMZTransactions`,
`PL-AMZSales-AMZSettlements`, `DailyTraffic`, `sp_performance_master`
(all through 2026-09-08/09).

### Root cause

The pipeline is a `.ipynb` scheduled through **BigQuery Data Pipelines**, which
executes it on a **Colab Enterprise runtime**. That runtime's base image
previously shipped `google-cloud-secret-manager`; it no longer does. The
notebook's

```python
from google.cloud import secretmanager
```

now raises `ModuleNotFoundError` in the first cell. The run dies before issuing
a single BigQuery job.

### Why nobody noticed for 16 days

This is the same class of failure as the March outage, and it is worth stating
precisely because it will recur otherwise:

`INFORMATION_SCHEMA.JOBS` for the last 45 days shows
`amzsales@punlabs.iam.gserviceaccount.com` with **1 job on 2026-08-14, zero
since, and zero errors ever**. A pipeline that fails *before* it reaches
BigQuery leaves no error row. There is nothing to alert on, because nothing
happened. Only an explicit freshness expectation can catch it — see §3.

### Fix

`pipelines/lib/secrets.py` reads Secret Manager over its REST API using
`google.auth`, which is guaranteed present on the runtime (it is a hard
dependency of `google-cloud-bigquery`). It therefore cannot be removed by a
future base-image refresh.

Replace the import and the accessor in each notebook:

```python
# before
from google.cloud import secretmanager
client = secretmanager.SecretManagerServiceClient()
name = f"projects/punlabs/secrets/{secret_id}/versions/latest"
token = client.access_secret_version(name=name).payload.data.decode()

# after
from pipelines.lib.secrets import get_secret, preflight
preflight(["sp-api-refresh-token", "sp-api-client-secret"])   # first cell
token = get_secret("sp-api-refresh-token")
```

If you would rather not vendor the module into the notebook environment, the
one-line alternative is `%pip install google-cloud-secret-manager` as the first
cell. It works, but it re-installs on every run and reintroduces a dependency
on package-index reachability from the runtime's network — the REST approach
has neither drawback.

### Verify after fixing

1. Trigger the pipeline manually once and confirm it completes.
2. Confirm the service account is issuing jobs again:

   ```sql
   SELECT MAX(creation_time)
   FROM `region-us-central1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
   WHERE user_email = 'amzsales@punlabs.iam.gserviceaccount.com';
   ```

3. Check the schedule is still **enabled** — repeated failures can disable a
   Data Pipelines schedule, in which case fixing the notebook is not by itself
   enough to resume automation.
4. Confirm the runtime's service account still holds
   `roles/secretmanager.secretAccessor` on each secret. The REST path surfaces
   a clear HTTP 403 if it does not.

### Backfill

Once automation is restored, backfill 2026-08-25 → present for the five source
tables above. `AMZFinances` and `DailySales` are derived and should be rebuilt
*after* their sources are whole.

---

## 2. Ad spend data: do not deduplicate `sp_performance_master`

The prior handoff suggests residual duplicates need cleaning. **That is wrong,
and acting on it would corrupt ad spend.** Full evidence in
`sql/validation/ads_grain_reconciliation.sql`.

Summary: 305 key groups (401 surplus rows), all between 2026-03-04 and
2026-05-06, repeat on `(date, campaignId, advertisedSku)`. They carry different
impressions, clicks and cost — so they are not repeated loads. They are distinct
**ad-group-level** rows; the report grain is
`(date, campaign, ad group, advertised SKU)` and `adGroupId` was never carried
into the table.

Verified against two independent sources overlapping the window: daily cost for
2026-03-04..11 in `sp_performance_master` **as it stands** matches
`DailySPAdsConsAPI` and `DailySPAdsBySKU` to the cent. The deduplicated figure
is 2–6% lower on every affected day. Deduplicating would understate spend and
inflate every ROAS/ACoS number in Looker Studio.

Stronger still: running the reconciliation in
`sql/validation/ads_grain_reconciliation.sql` across **every** day the two
tables overlap returns **zero divergent rows**. The table as it stands is
correct in full, not merely on the sampled window.

**Correct remedy:** add `adGroupId` to the extract and the table, then key on
`(date, campaignId, adGroupId, advertisedSku)`. Implemented: run
`sql/migrations/2026-09-18_sp_performance_master_add_adgroup.sql` once, then
switch the daily load to `pipelines/sp_ads_daily.py` (see `docs/ADS_STREAM.md`).

### The May 2026 dedup was correct

For the avoidance of doubt: `sp_performance_master_backup_20260525` held 111,506
rows against master's current 31,241, and *that* table was genuinely corrupt —
Feb 13 and Mar 4–11 carried ~11,000 rows/day at 46–75× the true cost
(e.g. 2026-03-06: backup \$13,041 vs true \$250.79). The May fix removed real
duplication. It simply also left behind these 401 rows, which are not
duplication at all. No regression has occurred: **no repeated key has been
created since 2026-05-06**.

---

## 3. Known data gaps

`sp_performance_master` covers 2025-12-06 → 2026-09-08 but is missing 16 days
that were never backfilled:

```
2026-02-03, 02-08, 02-09, 02-17, 02-18, 02-20, 02-22 .. 02-28, 03-01 .. 03-03
```

The March 12 → May 24 gap described in the earlier handoff **has been
backfilled** and is no longer present.

---

## 3a. FBA stock by fulfillment center

`AMZSales.fba_inventory_by_fc` holds FBA units per day, SKU, disposition and
fulfillment center (FC code such as DET3). Loaded by
`pipelines/spapi/fba_inventory_by_fc.py`, which the Daily Inventory pipeline
runs right after the country ledger (inside `pipelines.spapi.fba_ledger.run`,
heartbeat `fba_inventory_by_fc`). No notebook change was needed.

- **Lag.** Amazon publishes FC-level days about 10 days after the country-level
  ledger. Each run requests the trailing 21 days and replaces only the dates
  Amazon returned, so late days fill in on later runs and a short report never
  deletes history. The freshness alert fires if the newest FC day is more than
  16 days old.
- **Views.** `v_fba_stock_by_fc_latest` (units and share of each SKU per FC on
  the newest published day) and `v_fba_stock_by_fc_daily` (per-FC totals and
  movements).
- **Check.** `sql/validation/fba_fc_vs_country.sql` must return no rows: FC
  totals equal the country ledger per day, SKU and disposition.
- **Backfill.** Amazon keeps about 18 months:
  `python -m pipelines.spapi.fba_inventory_by_fc --backfill --start YYYY-MM-DD --end YYYY-MM-DD`
  (one report per quarter; ledger reports have a rolling daily cap of roughly 10 requests).
- **Not yet:** FC code to city/state/region mapping, and AWD stock by
  warehouse (the AWD API has no location; see the AWD notes in docs).

---

## 4. Preventing the next silent failure

Both outages share one root cause: *nothing asserts that data should have
arrived.* Two cheap guards:

- **`sql/monitoring/freshness_check.sql`** — per-table staleness tolerances over
  `__TABLES__`. Schedule it; alert on any returned row. This alone would have
  caught both the March and August outages within a day.
- **`sql/monitoring/pipeline_run_log.sql`** + **`pipelines/lib/heartbeat.py`** —
  every run records SUCCESS/FAILED with a traceback, so a failure is visible
  even when it happens before any BigQuery work.

Also worth doing: `preflight()` at the top of each notebook, so a credential
problem aborts loudly instead of producing a partial load.

---

## 5. Unrelated / out of scope

- `punlabs.AMZSalesbyTransaction` — referenced in the forecasting handoff, **does
  not exist**. The nearest live table is `PL-AMZSales-AMZTransactions`. Any
  forecasting SQL pointing at the old name needs repointing.
- `ShopifySales`, `EtsySales`, `FaireSales`, `PopShopData` are dormant
  (152–349 days stale) and are not part of this incident.
- `date` is stored as `STRING` in `sp_performance_master` and `_sp_load_staging`;
  every query must `PARSE_DATE('%F', date)`. Worth migrating to `DATE`.
