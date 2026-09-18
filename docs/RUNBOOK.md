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
`(date, campaignId, adGroupId, advertisedSku)`.

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

## 5. Inventory ledger notebook — empty-report crash (2026-09-18)

### Symptom

`AMZ_FBA_INVLedger.ipynb` aborts after reporting a *successful* report job:

```
Status: IN_QUEUE
Status: DONE
EmptyDataError: No columns to parse from file
```

### Root cause

Not a transport failure and not a bug in the report. `DONE` plus an empty
document is SP-API's ordinary way of saying **the job succeeded and matched no
rows** — and an empty report document is a **zero-byte file**, not a
header-only TSV.

The notebook did anticipate an empty result:

```python
df = pd.read_csv(io.StringIO(raw_data), sep='\t')

if df.empty:
    print(f"! No data found for period {start_time_str}.")
```

but `read_csv` raises `EmptyDataError` on zero bytes, so it raised one line
*before* the guard. **That `if df.empty:` branch was unreachable**, and every
no-data day surfaced as a crash instead of a log line.

### Why the report came back empty

The window was a single day, `00:00:00`–`23:59:59`, two days back. The summary
view aggregates to whole periods and snaps to period boundaries, so a window
that does not cover a complete period can match nothing. Ranked by likelihood:

1. the window is too recent and the ledger has not settled (24–72h is typical,
   but it runs longer after a backlog);
2. the window does not span a whole aggregation period;
3. a misspelled key in `reportOptions` — SP-API **accepts and silently ignores**
   unknown option keys, so a typo produces a wrong-shaped or empty report with
   no error. Note the genuinely inconsistent prefixes in Amazon's own names:
   `aggregateByLocation` but `aggregat**ed**ByTimePeriod`.

The rewrite requests midnight-to-midnight over a 3-day range, and on an empty
result retries once over 14 days before concluding there is no data — which
separates cause 1 from a genuinely idle ledger instead of guessing.

### Three further defects in the same notebook

- **It still imported `google.cloud.secretmanager`.** This is the §1 outage.
  Whatever else was fixed, the scheduled run would still have died in cell 1.
  It now uses `pipelines.lib.secrets`.
- **`WRITE_APPEND` with no key.** Every manual re-run appended a second copy of
  the day. See §6 — this has already cost 1,374 duplicate rows. The load is now
  delete-then-append keyed on the dates being written, so re-running is safe.
- **`while True` with no deadline, re-minting an LWA token every 30 seconds.**
  A stuck report pinned the runtime indefinitely, and the poll loop made three
  Secret Manager calls plus an LWA exchange per iteration. `wait_for_report`
  takes a timeout; `LwaTokenProvider` caches the token.

The reusable parts live in `pipelines/lib/spapi_reports.py`; the other SP-API
notebooks should be moved onto it, since they share all four defects.

---

## 6. Inventory ledger: 1,374 duplicate rows — and why this is NOT §2

`PL-AMZSales-INVLedger` carries **1,374 surplus rows across 36 dates**
(2025-10-15 .. 2026-07-27), left by the keyless `WRITE_APPEND` described above
plus the many manual re-runs since March.

**§2 says do not deduplicate `sp_performance_master`. That warning does not
carry over to this table, and the difference is measured, not assumed:**

| | `sp_performance_master` (§2) | `PL-AMZSales-INVLedger` |
|---|---|---|
| Surplus rows on the natural key | 401 | 1,374 |
| Do the surplus rows carry different measures? | **Yes** — different impressions, clicks, cost | **No** — byte-identical, in every one of 1,374 cases |
| Verdict | Real finer grain (ad group). **Keep.** | Same fact loaded twice. **Safe to remove.** |

Two re-run signatures account for all of it, and neither touches a measure
column:

- **2026-03-11 .. 03-31 (1,247 rows)** — one copy has `Parent SKU` and
  `Inventory Binary` populated, the other has both `NULL`. Two notebook
  versions, one predating the enrichment step, loaded the same days.
- **2026-07-15 .. 07-17 (81 rows)** — both copies enriched, differing only in
  `Title`, because the ASIN's title was edited on Amazon between the two runs.

`sql/validation/invledger_duplicate_audit.sql` characterises this and carries
the remediation, **commented out and not run**. Before running it, execute its
query 1: `surplus_with_differing_measures` must be `0`. If it is ever non-zero,
stop — that would mean a real grain is hiding in these rows, and §2's lesson
*would* apply. Verified 2026-09-18: the dedup keeps 46,114 of 47,488 rows,
removing exactly the 1,374 the audit predicts.

---

## 7. Unrelated / out of scope

- `punlabs.AMZSalesbyTransaction` — referenced in the forecasting handoff, **does
  not exist**. The nearest live table is `PL-AMZSales-AMZTransactions`. Any
  forecasting SQL pointing at the old name needs repointing.
- `ShopifySales`, `EtsySales`, `FaireSales`, `PopShopData` are dormant
  (152–349 days stale) and are not part of this incident.
- `date` is stored as `STRING` in `sp_performance_master` and `_sp_load_staging`;
  every query must `PARSE_DATE('%F', date)`. Worth migrating to `DATE`.
