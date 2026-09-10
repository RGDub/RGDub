-- Ad spend reconciliation for punlabs.AMZSales.sp_performance_master
--
-- ============================================================================
-- WARNING: DO NOT DEDUPLICATE THIS TABLE ON (date, campaignId, advertisedSku).
-- ============================================================================
--
-- sp_performance_master contains ~305 key groups (401 surplus rows, all between
-- 2026-03-04 and 2026-05-06) where (date, campaignId, advertisedSku) repeats.
-- These are NOT duplicates. They are legitimately distinct rows at a finer
-- grain than the schema records - the Sponsored Products advertised-product
-- report is emitted per (date, campaign, ad group, advertised SKU), and the
-- ad group identifier was never carried into this table. Two ad groups in one
-- campaign advertising the same SKU therefore collapse onto one apparent key
-- while carrying genuinely different impressions, clicks and cost.
--
-- Proof, verified 2026-09-10 against two independent sources that overlap the
-- affected window (both DailySPAdsConsAPI and DailySPAdsBySKU run to
-- 2026-03-11). Daily cost, master AS-IS vs deduped vs the two references:
--
--   day         master(as-is)  deduped   ConsAPI   BySKU
--   2026-03-04     119.49      117.62    119.49   119.49
--   2026-03-05     195.05      190.51    195.05   195.05
--   2026-03-06     250.79      242.98    250.79   250.79
--   2026-03-07     273.83      269.14    273.83   273.83
--   2026-03-08     277.75      271.95    277.75   277.75
--   2026-03-09     177.06      170.95    177.06   177.06
--   2026-03-10     163.42      152.81    163.42   163.42
--   2026-03-11     205.93      194.52    205.93   205.93
--
-- The table as it stands matches both references exactly. Deduplicating would
-- understate ad spend by roughly 2-6% per affected day, silently inflating
-- every ROAS and ACoS figure downstream in Looker Studio.
--
-- The correct remedy is schema, not deletion: carry adGroupId through from the
-- report so the true grain is representable, then key on
-- (date, campaignId, adGroupId, advertisedSku).
--
-- ---------------------------------------------------------------------------
-- Query 1: daily reconciliation against the reference tables.
-- Any row returned is a real divergence worth investigating.
-- ---------------------------------------------------------------------------
WITH master AS (
  SELECT PARSE_DATE('%F', date) AS d, SUM(cost) AS cost, SUM(impressions) AS impressions
  FROM `punlabs.AMZSales.sp_performance_master`
  GROUP BY d
),
reference AS (
  SELECT date AS d, SUM(cost) AS cost, SUM(impressions) AS impressions
  FROM `punlabs.AMZSales.DailySPAdsConsAPI`
  GROUP BY d
)
SELECT
  m.d AS day,
  ROUND(m.cost, 2)  AS master_cost,
  ROUND(r.cost, 2)  AS reference_cost,
  ROUND(m.cost - r.cost, 2) AS cost_delta,
  m.impressions     AS master_impressions,
  r.impressions     AS reference_impressions
FROM master m
JOIN reference r USING (d)
WHERE ABS(m.cost - r.cost) > 0.01
ORDER BY day;

-- ---------------------------------------------------------------------------
-- Query 2: inventory of the repeated-key groups, for reference only.
-- Expect ~305 groups confined to 2026-03-04 .. 2026-05-06. If groups start
-- appearing AFTER 2026-05-06, that is a genuine regression in the load path
-- and worth investigating - the current loader has not produced any since.
-- ---------------------------------------------------------------------------
-- SELECT date, campaignId, advertisedSku, COUNT(*) AS rows_in_group
-- FROM `punlabs.AMZSales.sp_performance_master`
-- GROUP BY 1, 2, 3
-- HAVING COUNT(*) > 1
-- ORDER BY date DESC;
