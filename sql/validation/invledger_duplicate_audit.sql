-- Duplicate audit for punlabs.AMZSales.PL-AMZSales-INVLedger
--
-- Context
-- -------
-- The ledger notebook loaded with a bare WRITE_APPEND and no key, so every
-- re-run appended a second copy of a day. Between the March and August 2026
-- outages the pipeline was re-run by hand many times, and 1,374 surplus rows
-- accumulated across 36 dates (2025-10-15 .. 2026-07-27).
--
-- IMPORTANT - this is NOT the ads situation
-- -----------------------------------------
-- `docs/RUNBOOK.md` §2 warns, correctly, that the surplus rows in
-- `sp_performance_master` are real ad-group-level rows and must NOT be
-- deduplicated. That warning does not carry over to this table, and the
-- difference is measurable rather than a judgement call:
--
--   * In `sp_performance_master`, surplus rows on the key carry DIFFERENT
--     impressions, clicks and cost - they are a finer grain the table failed
--     to capture.
--   * Here, query 1 below returns `surplus_with_differing_measures = 0`. Every
--     surplus row carries byte-identical ledger measures to its twin. There is
--     no hidden grain; they are the same fact loaded twice.
--
-- Run query 1 before acting. If it ever returns a non-zero count in the second
-- column, STOP - that would mean a real finer grain exists (a second Location
-- or Store dimension, say) and deduplicating would destroy it.

-- ---------------------------------------------------------------------------
-- 1. Characterise the duplication. Run this first, and on any re-audit.
-- ---------------------------------------------------------------------------
WITH keyed AS (
  SELECT
    Date, FNSKU, MSKU, Disposition, Location,
    -- Every measure column the report supplies. If two rows share the natural
    -- key AND this string, they are the same fact twice over.
    FORMAT('%t|%t|%t|%t|%t|%t|%t|%t|%t|%t|%t|%t|%t',
           `Starting Warehouse Balance`, `In Transit Between Warehouses`,
           Receipts, Shipments, CustomerReturns, VendorReturns, WhseTransfers,
           Found, Lost, Damaged, Disposed, Adjustments,
           `Ending Warehouse Balance`) AS measures
  FROM `punlabs.AMZSales.PL-AMZSales-INVLedger`
),
grp AS (
  SELECT
    Date, FNSKU, MSKU, Disposition, Location,
    COUNT(*) AS copies,
    COUNT(DISTINCT measures) AS distinct_measure_sets
  FROM keyed
  GROUP BY Date, FNSKU, MSKU, Disposition, Location
)
SELECT
  SUM(IF(copies > 1 AND distinct_measure_sets = 1, copies - 1, 0))
    AS safe_to_remove_same_measures,
  -- Must be 0. Anything else means a real grain is hiding in these rows.
  SUM(IF(copies > 1 AND distinct_measure_sets > 1, copies - 1, 0))
    AS surplus_with_differing_measures,
  COUNT(DISTINCT IF(copies > 1, Date, NULL)) AS affected_dates
FROM grp;
-- Verified 2026-09-18: 1374 | 0 | 36

-- ---------------------------------------------------------------------------
-- 2. How the two loads differ, per affected date. Diagnostic only.
--
-- Two distinct re-run signatures show up:
--   * 2026-03-11 .. 03-31 (1,247 rows) - one copy has `Parent SKU` and
--     `Inventory Binary` populated, the other has both NULL. Two notebook
--     versions, one predating the enrichment step, loaded the same days.
--   * 2026-07-15 .. 07-17 (81 rows) - both copies enriched, differing only in
--     `Title`, because the ASIN's title was edited on Amazon between runs.
-- Neither signature touches a measure column.
-- ---------------------------------------------------------------------------
SELECT
  Date,
  COUNT(*) AS n_rows,
  COUNTIF(`Parent SKU` IS NULL) AS rows_missing_parent_sku,
  COUNT(DISTINCT FORMAT('%t|%t|%t|%t', FNSKU, MSKU, Disposition, Location))
    AS distinct_natural_keys,
  COUNT(*) - COUNT(DISTINCT FORMAT('%t|%t|%t|%t', FNSKU, MSKU, Disposition, Location))
    AS surplus
FROM `punlabs.AMZSales.PL-AMZSales-INVLedger`
GROUP BY Date
HAVING surplus > 0
ORDER BY Date;

-- ---------------------------------------------------------------------------
-- 3. Remediation. NOT run automatically - review query 1 first, and snapshot.
--
--   CREATE TABLE `punlabs.AMZSales.PL-AMZSales-INVLedger_backup_20260918`
--   AS SELECT * FROM `punlabs.AMZSales.PL-AMZSales-INVLedger`;
--
-- Keeps the richest copy of each natural key: prefer a row that has
-- `Parent SKU` populated, then the longest `Title` (the July pairs differ only
-- there, and the longer title is the current one). Because every surplus row
-- has identical measures, which copy survives cannot change any total.
-- ---------------------------------------------------------------------------
-- CREATE OR REPLACE TABLE `punlabs.AMZSales.PL-AMZSales-INVLedger` AS
-- SELECT * EXCEPT (_rn) FROM (
--   SELECT t.*, ROW_NUMBER() OVER (
--            PARTITION BY Date, FNSKU, MSKU, Disposition, Location
--            ORDER BY IF(`Parent SKU` IS NULL, 1, 0), LENGTH(Title) DESC
--          ) AS _rn
--   FROM `punlabs.AMZSales.PL-AMZSales-INVLedger` t
-- )
-- WHERE _rn = 1;

-- ---------------------------------------------------------------------------
-- 4. Post-remediation check. Both columns must be 0, and the row count must
--    fall by exactly the figure query 1 reported.
-- ---------------------------------------------------------------------------
-- Re-run query 1: safe_to_remove_same_measures and affected_dates should be 0.
