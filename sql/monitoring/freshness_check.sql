-- Pipeline freshness monitor for punlabs.AMZSales
--
-- Detects the failure mode that hid the March and August 2026 outages: a table
-- that quietly stops receiving rows. Absence of data raises no error, so it has
-- to be asserted against an expected cadence.
--
-- Run as a BigQuery scheduled query; alert whenever it returns any row.
-- Every row it returns is a pipeline that has missed its SLA.

WITH expectations AS (
  -- table_id, human-readable owner, max tolerated staleness in days.
  -- Tolerances allow for Amazon's own reporting lag plus a day of slack.
  SELECT * FROM UNNEST([
    STRUCT('PL-AMZSales-AMZTransactions' AS table_id, 'SP-API orders'        AS pipeline, 2 AS max_stale_days),
    STRUCT('PL-AMZSales-AMZSettlements',              'SP-API settlements',        3),
    STRUCT('PL-AMZSales-PendingFinances',             'SP-API pending finances',   3),
    STRUCT('PL-AMZSales-INVLedger',                   'SP-API inventory ledger',   3),
    STRUCT('AWDInventoryDaily',                       'AWD inventory',             3),
    STRUCT('DailyTraffic',                            'SP-API traffic',            3),
    STRUCT('DailySales',                              'Daily sales rollup',        3),
    STRUCT('sp_performance_master',                   'SP ads performance',        3),
    STRUCT('_sp_load_staging',                        'SP ads staging',            3),
    STRUCT('PL-AMZSales-AdsInvoices',                 'Ads invoices (monthly)',   35)
  ])
),
actuals AS (
  SELECT
    table_id,
    DATE(TIMESTAMP_MILLIS(last_modified_time)) AS last_written,
    DATE_DIFF(CURRENT_DATE(), DATE(TIMESTAMP_MILLIS(last_modified_time)), DAY) AS days_stale,
    row_count
  FROM `punlabs.AMZSales.__TABLES__`
)
SELECT
  e.pipeline,
  e.table_id,
  a.last_written,
  a.days_stale,
  e.max_stale_days,
  a.row_count,
  FORMAT('%s has not been written in %d days (tolerance %d)',
         e.pipeline, a.days_stale, e.max_stale_days) AS alert
FROM expectations e
JOIN actuals a USING (table_id)
WHERE a.days_stale > e.max_stale_days
ORDER BY a.days_stale DESC;
