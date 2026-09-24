-- Scheduled-query wrapper around freshness_check.sql that FAILS when any table
-- is stale. BigQuery scheduled queries only notify on failure, so raising is
-- how "alert on any row" actually reaches an inbox. Schedule daily after the
-- 07:00 and 15:00 ET pipelines (e.g. 17:00 ET) with email notifications on.
--
-- Stream tables are measured in hours, not days, so they get their own check.

WITH daily AS (
  SELECT table_id, pipeline, max_stale_days FROM UNNEST([
    STRUCT('PL-AMZSales-AMZTransactions' AS table_id, 'SP-API orders'        AS pipeline, 2 AS max_stale_days),
    STRUCT('PL-AMZSales-AMZSettlements',              'SP-API settlements',        3),
    STRUCT('PL-AMZSales-PendingFinances',             'SP-API pending finances',   3),
    STRUCT('PL-AMZSales-INVLedger',                   'SP-API inventory ledger',   3),
    STRUCT('AWDInventoryDaily',                       'AWD inventory',             3),
    STRUCT('fba_inventory_by_fc',                     'FBA stock by FC (loaded)',  3),
    STRUCT('DailyTraffic',                            'SP-API traffic',            3),
    STRUCT('DailySales',                              'Daily sales rollup',        3),
    STRUCT('AMZFinances',                             'Finances rollup',           3),
    STRUCT('sp_performance_master',                   'SP ads performance',        3),
    STRUCT('PL-AMZSales-AdsInvoices',                 'Ads invoices (monthly)',   35),
    STRUCT('ads_entity_log',                          'Ads campaign structure',   35),
    STRUCT('ads_sp_search_term_daily',                'Ads search terms',          3),
    STRUCT('ads_sp_placement_daily',                  'Ads placements',            3),
    STRUCT('catalog_snapshot',                        'Catalog snapshot',          2),
    STRUCT('pricing_daily',                           'Pricing snapshot',          2),
    STRUCT('ba_search_catalog_perf',                  'Brand Analytics SCP',      10),
    STRUCT('ba_search_query_perf',                    'Brand Analytics SQP',      10)
  ])
),
daily_stale AS (
  SELECT FORMAT('%s (%s) last written %t, %d days ago, tolerance %d',
                d.pipeline, d.table_id, DATE(TIMESTAMP_MILLIS(t.last_modified_time)),
                DATE_DIFF(CURRENT_DATE(), DATE(TIMESTAMP_MILLIS(t.last_modified_time)), DAY), d.max_stale_days) AS alert
  FROM daily d
  JOIN `punlabs.AMZSales.__TABLES__` t USING (table_id)
  WHERE DATE_DIFF(CURRENT_DATE(), DATE(TIMESTAMP_MILLIS(t.last_modified_time)), DAY) > d.max_stale_days
),
stream_stale AS (
  SELECT FORMAT('Marketing Stream %s last record %t, %d hours ago, tolerance 6',
                dataset_id, MAX(received_at), TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(received_at), HOUR)) AS alert
  FROM `punlabs.AMZSales.ads_stream_raw`
  WHERE dataset_id IN ('sp-traffic', 'sp-conversion')
  GROUP BY dataset_id
  HAVING TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(received_at), HOUR) > 6
),
-- FC-level ledger days publish ~10 days late; alert if the newest day falls further behind.
fc_lag AS (
  SELECT FORMAT('FBA stock by FC newest day is %t, %d days behind, tolerance 16', MAX(date),
                DATE_DIFF(CURRENT_DATE('America/Los_Angeles'), MAX(date), DAY)) AS alert
  FROM `punlabs.AMZSales.fba_inventory_by_fc`
  HAVING DATE_DIFF(CURRENT_DATE('America/Los_Angeles'), MAX(date), DAY) > 16
),
heartbeat_stale AS (
  SELECT FORMAT('no SUCCESS heartbeat for %s in the last 36 hours', pipeline) AS alert
  FROM UNNEST(['sp_orders_daily', 'sp_traffic_daily', 'sp_settlements_daily', 'sp_pending_finances_daily',
               'sp_ads_daily', 'amz_fba_inv_ledger', 'fba_inventory_by_fc', 'awd_inventory_daily',
               'qbo_daily', 'faire_daily', 'etsy_daily', 'shopify_daily']) AS pipeline
  WHERE pipeline NOT IN (
    SELECT pipeline FROM `punlabs.AMZSales.pipeline_run_log`
    WHERE status = 'SUCCESS' AND started_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 36 HOUR))
),
alerts AS (
  SELECT alert FROM daily_stale UNION ALL SELECT alert FROM stream_stale UNION ALL SELECT alert FROM fc_lag
  UNION ALL SELECT alert FROM heartbeat_stale
)
SELECT IF(COUNT(*) = 0, 'all pipelines fresh',
          ERROR(CONCAT(COUNT(*), ' stale pipeline(s): ', STRING_AGG(alert, ' | ')))) AS status
FROM alerts;
