-- Run-log table backing pipelines/lib/heartbeat.py
--
-- Create once, then have every scheduled notebook write a row per run. The
-- point is to turn "nothing happened" into a queryable fact: a pipeline that
-- never starts writes no rows anywhere, so only an explicit expectation can
-- detect it.

CREATE TABLE IF NOT EXISTS `punlabs.AMZSales.pipeline_run_log` (
  pipeline     STRING    NOT NULL OPTIONS(description="Logical pipeline name, e.g. sp_ads_daily"),
  run_id       STRING    NOT NULL OPTIONS(description="UUID for this execution"),
  started_at   TIMESTAMP NOT NULL,
  finished_at  TIMESTAMP,
  status       STRING    OPTIONS(description="SUCCESS or FAILED"),
  rows_written INT64,
  error        STRING,
  traceback    STRING
)
PARTITION BY DATE(started_at)
OPTIONS (
  description = "Heartbeat log for BigQuery Data Pipelines notebook runs.",
  partition_expiration_days = 365
);

-- ---------------------------------------------------------------------------
-- Alert query: pipelines with no successful run in the last 24 hours.
-- Schedule this and alert on any returned row.
-- ---------------------------------------------------------------------------
-- WITH expected AS (
--   SELECT pipeline FROM UNNEST([
--     'sp_ads_daily', 'sp_orders_daily', 'sp_settlements_daily',
--     'sp_inventory_ledger_daily', 'awd_inventory_daily', 'sp_traffic_daily'
--   ]) AS pipeline
-- ),
-- recent AS (
--   SELECT pipeline, MAX(started_at) AS last_success
--   FROM `punlabs.AMZSales.pipeline_run_log`
--   WHERE status = 'SUCCESS'
--     AND started_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
--   GROUP BY pipeline
-- )
-- SELECT
--   e.pipeline,
--   r.last_success,
--   TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.last_success, HOUR) AS hours_since_success
-- FROM expected e
-- LEFT JOIN recent r USING (pipeline)
-- WHERE r.last_success IS NULL
--    OR r.last_success < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);
