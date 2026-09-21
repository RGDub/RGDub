-- Amazon Marketing Stream landing table and read views.
--
-- ads_stream_raw is append-only. Records are DELTAS: Amazon may send several
-- records for the same hour and key (including negative adjustments). The
-- truth for an hour is SUM over de-duplicated records, never "latest wins".

CREATE TABLE IF NOT EXISTS `punlabs.AMZSales.ads_stream_raw` (
  dataset_id        STRING    NOT NULL,   -- sp-traffic, sp-conversion, budget-usage, ads-campaign-management-*
  idempotency_id    STRING    NOT NULL,   -- Amazon's per-record id; de-dup key
  advertiser_id     STRING,
  marketplace_id    STRING,
  time_window_start TIMESTAMP,            -- hourly datasets; NULL on entity events
  received_at       TIMESTAMP NOT NULL,
  campaign_id       STRING,
  ad_group_id       STRING,
  ad_id             STRING,
  target_id         STRING,               -- keyword_id or target_id
  placement         STRING,
  payload           JSON      NOT NULL    -- the record exactly as delivered
)
PARTITION BY DATE(received_at)
CLUSTER BY dataset_id, time_window_start
OPTIONS (description = 'Amazon Marketing Stream records, one row per delivered record. Deltas: sum after de-dup.');

-- Hourly Sponsored Products traffic at ad x target x placement grain.
CREATE OR REPLACE VIEW `punlabs.AMZSales.v_sp_traffic_hourly` AS
WITH dedup AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY idempotency_id ORDER BY received_at) AS rn
    FROM `punlabs.AMZSales.ads_stream_raw`
    WHERE dataset_id = 'sp-traffic'
  )
  WHERE rn = 1
)
SELECT
  time_window_start,
  DATE(time_window_start, 'America/Los_Angeles') AS day_pt,   -- Amazon reports NA in PT
  campaign_id, ad_group_id, ad_id, target_id, placement,
  SUM(INT64(payload.impressions)) AS impressions,
  SUM(INT64(payload.clicks))      AS clicks,
  SUM(FLOAT64(payload.cost))      AS cost
FROM dedup
GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- Hourly Sponsored Products conversions. time_window_start is the CLICK hour;
-- attributed conversions arrive later as further deltas, so this view's
-- recent hours keep changing for up to 30 days.
CREATE OR REPLACE VIEW `punlabs.AMZSales.v_sp_conversion_hourly` AS
WITH dedup AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY idempotency_id ORDER BY received_at) AS rn
    FROM `punlabs.AMZSales.ads_stream_raw`
    WHERE dataset_id = 'sp-conversion'
  )
  WHERE rn = 1
)
SELECT
  time_window_start,
  DATE(time_window_start, 'America/Los_Angeles') AS day_pt,
  campaign_id, ad_group_id, ad_id, target_id, placement,
  SUM(FLOAT64(payload.attributed_sales_1d))          AS sales1d,
  SUM(FLOAT64(payload.attributed_sales_7d))          AS sales7d,
  SUM(FLOAT64(payload.attributed_sales_14d))         AS sales14d,
  SUM(FLOAT64(payload.attributed_sales_30d))         AS sales30d,
  SUM(INT64(payload.attributed_conversions_1d))      AS purchases1d,
  SUM(INT64(payload.attributed_conversions_7d))      AS purchases7d,
  SUM(INT64(payload.attributed_conversions_14d))     AS purchases14d,
  SUM(INT64(payload.attributed_conversions_30d))     AS purchases30d,
  SUM(INT64(payload.attributed_units_ordered_14d))   AS units14d
FROM dedup
GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- Latest known ad -> SKU/ASIN mapping from the `ads-campaign-management-ads`
-- entity dataset, for joining stream rows to the SKU grain
-- sp_performance_master uses. The advertised product sits under
-- creative.product_creative.product_creative_settings.advertised_product with a
-- product_id + product_id_type (ASIN | SKU) pair, and a resolved_* pair for the
-- product Amazon actually matched it to.
CREATE OR REPLACE VIEW `punlabs.AMZSales.v_sp_ads_dim` AS
SELECT * EXCEPT (rn) FROM (
  SELECT
    ad_id, ad_group_id, campaign_id,
    IF(STRING(ap.product_id_type) = 'SKU',  STRING(ap.product_id),          NULL) AS sku,
    COALESCE(
      IF(STRING(ap.product_id_type) = 'ASIN',          STRING(ap.product_id),          NULL),
      IF(STRING(ap.resolved_product_id_type) = 'ASIN', STRING(ap.resolved_product_id), NULL)
    ) AS asin,
    STRING(payload.state) AS state,
    received_at           AS as_of,
    ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY received_at DESC) AS rn
  FROM `punlabs.AMZSales.ads_stream_raw`,
       UNNEST([payload.creative.product_creative.product_creative_settings.advertised_product]) AS ap
  WHERE dataset_id = 'ads-campaign-management-ads'
)
WHERE rn = 1;

-- Freshness: hourly datasets should never be more than a few hours behind.
-- Schedule hourly; alert on any row.
-- SELECT dataset_id, MAX(received_at) AS last_seen,
--        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(received_at), HOUR) AS hours_stale
-- FROM `punlabs.AMZSales.ads_stream_raw`
-- WHERE dataset_id IN ('sp-traffic', 'sp-conversion')
-- GROUP BY dataset_id
-- HAVING hours_stale > 3;
