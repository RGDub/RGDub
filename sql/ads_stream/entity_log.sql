-- Campaign structure: every known state of every campaign / ad group / ad /
-- target, from two sources that share one shape.
--
--   source = 'snapshot'  rows written by pipelines/ads_entities.py, which pulls
--                        the full tree through the Ads API v1 query endpoints
--                        (POST /adsApi/v1/query/{campaigns,adGroups,ads,targets}).
--   source = 'stream'    rows written by pipelines/ads_stream/poller.py from the
--                        ads-campaign-management-* Marketing Stream datasets,
--                        one row per entity change event.
--
-- Both are stored with snake_case keys (the stream's native casing; the API's
-- camelCase is converted on the way in) so the same views read both. The log
-- is append-only; "current" is the latest row per entity, and "what changed"
-- is the diff between consecutive rows for an entity.

CREATE TABLE IF NOT EXISTS `punlabs.AMZSales.ads_entity_log` (
  entity_type   STRING    NOT NULL,   -- campaign | ad_group | ad | target
  entity_id     STRING    NOT NULL,
  campaign_id   STRING,
  ad_group_id   STRING,
  ad_product    STRING,               -- SPONSORED_PRODUCTS, ...
  state         STRING,               -- ENABLED | PAUSED | ARCHIVED | PROPOSED
  observed_at   TIMESTAMP NOT NULL,   -- when we saw this state (snapshot run or stream receipt)
  last_updated  TIMESTAMP,            -- Amazon's last_updated_date_time on the entity
  source        STRING    NOT NULL,   -- snapshot | stream
  payload       JSON      NOT NULL    -- the full entity, snake_case
)
PARTITION BY DATE(observed_at)
CLUSTER BY entity_type, entity_id
OPTIONS (description = 'Ads campaign-structure log: snapshot pulls + Marketing Stream change events. Append-only; latest row per entity is current.');

-- Latest known state of every entity.
CREATE OR REPLACE VIEW `punlabs.AMZSales.v_ads_entity_current` AS
SELECT * EXCEPT (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY entity_type, entity_id ORDER BY observed_at DESC) AS rn
  FROM `punlabs.AMZSales.ads_entity_log`
)
WHERE rn = 1;

-- Campaigns, flattened: budget, bid strategy, placement adjustments.
CREATE OR REPLACE VIEW `punlabs.AMZSales.v_ads_campaigns_current` AS
SELECT
  entity_id AS campaign_id,
  JSON_VALUE(payload, '$.name') AS name,
  state,
  JSON_VALUE(payload, '$.portfolio_id') AS portfolio_id,
  SAFE_CAST(JSON_VALUE(payload, '$.budgets[0].budget_value.monetary_budget_value.monetary_budget.value') AS FLOAT64) AS daily_budget,
  STRING(payload.budgets[0].budget_type) AS budget_type,
  STRING(payload.budgets[0].recurrence_time_period) AS budget_period,
  JSON_VALUE(payload, '$.optimizations.bid_settings.bid_strategy') AS bid_strategy,
  (SELECT MAX(SAFE_CAST(JSON_VALUE(a, '$.percentage') AS FLOAT64)) FROM UNNEST(JSON_QUERY_ARRAY(payload.optimizations.bid_settings.bid_adjustments.placement_bid_adjustments)) a
     WHERE JSON_VALUE(a, '$.placement') = 'TOP_OF_SEARCH')  AS top_of_search_adj_pct,
  (SELECT MAX(SAFE_CAST(JSON_VALUE(a, '$.percentage') AS FLOAT64)) FROM UNNEST(JSON_QUERY_ARRAY(payload.optimizations.bid_settings.bid_adjustments.placement_bid_adjustments)) a
     WHERE JSON_VALUE(a, '$.placement') = 'REST_OF_SEARCH') AS rest_of_search_adj_pct,
  (SELECT MAX(SAFE_CAST(JSON_VALUE(a, '$.percentage') AS FLOAT64)) FROM UNNEST(JSON_QUERY_ARRAY(payload.optimizations.bid_settings.bid_adjustments.placement_bid_adjustments)) a
     WHERE JSON_VALUE(a, '$.placement') = 'PRODUCT_PAGE')   AS product_page_adj_pct,
  JSON_VALUE(payload, '$.start_date_time') AS start_date_time,
  JSON_VALUE(payload, '$.end_date_time')   AS end_date_time,
  JSON_VALUE(payload, '$.status.delivery_status') AS delivery_status,
  last_updated, observed_at, source
FROM `punlabs.AMZSales.v_ads_entity_current`
WHERE entity_type = 'campaign';

-- Targets, flattened: what is targeted and at what bid.
CREATE OR REPLACE VIEW `punlabs.AMZSales.v_ads_targets_current` AS
SELECT
  entity_id AS target_id,
  campaign_id, ad_group_id,
  state,
  SAFE_CAST(JSON_VALUE(payload, '$.negative') AS BOOL) AS negative,
  JSON_VALUE(payload, '$.target_level') AS target_level,
  JSON_VALUE(payload, '$.target_type')  AS target_type,
  JSON_VALUE(payload, '$.target_details.keyword_target.keyword')     AS keyword,
  JSON_VALUE(payload, '$.target_details.keyword_target.match_type')  AS keyword_match_type,
  JSON_VALUE(payload, '$.target_details.product_target.product.product_id') AS product_id,
  JSON_VALUE(payload, '$.target_details.product_target.product_id_type')    AS product_id_type,
  JSON_VALUE(payload, '$.target_details.product_target.match_type')         AS product_match_type,
  JSON_VALUE(payload, '$.target_details.theme_target.match_type')           AS theme_match_type,
  JSON_VALUE(payload, '$.target_details.product_category_target.product_category_refinement.product_category_refinement.product_category_id') AS product_category_id,
  SAFE_CAST(JSON_VALUE(payload, '$.bid.bid') AS FLOAT64) AS bid,
  JSON_VALUE(payload, '$.bid.currency_code') AS bid_currency,
  last_updated, observed_at, source
FROM `punlabs.AMZSales.v_ads_entity_current`
WHERE entity_type = 'target';

-- Ad groups, flattened.
CREATE OR REPLACE VIEW `punlabs.AMZSales.v_ads_adgroups_current` AS
SELECT
  entity_id AS ad_group_id, campaign_id,
  JSON_VALUE(payload, '$.name') AS name, state,
  SAFE_CAST(JSON_VALUE(payload, '$.bid.default_bid') AS FLOAT64) AS default_bid,
  last_updated, observed_at, source
FROM `punlabs.AMZSales.v_ads_entity_current`
WHERE entity_type = 'ad_group';

-- Ads, flattened to the advertised product.
CREATE OR REPLACE VIEW `punlabs.AMZSales.v_ads_ads_current` AS
SELECT
  entity_id AS ad_id, ad_group_id, campaign_id, state,
  JSON_VALUE(payload, '$.creative.product_creative.product_creative_settings.advertised_product.product_id')      AS product_id,
  JSON_VALUE(payload, '$.creative.product_creative.product_creative_settings.advertised_product.product_id_type') AS product_id_type,
  JSON_VALUE(payload, '$.creative.product_creative.product_creative_settings.advertised_product.resolved_product_id') AS resolved_product_id,
  last_updated, observed_at, source
FROM `punlabs.AMZSales.v_ads_entity_current`
WHERE entity_type = 'ad';

-- Change log: each row is one observed transition for one entity, with the
-- handful of fields people actually change pulled out side by side. Anything
-- else can be compared through prev_payload / payload.
CREATE OR REPLACE VIEW `punlabs.AMZSales.v_ads_entity_changes` AS
WITH ordered AS (
  SELECT
    entity_type, entity_id, campaign_id, ad_group_id, observed_at, source, last_updated, state, payload,
    LAG(payload) OVER w AS prev_payload,
    LAG(state)   OVER w AS prev_state,
    LAG(observed_at) OVER w AS prev_observed_at
  FROM `punlabs.AMZSales.ads_entity_log`
  WINDOW w AS (PARTITION BY entity_type, entity_id ORDER BY observed_at)
)
SELECT
  COALESCE(last_updated, observed_at) AS changed_at,   -- Amazon's edit time; falls back to receipt time
  observed_at, entity_type, entity_id, campaign_id, ad_group_id, source, last_updated,
  prev_state, state,
  SAFE_CAST(JSON_VALUE(prev_payload, '$.bid.bid') AS FLOAT64) AS prev_bid, SAFE_CAST(JSON_VALUE(payload, '$.bid.bid') AS FLOAT64) AS bid,
  SAFE_CAST(JSON_VALUE(prev_payload, '$.budgets[0].budget_value.monetary_budget_value.monetary_budget.value') AS FLOAT64) AS prev_daily_budget,
  SAFE_CAST(JSON_VALUE(payload, '$.budgets[0].budget_value.monetary_budget_value.monetary_budget.value') AS FLOAT64)      AS daily_budget,
  JSON_VALUE(prev_payload, '$.name') AS prev_name, JSON_VALUE(payload, '$.name') AS name,
  JSON_VALUE(payload, '$.target_details.keyword_target.keyword') AS keyword,
  prev_payload, payload,
  prev_observed_at
FROM ordered
WHERE prev_payload IS NULL                                     -- first sighting (new entity, or the snapshot)
   OR TO_JSON_STRING(prev_payload) != TO_JSON_STRING(payload); -- something changed
