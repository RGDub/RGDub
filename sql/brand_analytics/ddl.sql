-- Brand Analytics weekly tables (SP-API Reports, reportPeriod=WEEK).
--
-- Weeks are Amazon's Sunday..Saturday. Both tables are partitioned by the
-- week's start date and clustered by asin. `payload` keeps the record exactly
-- as Amazon delivered it; the flat columns are for querying.

CREATE TABLE IF NOT EXISTS `punlabs.AMZSales.ba_search_catalog_perf` (
  week_start                DATE      NOT NULL OPTIONS(description="Sunday the Amazon week starts"),
  week_end                  DATE      NOT NULL OPTIONS(description="Saturday the Amazon week ends"),
  asin                      STRING    NOT NULL OPTIONS(description="Child ASIN"),
  impressions               INT64     OPTIONS(description="Search result impressions for the week"),
  impression_median_price   FLOAT64   OPTIONS(description="Median offer price shown at impression, USD"),
  same_day_impressions      INT64,
  one_day_impressions       INT64,
  two_day_impressions       INT64,
  clicks                    INT64     OPTIONS(description="Clicks from search results"),
  click_rate                FLOAT64   OPTIONS(description="clicks / impressions as Amazon reports it"),
  clicked_median_price      FLOAT64,
  same_day_clicks           INT64,
  one_day_clicks            INT64,
  two_day_clicks            INT64,
  cart_adds                 INT64     OPTIONS(description="Add-to-cart events from search traffic"),
  cart_added_median_price   FLOAT64,
  same_day_cart_adds        INT64,
  one_day_cart_adds         INT64,
  two_day_cart_adds         INT64,
  purchases                 INT64     OPTIONS(description="Purchases from search traffic"),
  search_traffic_sales      FLOAT64   OPTIONS(description="Sales from search traffic, USD"),
  conversion_rate           FLOAT64   OPTIONS(description="purchases / clicks as Amazon reports it"),
  purchase_median_price     FLOAT64,
  same_day_purchases        INT64,
  one_day_purchases         INT64,
  two_day_purchases         INT64,
  currency                  STRING,
  loaded_at                 TIMESTAMP NOT NULL,
  payload                   JSON      NOT NULL OPTIONS(description="The dataByAsin record as delivered")
)
PARTITION BY week_start
CLUSTER BY asin
OPTIONS (description = "Brand Analytics Search Catalog Performance: search funnel per ASIN per week (impressions > clicks > cart adds > purchases). Source: GET_BRAND_ANALYTICS_SEARCH_CATALOG_PERFORMANCE_REPORT, reportPeriod=WEEK.");

CREATE TABLE IF NOT EXISTS `punlabs.AMZSales.ba_search_query_perf` (
  week_start                DATE      NOT NULL OPTIONS(description="Sunday the Amazon week starts"),
  week_end                  DATE      NOT NULL OPTIONS(description="Saturday the Amazon week ends"),
  asin                      STRING    NOT NULL OPTIONS(description="Child ASIN the query metrics are for"),
  search_query              STRING    NOT NULL OPTIONS(description="Customer search query text"),
  search_query_score        INT64     OPTIONS(description="Amazon's rank of the query for this ASIN (1 = most impressions)"),
  search_query_volume       INT64     OPTIONS(description="Total searches for the query, all products"),
  impressions_total         INT64     OPTIONS(description="Impressions for the query across all ASINs"),
  impressions_asin          INT64     OPTIONS(description="Impressions for this ASIN on the query"),
  impressions_asin_share    FLOAT64   OPTIONS(description="Percent (0-100) of the query's impressions that were this ASIN"),
  clicks_total              INT64,
  clicks_asin               INT64,
  clicks_asin_share         FLOAT64   OPTIONS(description="Percent (0-100) of the query's clicks that went to this ASIN"),
  click_rate_total          FLOAT64   OPTIONS(description="Query-wide clicks / impressions, percent (0-100)"),
  click_median_price_total  FLOAT64,
  click_median_price_asin   FLOAT64,
  cart_adds_total           INT64,
  cart_adds_asin            INT64,
  cart_adds_asin_share      FLOAT64   OPTIONS(description="Percent (0-100)"),
  cart_add_rate_total       FLOAT64   OPTIONS(description="Query-wide cart adds / clicks, percent (0-100)"),
  purchases_total           INT64,
  purchases_asin            INT64,
  purchases_asin_share      FLOAT64   OPTIONS(description="Percent (0-100)"),
  purchase_rate_total       FLOAT64   OPTIONS(description="Query-wide purchases / clicks, percent (0-100)"),
  purchase_median_price_total FLOAT64,
  purchase_median_price_asin  FLOAT64,
  loaded_at                 TIMESTAMP NOT NULL,
  payload                   JSON      NOT NULL OPTIONS(description="The record as delivered")
)
PARTITION BY week_start
CLUSTER BY asin, search_query
OPTIONS (description = "Brand Analytics Search Query Performance: per ASIN per customer search query per week, this ASIN's share of impressions/clicks/cart adds/purchases vs the whole query. Source: GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT, reportPeriod=WEEK, asin option.");
