-- Sponsored Products daily reports beyond the advertised-product one:
-- search terms (what shoppers typed) and placements (where the ad showed).
-- Both are re-pulled for a trailing window every day because attribution
-- restates for up to 14 days. Partitioned by date, clustered for the joins the
-- box-design analysis makes.

CREATE TABLE IF NOT EXISTS `punlabs.AMZSales.ads_sp_search_term_daily` (
  date          DATE    NOT NULL OPTIONS(description="Report day, advertiser time zone (Pacific)"),
  campaign_id   INT64   NOT NULL,
  campaign_name STRING,
  ad_group_id   INT64   NOT NULL,
  ad_group_name STRING,
  keyword_id    INT64   OPTIONS(description="Target (keyword or auto-targeting expression) that matched"),
  keyword       STRING  OPTIONS(description="Keyword text, or the auto expression name (close-match, substitutes, ...)"),
  match_type    STRING  OPTIONS(description="EXACT | PHRASE | BROAD | TARGETING_EXPRESSION | TARGETING_EXPRESSION_PREDEFINED"),
  search_term   STRING  NOT NULL OPTIONS(description="What the shopper actually typed (or an ASIN for product-page placements)"),
  impressions   INT64,
  clicks        INT64,
  cost          FLOAT64 OPTIONS(description="Spend, USD"),
  purchases_7d  INT64,
  sales_7d      FLOAT64,
  purchases_14d INT64,
  sales_14d     FLOAT64,
  loaded_at     TIMESTAMP NOT NULL
)
PARTITION BY date
CLUSTER BY campaign_id, search_term
OPTIONS (description = "Sponsored Products search-term report, daily: shopper query x matched target x ad group. Source: Ads API v3 spSearchTerm. Grain: (date, campaign_id, ad_group_id, keyword_id, search_term).");

CREATE TABLE IF NOT EXISTS `punlabs.AMZSales.ads_sp_placement_daily` (
  date          DATE    NOT NULL,
  campaign_id   INT64   NOT NULL,
  campaign_name STRING,
  placement     STRING  NOT NULL OPTIONS(description="Top of Search on-Amazon | Detail Page on-Amazon | Other on-Amazon | Off Amazon"),
  impressions   INT64,
  clicks        INT64,
  cost          FLOAT64 OPTIONS(description="Spend, USD"),
  purchases_7d  INT64,
  sales_7d      FLOAT64,
  purchases_14d INT64,
  sales_14d     FLOAT64,
  loaded_at     TIMESTAMP NOT NULL
)
PARTITION BY date
CLUSTER BY campaign_id, placement
OPTIONS (description = "Sponsored Products campaign x placement report, daily. Source: Ads API v3 spCampaigns grouped by campaign, campaignPlacement. Grain: (date, campaign_id, placement).");
