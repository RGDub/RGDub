-- Daily snapshots of what Amazon shows for each ASIN: the catalog record
-- (title, main image, rank, category) and the offer/price picture. One row per
-- ASIN per day; a day is replaced if re-run. Raw API record kept in payload.

CREATE TABLE IF NOT EXISTS `punlabs.AMZSales.catalog_snapshot` (
  snapshot_date          DATE      NOT NULL OPTIONS(description="Day the snapshot was taken (UTC)"),
  asin                   STRING    NOT NULL,
  title                  STRING    OPTIONS(description="Listing title as shown on Amazon"),
  brand                  STRING,
  browse_classification  STRING    OPTIONS(description="Category shown on the listing, e.g. Colored Pencils"),
  classification_id      STRING,
  main_image_url         STRING    OPTIONS(description="URL of the MAIN image (the box shot in search results)"),
  main_image_width       INT64,
  main_image_height      INT64,
  image_count            INT64     OPTIONS(description="All image variants, all sizes"),
  sales_rank_category    STRING    OPTIONS(description="Most specific category Amazon ranks the ASIN in"),
  sales_rank             INT64     OPTIONS(description="Rank within sales_rank_category"),
  display_group          STRING    OPTIONS(description="Top-level store ranking group, e.g. Office Products"),
  display_group_rank     INT64,
  item_length_in         FLOAT64,
  item_width_in          FLOAT64,
  item_height_in         FLOAT64,
  loaded_at              TIMESTAMP NOT NULL,
  payload                JSON      NOT NULL OPTIONS(description="getCatalogItem response as delivered")
)
PARTITION BY snapshot_date
CLUSTER BY asin
OPTIONS (description = "Daily catalog snapshot per ASIN from SP-API Catalog Items 2022-04-01 (images, summaries, salesRanks, classifications, dimensions). Main-image URL changes here date a listing image change.");

CREATE TABLE IF NOT EXISTS `punlabs.AMZSales.pricing_daily` (
  snapshot_date               DATE      NOT NULL,
  asin                        STRING    NOT NULL,
  featured_price              FLOAT64   OPTIONS(description="Featured (Buy Box) offer listing price for the default segment, USD"),
  featured_shipping           FLOAT64,
  featured_seller_id          STRING,
  featured_is_ours            BOOL      OPTIONS(description="TRUE when Pun Labs holds the featured offer"),
  featured_fulfillment        STRING    OPTIONS(description="AFN (FBA) or MFN"),
  featured_glance_view_pct    FLOAT64   OPTIONS(description="Share of glance views the default segment represents, percent"),
  lowest_new_price            FLOAT64   OPTIONS(description="Lowest new-condition landed price (listing + shipping) across all sellers"),
  lowest_new_seller_id        STRING,
  our_lowest_price            FLOAT64   OPTIONS(description="Our own lowest new-condition landed price"),
  offer_count_new             INT64,
  was_price                   FLOAT64   OPTIONS(description="Amazon's WasPrice reference price, if any"),
  loaded_at                   TIMESTAMP NOT NULL,
  payload                     JSON      NOT NULL OPTIONS(description="competitiveSummary response body as delivered")
)
PARTITION BY snapshot_date
CLUSTER BY asin
OPTIONS (description = "Daily price and Featured Offer snapshot per ASIN from SP-API Product Pricing 2022-05-01 competitiveSummary (featuredBuyingOptions, lowestPricedOffers, referencePrices).");
