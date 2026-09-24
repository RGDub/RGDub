-- Etsy marketplace: landing tables and read views in punlabs.EtsySales.
--
-- Raw tables are append-only and keep every version Etsy returned. "Current"
-- is the newest version per id. Money in receipt payloads is {amount, divisor,
-- currency_code}; the views divide. Ledger amounts are already in cents in the
-- API and stored as dollars. Timestamps are UTC; dates below are America/New_York,
-- which is what the legacy CSV exports used.
-- The legacy CSV table PL-EtsySales-Transactions (2018-07..2025-12) stays as
-- it is; v_etsy_sold_items_legacy reproduces its columns from the API data.

CREATE TABLE IF NOT EXISTS `punlabs.EtsySales.etsy_receipts_raw` (
  receipt_id    INT64     NOT NULL OPTIONS(description="Etsy receipt (order) id"),
  status        STRING    OPTIONS(description="paid, completed, open, payment processing, canceled, fully refunded, partially refunded"),
  is_paid       BOOL,
  is_shipped    BOOL,
  created_at    TIMESTAMP OPTIONS(description="When the buyer placed the order (UTC)"),
  updated_at    TIMESTAMP OPTIONS(description="Etsy's last-updated time for this version (UTC)"),
  buyer_user_id INT64,
  pulled_at     TIMESTAMP NOT NULL,
  run_id        STRING,
  payload       JSON      NOT NULL OPTIONS(description="The receipt exactly as the Open API v3 returned it: transactions (line items), shipments, refunds, totals, address")
)
PARTITION BY DATE(created_at)
CLUSTER BY receipt_id
OPTIONS (description = "Etsy receipts, one row per version returned by the API. Append-only; use v_etsy_receipts for the current state.");

CREATE TABLE IF NOT EXISTS `punlabs.EtsySales.etsy_ledger_raw` (
  entry_id       INT64     NOT NULL,
  ledger_type    STRING    OPTIONS(description="e.g. sale, refund, fee, deposit (payout), tax, shipping label"),
  reference_type STRING    OPTIONS(description="What the entry refers to, e.g. receipt, payment, deposit"),
  reference_id   STRING,
  created_at     TIMESTAMP,
  amount         FLOAT64   OPTIONS(description="Signed dollars; negative for fees and deposits"),
  balance        FLOAT64   OPTIONS(description="Payment account balance after this entry, dollars"),
  currency       STRING,
  description    STRING,
  pulled_at      TIMESTAMP NOT NULL,
  run_id         STRING,
  payload        JSON      NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY ledger_type
OPTIONS (description = "Etsy payment-account ledger. The trailing 45 days are replaced on every run.");

CREATE TABLE IF NOT EXISTS `punlabs.EtsySales.etsy_listings_raw` (
  listing_id INT64     NOT NULL,
  state      STRING    OPTIONS(description="active, inactive, sold_out, draft, expired"),
  title      STRING,
  updated_at TIMESTAMP,
  pulled_at  TIMESTAMP NOT NULL,
  run_id     STRING,
  payload    JSON      NOT NULL OPTIONS(description="The listing with inventory.products[] (sku, offerings with price and quantity)")
)
PARTITION BY DATE(pulled_at)
CLUSTER BY listing_id
OPTIONS (description = "Etsy listing snapshot, one row per listing per daily pull. Use v_etsy_listings for the latest.");

-- Current state of every receipt, totals flattened to dollars.
CREATE OR REPLACE VIEW `punlabs.EtsySales.v_etsy_receipts`
OPTIONS (description = "One row per Etsy receipt (latest version), money in dollars, dates in America/New_York.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY receipt_id ORDER BY updated_at DESC, pulled_at DESC) AS rn
    FROM `punlabs.EtsySales.etsy_receipts_raw`)
  WHERE rn = 1
)
SELECT
  receipt_id, status, is_paid, is_shipped,
  DATE(created_at, 'America/New_York') AS order_date,
  created_at, updated_at, buyer_user_id,
  JSON_VALUE(payload, '$.name')          AS buyer_name,
  JSON_VALUE(payload, '$.buyer_email')   AS buyer_email,
  JSON_VALUE(payload, '$.city')          AS ship_city,
  JSON_VALUE(payload, '$.state')         AS ship_state,
  JSON_VALUE(payload, '$.zip')           AS ship_zip,
  JSON_VALUE(payload, '$.country_iso')   AS ship_country,
  JSON_VALUE(payload, '$.payment_method') AS payment_method,
  SAFE_CAST(JSON_VALUE(payload, '$.is_gift') AS BOOL) AS is_gift,
  SAFE_CAST(JSON_VALUE(payload, '$.grandtotal.amount') AS FLOAT64) / SAFE_CAST(JSON_VALUE(payload, '$.grandtotal.divisor') AS FLOAT64)                 AS grand_total,
  SAFE_CAST(JSON_VALUE(payload, '$.subtotal.amount') AS FLOAT64) / SAFE_CAST(JSON_VALUE(payload, '$.subtotal.divisor') AS FLOAT64)                     AS subtotal,
  SAFE_CAST(JSON_VALUE(payload, '$.total_price.amount') AS FLOAT64) / SAFE_CAST(JSON_VALUE(payload, '$.total_price.divisor') AS FLOAT64)               AS total_price,
  SAFE_CAST(JSON_VALUE(payload, '$.total_shipping_cost.amount') AS FLOAT64) / SAFE_CAST(JSON_VALUE(payload, '$.total_shipping_cost.divisor') AS FLOAT64) AS shipping,
  SAFE_CAST(JSON_VALUE(payload, '$.total_tax_cost.amount') AS FLOAT64) / SAFE_CAST(JSON_VALUE(payload, '$.total_tax_cost.divisor') AS FLOAT64)         AS sales_tax,
  SAFE_CAST(JSON_VALUE(payload, '$.discount_amt.amount') AS FLOAT64) / SAFE_CAST(JSON_VALUE(payload, '$.discount_amt.divisor') AS FLOAT64)             AS discount,
  JSON_VALUE(payload, '$.grandtotal.currency_code') AS currency,
  ARRAY_LENGTH(JSON_QUERY_ARRAY(payload, '$.transactions')) AS line_items,
  (SELECT SUM(SAFE_CAST(JSON_VALUE(t, '$.quantity') AS INT64)) FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.transactions')) t) AS units,
  ARRAY_LENGTH(JSON_QUERY_ARRAY(payload, '$.refunds')) > 0 AS has_refund,
  (SELECT MAX(JSON_VALUE(s, '$.tracking_code')) FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.shipments')) s) AS tracking_code,
  pulled_at
FROM latest;

-- One row per line item (Etsy "transaction"), joined to its receipt.
CREATE OR REPLACE VIEW `punlabs.EtsySales.v_etsy_transactions`
OPTIONS (description = "One row per Etsy line item with SKU, quantity, unit price, and the receipt's status and buyer. Money in dollars.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY receipt_id ORDER BY updated_at DESC, pulled_at DESC) AS rn
    FROM `punlabs.EtsySales.etsy_receipts_raw`)
  WHERE rn = 1
)
SELECT
  SAFE_CAST(JSON_VALUE(t, '$.transaction_id') AS INT64) AS transaction_id,
  r.receipt_id, r.status AS receipt_status,
  DATE(r.created_at, 'America/New_York') AS order_date,
  r.created_at,
  SAFE_CAST(JSON_VALUE(t, '$.listing_id') AS INT64) AS listing_id,
  SAFE_CAST(JSON_VALUE(t, '$.product_id') AS INT64) AS product_id,
  JSON_VALUE(t, '$.sku')   AS sku,
  JSON_VALUE(t, '$.title') AS title,
  SAFE_CAST(JSON_VALUE(t, '$.quantity') AS INT64) AS quantity,
  SAFE_CAST(JSON_VALUE(t, '$.price.amount') AS FLOAT64) / SAFE_CAST(JSON_VALUE(t, '$.price.divisor') AS FLOAT64) AS unit_price,
  SAFE_CAST(JSON_VALUE(t, '$.quantity') AS INT64)
    * SAFE_CAST(JSON_VALUE(t, '$.price.amount') AS FLOAT64) / SAFE_CAST(JSON_VALUE(t, '$.price.divisor') AS FLOAT64) AS item_total,
  SAFE_CAST(JSON_VALUE(t, '$.shipping_cost.amount') AS FLOAT64) / SAFE_CAST(JSON_VALUE(t, '$.shipping_cost.divisor') AS FLOAT64) AS shipping_cost,
  SAFE_CAST(JSON_VALUE(t, '$.buyer_coupon') AS FLOAT64) AS buyer_coupon,
  SAFE_CAST(JSON_VALUE(t, '$.shop_coupon') AS FLOAT64)  AS shop_coupon,
  TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(t, '$.paid_timestamp') AS INT64))    AS paid_at,
  TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(t, '$.shipped_timestamp') AS INT64)) AS shipped_at,
  JSON_VALUE(t, '$.transaction_type') AS transaction_type,
  SAFE_CAST(JSON_VALUE(t, '$.is_digital') AS BOOL) AS is_digital,
  ARRAY_TO_STRING(ARRAY(SELECT CONCAT(JSON_VALUE(v, '$.formatted_name'), ': ', JSON_VALUE(v, '$.formatted_value'))
                        FROM UNNEST(JSON_QUERY_ARRAY(t, '$.variations')) v), ', ') AS variations,
  JSON_VALUE(r.payload, '$.name') AS buyer_name,
  JSON_VALUE(r.payload, '$.grandtotal.currency_code') AS currency
FROM latest r, UNNEST(JSON_QUERY_ARRAY(r.payload, '$.transactions')) t;

-- Ledger, as stored (already flat).
CREATE OR REPLACE VIEW `punlabs.EtsySales.v_etsy_ledger`
OPTIONS (description = "Etsy payment-account ledger entries: sales credits, fees, refunds, deposits (payouts). amount is signed dollars.") AS
SELECT entry_id, DATE(created_at, 'America/New_York') AS entry_date, created_at, ledger_type, reference_type, reference_id,
       amount, balance, currency, description
FROM `punlabs.EtsySales.etsy_ledger_raw`;

-- Latest snapshot of every listing, one row per SKU (inventory product).
CREATE OR REPLACE VIEW `punlabs.EtsySales.v_etsy_listings`
OPTIONS (description = "Latest Etsy listing snapshot, one row per inventory product (SKU) with its offering price and quantity.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY pulled_at DESC) AS rn
    FROM `punlabs.EtsySales.etsy_listings_raw`)
  WHERE rn = 1
)
SELECT
  l.listing_id, l.state, l.title,
  JSON_VALUE(l.payload, '$.url') AS url,
  SAFE_CAST(JSON_VALUE(l.payload, '$.quantity') AS INT64) AS listing_quantity,
  SAFE_CAST(JSON_VALUE(l.payload, '$.price.amount') AS FLOAT64) / SAFE_CAST(JSON_VALUE(l.payload, '$.price.divisor') AS FLOAT64) AS listing_price,
  SAFE_CAST(JSON_VALUE(p, '$.product_id') AS INT64) AS product_id,
  JSON_VALUE(p, '$.sku') AS sku,
  (SELECT SAFE_CAST(JSON_VALUE(o, '$.price.amount') AS FLOAT64) / SAFE_CAST(JSON_VALUE(o, '$.price.divisor') AS FLOAT64)
     FROM UNNEST(JSON_QUERY_ARRAY(p, '$.offerings')) o LIMIT 1) AS sku_price,
  (SELECT SUM(SAFE_CAST(JSON_VALUE(o, '$.quantity') AS INT64)) FROM UNNEST(JSON_QUERY_ARRAY(p, '$.offerings')) o) AS sku_quantity,
  l.updated_at, l.pulled_at AS as_of
FROM latest l
LEFT JOIN UNNEST(IFNULL(JSON_QUERY_ARRAY(l.payload, '$.inventory.products'), [])) p;

-- The legacy CSV export's columns, from API data, so existing reports keep working.
-- Discount is the receipt-level discount allocated pro rata to line items; the
-- CSV did the same per item. Order Type is always "online" through the API.
CREATE OR REPLACE VIEW `punlabs.EtsySales.v_etsy_sold_items_legacy`
OPTIONS (description = "Etsy sold order items in the legacy CSV-export column layout (PL-EtsySales-Transactions), derived from the API. Use for continuity with older reports.") AS
WITH tx AS (
  SELECT t.*, r.discount, r.shipping AS order_shipping, r.sales_tax AS order_sales_tax, r.ship_city, r.ship_state, r.ship_zip,
         r.ship_country, r.payment_method,
         SUM(t.item_total) OVER (PARTITION BY t.receipt_id) AS receipt_items_total
  FROM `punlabs.EtsySales.v_etsy_transactions` t
  JOIN `punlabs.EtsySales.v_etsy_receipts` r USING (receipt_id)
)
SELECT
  order_date                                          AS `Sale Date`,
  title                                               AS `Item Name`,
  buyer_name                                          AS `Buyer`,
  quantity                                            AS `Quantity`,
  unit_price                                          AS `Price`,
  CAST(NULL AS STRING)                                AS `Coupon Code`,
  CAST(NULL AS STRING)                                AS `Coupon Details`,
  ROUND(IFNULL(discount, 0) * SAFE_DIVIDE(item_total, receipt_items_total), 2) AS `Discount Amount`,
  CAST(NULL AS FLOAT64)                               AS `Shipping Discount`,
  order_shipping                                      AS `Order Shipping`,
  order_sales_tax                                     AS `Order Sales Tax`,
  item_total                                          AS `Item Total`,
  currency                                            AS `Currency`,
  CAST(transaction_id AS STRING)                      AS `Transaction ID`,
  CAST(listing_id AS STRING)                          AS `Listing ID`,
  DATE(paid_at, 'America/New_York')                   AS `Date Paid`,
  DATE(shipped_at, 'America/New_York')                AS `Date Shipped`,
  buyer_name                                          AS `Ship Name`,
  ship_city                                           AS `Ship City`,
  ship_state                                          AS `Ship State`,
  ship_zip                                            AS `Ship Zipcode`,
  ship_country                                        AS `Ship Country`,
  CAST(receipt_id AS STRING)                          AS `Order ID`,
  variations                                          AS `Variations`,
  'online'                                            AS `Order Type`,
  IF(is_digital, 'digital', 'physical')               AS `Listings Type`,
  payment_method                                      AS `Payment Type`,
  sku                                                 AS `SKU`
FROM tx;
