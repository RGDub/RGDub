-- Faire wholesale marketplace: landing tables and read views in punlabs.FaireSales.
--
-- Raw tables are append-only and keep every version Faire returned (an order
-- whose updated_at changes gets a new row). "Current" is the newest version
-- per id. Money in the payload is in minor units (cents); the views divide.
-- The two legacy CSV-export tables (PL-FaireSales-Transactions, -Payouts,
-- last written 2025-12) stay as they are for history.

CREATE TABLE IF NOT EXISTS `punlabs.FaireSales.faire_orders_raw` (
  order_id             STRING    NOT NULL OPTIONS(description="Faire order id, e.g. bo_bxdmjbwxid"),
  display_id           STRING    OPTIONS(description="Order number as shown in the Faire portal and on payouts, e.g. BXDMJBWXID"),
  state                STRING    OPTIONS(description="NEW, PROCESSING, PRE_TRANSIT, IN_TRANSIT, DELIVERED, CANCELED, BACKORDERED, PENDING_RETAILER_CONFIRMATION"),
  created_at           TIMESTAMP OPTIONS(description="When the retailer placed the order (UTC)"),
  updated_at           TIMESTAMP OPTIONS(description="Faire's last-updated time for this version (UTC)"),
  payment_initiated_at TIMESTAMP OPTIONS(description="When Faire paid the brand; NULL until paid"),
  retailer_id          STRING    OPTIONS(description="Faire retailer id, e.g. r_c9385ldj"),
  source               STRING    OPTIONS(description="MARKETPLACE, FAIRE_DIRECT, TRADESHOW, ..."),
  pulled_at            TIMESTAMP NOT NULL OPTIONS(description="When this row was fetched"),
  run_id               STRING    OPTIONS(description="Load run that wrote the row"),
  payload              JSON      NOT NULL OPTIONS(description="The order exactly as the Faire External API v2 returned it: items, shipments, address, payout_costs, discounts")
)
PARTITION BY DATE(created_at)
CLUSTER BY order_id
OPTIONS (description = "Faire orders, one row per version returned by the API. Append-only; use v_faire_orders for the current state.");

CREATE TABLE IF NOT EXISTS `punlabs.FaireSales.faire_products_raw` (
  product_id  STRING    NOT NULL OPTIONS(description="Faire product id, e.g. p_fccaefnahr"),
  name        STRING,
  state       STRING    OPTIONS(description="Sale state as returned by Faire"),
  updated_at  TIMESTAMP,
  pulled_at   TIMESTAMP NOT NULL,
  run_id      STRING,
  payload     JSON      NOT NULL OPTIONS(description="The product exactly as returned, including variants with sku and prices")
)
PARTITION BY DATE(pulled_at)
CLUSTER BY product_id
OPTIONS (description = "Faire catalog snapshot, one row per product per daily pull. Use v_faire_products for the latest.");

-- Current state of every order, with the retailer and the payout economics flattened.
CREATE OR REPLACE VIEW `punlabs.FaireSales.v_faire_orders`
OPTIONS (description = "One row per Faire order (latest version). Money in dollars. Payout fields may change until payment_initiated_at is set.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, pulled_at DESC) AS rn
    FROM `punlabs.FaireSales.faire_orders_raw`)
  WHERE rn = 1
)
SELECT
  order_id, display_id, state, source,
  DATE(created_at, 'America/New_York')            AS order_date,
  created_at, updated_at, payment_initiated_at,
  SAFE_CAST(JSON_VALUE(payload, '$.estimated_payout_at') AS TIMESTAMP) AS estimated_payout_at,
  SAFE_CAST(JSON_VALUE(payload, '$.ship_after') AS TIMESTAMP)          AS ship_after,
  retailer_id,
  COALESCE(JSON_VALUE(payload, '$.address.company_name'), JSON_VALUE(payload, '$.address.name')) AS retailer_name,
  JSON_VALUE(payload, '$.address.city')          AS city,
  JSON_VALUE(payload, '$.address.state_code')    AS state_code,
  JSON_VALUE(payload, '$.address.postal_code')   AS postal_code,
  JSON_VALUE(payload, '$.address.country_code')  AS country_code,
  JSON_VALUE(payload, '$.purchase_order_number') AS purchase_order_number,
  SAFE_CAST(JSON_VALUE(payload, '$.is_free_shipping') AS BOOL) AS is_free_shipping,
  JSON_VALUE(payload, '$.free_shipping_reason')  AS free_shipping_reason,
  JSON_VALUE(payload, '$.sales_rep_name')        AS sales_rep_name,
  ARRAY_LENGTH(JSON_QUERY_ARRAY(payload, '$.items')) AS item_count,
  (SELECT SUM(SAFE_CAST(JSON_VALUE(i, '$.quantity') AS INT64)) FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.items')) i) AS units,
  (SELECT SUM(SAFE_CAST(JSON_VALUE(i, '$.quantity') AS INT64) * SAFE_CAST(JSON_VALUE(i, '$.price.amount_minor') AS INT64)) / 100
     FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.items')) i) AS wholesale_subtotal,
  SAFE_CAST(JSON_VALUE(payload, '$.payout_costs.subtotal_after_brand_discounts.amount_minor') AS INT64) / 100 AS subtotal_after_discounts,
  SAFE_CAST(JSON_VALUE(payload, '$.payout_costs.commission_bps') AS INT64) / 100.0                             AS commission_pct,
  SAFE_CAST(JSON_VALUE(payload, '$.payout_costs.commission.amount_minor') AS INT64) / 100                       AS commission,
  SAFE_CAST(JSON_VALUE(payload, '$.payout_costs.commission_flat_fee.amount_minor') AS INT64) / 100              AS new_customer_fee,
  SAFE_CAST(JSON_VALUE(payload, '$.payout_costs.payout_fee.amount_minor') AS INT64) / 100                       AS payout_fee,
  SAFE_CAST(JSON_VALUE(payload, '$.payout_costs.net_tax.amount_minor') AS INT64) / 100                          AS net_tax,
  SAFE_CAST(JSON_VALUE(payload, '$.payout_costs.shipping_subsidy.amount_minor') AS INT64) / 100                 AS shipping_subsidy,
  SAFE_CAST(JSON_VALUE(payload, '$.payout_costs.payout_protection_fee.amount_minor') AS INT64) / 100            AS shipping_protection_fee,
  SAFE_CAST(JSON_VALUE(payload, '$.payout_costs.damaged_and_missing_items.amount_minor') AS INT64) / 100        AS damaged_or_missing,
  SAFE_CAST(JSON_VALUE(payload, '$.payout_costs.total_payout.amount_minor') AS INT64) / 100                     AS total_payout,
  JSON_VALUE(payload, '$.payout_costs.total_payout.currency') AS currency,
  pulled_at
FROM latest;

-- One row per order line: what was bought, at what wholesale price.
CREATE OR REPLACE VIEW `punlabs.FaireSales.v_faire_order_items`
OPTIONS (description = "Faire order lines from the latest version of each order. sku is the SKU at time of purchase. Prices in dollars.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, pulled_at DESC) AS rn
    FROM `punlabs.FaireSales.faire_orders_raw`)
  WHERE rn = 1
)
SELECT
  o.order_id, o.display_id, o.state AS order_state,
  DATE(o.created_at, 'America/New_York') AS order_date,
  o.retailer_id,
  JSON_VALUE(i, '$.id')           AS item_id,
  JSON_VALUE(i, '$.sku')          AS sku,
  JSON_VALUE(i, '$.product_id')   AS product_id,
  JSON_VALUE(i, '$.variant_id')   AS variant_id,
  JSON_VALUE(i, '$.product_name') AS product_name,
  JSON_VALUE(i, '$.variant_name') AS variant_name,
  JSON_VALUE(i, '$.state')        AS item_state,
  SAFE_CAST(JSON_VALUE(i, '$.quantity') AS INT64)                    AS quantity,
  SAFE_CAST(JSON_VALUE(i, '$.price.amount_minor') AS INT64) / 100    AS wholesale_price,
  SAFE_CAST(JSON_VALUE(i, '$.quantity') AS INT64) * SAFE_CAST(JSON_VALUE(i, '$.price.amount_minor') AS INT64) / 100 AS line_total,
  SAFE_CAST(JSON_VALUE(i, '$.includes_tester') AS BOOL)              AS includes_tester,
  ARRAY_LENGTH(JSON_QUERY_ARRAY(i, '$.discounts'))                   AS discount_count
FROM latest o, UNNEST(JSON_QUERY_ARRAY(o.payload, '$.items')) i;

-- Shipments per order (carrier, tracking, ship date).
CREATE OR REPLACE VIEW `punlabs.FaireSales.v_faire_shipments`
OPTIONS (description = "Faire shipments from the latest version of each order.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, pulled_at DESC) AS rn
    FROM `punlabs.FaireSales.faire_orders_raw`)
  WHERE rn = 1
)
SELECT
  o.order_id, o.display_id,
  JSON_VALUE(s, '$.id')             AS shipment_id,
  JSON_VALUE(s, '$.carrier')        AS carrier,
  JSON_VALUE(s, '$.tracking_code')  AS tracking_code,
  JSON_VALUE(s, '$.shipping_type')  AS shipping_type,
  SAFE_CAST(JSON_VALUE(s, '$.created_at') AS TIMESTAMP) AS shipped_at,
  SAFE_CAST(JSON_VALUE(s, '$.maker_cost.amount_minor') AS INT64) / 100 AS brand_shipping_cost
FROM latest o, UNNEST(JSON_QUERY_ARRAY(o.payload, '$.shipments')) s;

-- Latest catalog: one row per variant with its SKU and prices.
CREATE OR REPLACE VIEW `punlabs.FaireSales.v_faire_products`
OPTIONS (description = "Faire catalog from the most recent pull, one row per product variant. Prices in dollars.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY pulled_at DESC) AS rn
    FROM `punlabs.FaireSales.faire_products_raw`)
  WHERE rn = 1
)
SELECT
  p.product_id, p.name AS product_name, p.state AS product_state,
  JSON_VALUE(v, '$.id')   AS variant_id,
  JSON_VALUE(v, '$.sku')  AS sku,
  JSON_VALUE(v, '$.name') AS variant_name,
  JSON_VALUE(v, '$.sale_state') AS variant_state,
  SAFE_CAST(JSON_VALUE(v, '$.prices[0].wholesale_price.amount_minor') AS INT64) / 100 AS wholesale_price,
  SAFE_CAST(JSON_VALUE(v, '$.prices[0].retail_price.amount_minor') AS INT64) / 100    AS retail_price,
  SAFE_CAST(JSON_VALUE(v, '$.available_quantity') AS INT64) AS available_quantity,
  p.pulled_at
FROM latest p, UNNEST(JSON_QUERY_ARRAY(p.payload, '$.variants')) v;
