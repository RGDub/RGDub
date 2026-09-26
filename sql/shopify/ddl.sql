-- Shopify (popcolors.myshopify.com): landing tables and read views in punlabs.ShopifySales.
--
-- Raw tables are append-only and keep every version the Admin API returned
-- (an order whose updatedAt changes gets a new row). "Current" is the newest
-- version per id. Money in the payload is decimal strings in the shop
-- currency (shopMoney); the views cast. Dates in the views are the shop's
-- local day (America/New_York). The legacy table PL-ShopifySales-SalesbyDay
-- (a manual export, last written 2025-12) stays as it is for history;
-- v_shopify_sales_by_day carries the same figures forward from the raw orders.
-- "Test orders" = Shopify's test flag OR a discount code starting with "test" (the $0 test100/TEST10001
-- orders of 2025 were placed on the live checkout, so Shopify never flagged them).

CREATE TABLE IF NOT EXISTS `punlabs.ShopifySales.shopify_orders_raw` (
  order_id           STRING    NOT NULL OPTIONS(description="Shopify order GID, e.g. gid://shopify/Order/1234567890"),
  order_number       STRING    OPTIONS(description="Order name as shown in the admin, e.g. #1042"),
  created_at         TIMESTAMP OPTIONS(description="When the order was created (UTC)"),
  updated_at         TIMESTAMP OPTIONS(description="Shopify's last-updated time for this version (UTC)"),
  processed_at       TIMESTAMP OPTIONS(description="When the order was processed; Shopify reports attribute sales to this date"),
  financial_status   STRING    OPTIONS(description="PENDING, AUTHORIZED, PAID, PARTIALLY_PAID, PARTIALLY_REFUNDED, REFUNDED, VOIDED, EXPIRED"),
  fulfillment_status STRING    OPTIONS(description="UNFULFILLED, PARTIALLY_FULFILLED, FULFILLED, RESTOCKED, ..."),
  test               BOOL      OPTIONS(description="TRUE for test orders; excluded from the sales views"),
  pulled_at          TIMESTAMP NOT NULL OPTIONS(description="When this row was fetched"),
  run_id             STRING    OPTIONS(description="Load run that wrote the row"),
  payload            JSON      NOT NULL OPTIONS(description="The order as the Admin GraphQL API returned it: lineItems, discountApplications, shippingLines, refunds (with refundLineItems, refundShippingLines, transactions), transactions (with fees), fulfillments, shippingAddress (city/province/country/zip only)")
)
PARTITION BY DATE(created_at)
CLUSTER BY order_id
OPTIONS (description = "Shopify orders, one row per version returned by the API. Append-only; use v_shopify_orders for the current state.");

CREATE TABLE IF NOT EXISTS `punlabs.ShopifySales.shopify_products_raw` (
  product_id  STRING    NOT NULL OPTIONS(description="Shopify product GID"),
  title       STRING,
  status      STRING    OPTIONS(description="ACTIVE, ARCHIVED, DRAFT"),
  updated_at  TIMESTAMP,
  pulled_at   TIMESTAMP NOT NULL,
  run_id      STRING,
  payload     JSON      NOT NULL OPTIONS(description="The product with every variant (sku, barcode, price, compareAtPrice, inventoryQuantity, selectedOptions) and each variant's inventoryItem (tracked, unitCost, inventoryLevels by location with available/on_hand/committed/incoming/reserved)")
)
PARTITION BY DATE(pulled_at)
CLUSTER BY product_id
OPTIONS (description = "Shopify catalog snapshot, one row per product per daily pull. Use v_shopify_products / v_shopify_inventory for the latest.");

CREATE TABLE IF NOT EXISTS `punlabs.ShopifySales.shopify_payouts_raw` (
  payout_id        STRING    NOT NULL OPTIONS(description="Shopify Payments payout GID"),
  issued_at        TIMESTAMP OPTIONS(description="When the payout was (or is scheduled to be) issued"),
  status           STRING    OPTIONS(description="SCHEDULED, IN_TRANSIT, PAID, FAILED, CANCELED"),
  transaction_type STRING    OPTIONS(description="DEPOSIT or WITHDRAWAL"),
  net              FLOAT64   OPTIONS(description="Amount deposited to the bank account"),
  currency         STRING,
  pulled_at        TIMESTAMP NOT NULL,
  run_id           STRING,
  payload          JSON      NOT NULL OPTIONS(description="The payout with its summary (charges/refunds/adjustments/reserved funds gross and fee)")
)
PARTITION BY DATE(issued_at)
CLUSTER BY payout_id
OPTIONS (description = "Shopify Payments payouts, one row per payout per status seen. Use v_shopify_payouts for the latest.");

-- Current state of every order with the money fields flattened.
CREATE OR REPLACE VIEW `punlabs.ShopifySales.v_shopify_orders`
OPTIONS (description = "One row per Shopify order (latest version), test orders excluded. Money in shop currency. gross_sales = line items at original price; discounts = product discounts (positive); shipping = shipping charged after any free-shipping discount (shipping_discount). Matches Shopify's Sales report definitions.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, pulled_at DESC) AS rn
    FROM `punlabs.ShopifySales.shopify_orders_raw`)
  WHERE rn = 1
)
SELECT
  order_id, order_number, financial_status, fulfillment_status,
  DATE(processed_at, 'America/New_York')                                   AS order_date,
  created_at, updated_at, processed_at,
  SAFE_CAST(JSON_VALUE(payload, '$.cancelledAt') AS TIMESTAMP)             AS cancelled_at,
  SAFE_CAST(JSON_VALUE(payload, '$.closedAt') AS TIMESTAMP)                AS closed_at,
  JSON_VALUE(payload, '$.cancelReason')                                    AS cancel_reason,
  JSON_VALUE(payload, '$.sourceName')                                      AS source,
  JSON_VALUE(payload, '$.app.name')                                        AS app_name,
  JSON_VALUE(payload, '$.currencyCode')                                    AS currency,
  JSON_VALUE(payload, '$.shippingAddress.city')                            AS ship_city,
  JSON_VALUE(payload, '$.shippingAddress.provinceCode')                    AS ship_province,
  JSON_VALUE(payload, '$.shippingAddress.countryCodeV2')                   AS ship_country,
  JSON_VALUE(payload, '$.shippingAddress.zip')                             AS ship_zip,
  ARRAY_LENGTH(JSON_QUERY_ARRAY(payload, '$.lineItems'))                   AS line_count,
  (SELECT SUM(SAFE_CAST(JSON_VALUE(i, '$.quantity') AS INT64)) FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.lineItems')) i) AS units,
  (SELECT SUM(SAFE_CAST(JSON_VALUE(i, '$.originalTotalSet.shopMoney.amount') AS FLOAT64))
     FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.lineItems')) i)                             AS gross_sales,
  SAFE_CAST(JSON_VALUE(payload, '$.totalDiscountsSet.shopMoney.amount') AS FLOAT64)
    - IFNULL((SELECT SUM(SAFE_CAST(JSON_VALUE(l, '$.originalPriceSet.shopMoney.amount') AS FLOAT64)
                       - SAFE_CAST(JSON_VALUE(l, '$.discountedPriceSet.shopMoney.amount') AS FLOAT64))
              FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.shippingLines')) l), 0)            AS discounts,
  IFNULL((SELECT SUM(SAFE_CAST(JSON_VALUE(l, '$.originalPriceSet.shopMoney.amount') AS FLOAT64)
                   - SAFE_CAST(JSON_VALUE(l, '$.discountedPriceSet.shopMoney.amount') AS FLOAT64))
          FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.shippingLines')) l), 0)                AS shipping_discount,
  SAFE_CAST(JSON_VALUE(payload, '$.subtotalPriceSet.shopMoney.amount') AS FLOAT64)        AS subtotal,
  SAFE_CAST(JSON_VALUE(payload, '$.totalShippingPriceSet.shopMoney.amount') AS FLOAT64)
    - IFNULL((SELECT SUM(SAFE_CAST(JSON_VALUE(l, '$.originalPriceSet.shopMoney.amount') AS FLOAT64)
                       - SAFE_CAST(JSON_VALUE(l, '$.discountedPriceSet.shopMoney.amount') AS FLOAT64))
              FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.shippingLines')) l), 0)            AS shipping,
  SAFE_CAST(JSON_VALUE(payload, '$.totalTaxSet.shopMoney.amount') AS FLOAT64)             AS tax,
  SAFE_CAST(JSON_VALUE(payload, '$.currentTotalDutiesSet.shopMoney.amount') AS FLOAT64)   AS duties,
  SAFE_CAST(JSON_VALUE(payload, '$.currentTotalAdditionalFeesSet.shopMoney.amount') AS FLOAT64) AS additional_fees,
  SAFE_CAST(JSON_VALUE(payload, '$.totalTipReceivedSet.shopMoney.amount') AS FLOAT64)     AS tips,
  SAFE_CAST(JSON_VALUE(payload, '$.totalPriceSet.shopMoney.amount') AS FLOAT64)           AS total,
  SAFE_CAST(JSON_VALUE(payload, '$.totalRefundedSet.shopMoney.amount') AS FLOAT64)        AS refunded,
  SAFE_CAST(JSON_VALUE(payload, '$.totalRefundedShippingSet.shopMoney.amount') AS FLOAT64) AS refunded_shipping,
  SAFE_CAST(JSON_VALUE(payload, '$.currentTotalPriceSet.shopMoney.amount') AS FLOAT64)    AS current_total,
  SAFE_CAST(JSON_VALUE(payload, '$.netPaymentSet.shopMoney.amount') AS FLOAT64)           AS net_payment,
  (SELECT SUM(SAFE_CAST(JSON_VALUE(f, '$.amount.amount') AS FLOAT64))
     FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.transactions')) t, UNNEST(JSON_QUERY_ARRAY(t, '$.fees')) f
     WHERE JSON_VALUE(t, '$.status') = 'SUCCESS')                                         AS payment_fees,
  ARRAY_LENGTH(JSON_QUERY_ARRAY(payload, '$.refunds'))                                    AS refund_count,
  ARRAY_TO_STRING(JSON_VALUE_ARRAY(payload, '$.discountCodes'), ',')                      AS discount_codes,
  ARRAY_TO_STRING(JSON_VALUE_ARRAY(payload, '$.tags'), ',')                               AS tags,
  JSON_VALUE(payload, '$.fulfillments[0].trackingInfo[0].company')                        AS carrier,
  JSON_VALUE(payload, '$.fulfillments[0].trackingInfo[0].number')                         AS tracking_number,
  SAFE_CAST(JSON_VALUE(payload, '$.fulfillments[0].createdAt') AS TIMESTAMP)              AS fulfilled_at,
  pulled_at
FROM latest
WHERE NOT (IFNULL(test, FALSE) OR EXISTS (SELECT 1 FROM UNNEST(JSON_VALUE_ARRAY(payload, '$.discountCodes')) AS code WHERE LOWER(code) LIKE 'test%'));

-- One row per order line.
CREATE OR REPLACE VIEW `punlabs.ShopifySales.v_shopify_order_items`
OPTIONS (description = "Shopify order lines from the latest version of each order, test orders excluded. gross = original price x quantity; net = after line and order discounts, before tax.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, pulled_at DESC) AS rn
    FROM `punlabs.ShopifySales.shopify_orders_raw`)
  WHERE rn = 1 AND NOT (IFNULL(test, FALSE) OR EXISTS (SELECT 1 FROM UNNEST(JSON_VALUE_ARRAY(payload, '$.discountCodes')) AS code WHERE LOWER(code) LIKE 'test%'))
)
SELECT
  o.order_id, o.order_number, o.financial_status,
  DATE(o.processed_at, 'America/New_York') AS order_date,
  JSON_VALUE(i, '$.id')            AS line_item_id,
  JSON_VALUE(i, '$.sku')           AS sku,
  JSON_VALUE(i, '$.product.id')    AS product_id,
  JSON_VALUE(i, '$.variant.id')    AS variant_id,
  JSON_VALUE(i, '$.title')         AS product_title,
  JSON_VALUE(i, '$.variantTitle')  AS variant_title,
  JSON_VALUE(i, '$.vendor')        AS vendor,
  SAFE_CAST(JSON_VALUE(i, '$.quantity') AS INT64)           AS quantity,
  SAFE_CAST(JSON_VALUE(i, '$.currentQuantity') AS INT64)    AS current_quantity,
  SAFE_CAST(JSON_VALUE(i, '$.refundableQuantity') AS INT64) AS refundable_quantity,
  SAFE_CAST(JSON_VALUE(i, '$.originalUnitPriceSet.shopMoney.amount') AS FLOAT64)   AS unit_price,
  SAFE_CAST(JSON_VALUE(i, '$.discountedUnitPriceSet.shopMoney.amount') AS FLOAT64) AS discounted_unit_price,
  SAFE_CAST(JSON_VALUE(i, '$.originalTotalSet.shopMoney.amount') AS FLOAT64)       AS gross,
  SAFE_CAST(JSON_VALUE(i, '$.totalDiscountSet.shopMoney.amount') AS FLOAT64)       AS line_discount,
  (SELECT SUM(SAFE_CAST(JSON_VALUE(a, '$.allocatedAmountSet.shopMoney.amount') AS FLOAT64))
     FROM UNNEST(JSON_QUERY_ARRAY(i, '$.discountAllocations')) a)                    AS allocated_discount,
  SAFE_CAST(JSON_VALUE(i, '$.discountedTotalSet.shopMoney.amount') AS FLOAT64)     AS net,
  (SELECT SUM(SAFE_CAST(JSON_VALUE(t, '$.priceSet.shopMoney.amount') AS FLOAT64))
     FROM UNNEST(JSON_QUERY_ARRAY(i, '$.taxLines')) t)                               AS tax,
  SAFE_CAST(JSON_VALUE(i, '$.requiresShipping') AS BOOL) AS requires_shipping,
  SAFE_CAST(JSON_VALUE(i, '$.isGiftCard') AS BOOL)       AS is_gift_card
FROM latest o, UNNEST(JSON_QUERY_ARRAY(o.payload, '$.lineItems')) i;

-- One row per refund.
CREATE OR REPLACE VIEW `punlabs.ShopifySales.v_shopify_refunds`
OPTIONS (description = "Shopify refunds from the latest version of each order. items_refund = refund_total less shipping and tax (what Shopify's report books as Returns, also for refunds issued without line items); items_subtotal is the line-item figure when lines were recorded.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, pulled_at DESC) AS rn
    FROM `punlabs.ShopifySales.shopify_orders_raw`)
  WHERE rn = 1 AND NOT (IFNULL(test, FALSE) OR EXISTS (SELECT 1 FROM UNNEST(JSON_VALUE_ARRAY(payload, '$.discountCodes')) AS code WHERE LOWER(code) LIKE 'test%'))
)
SELECT
  o.order_id, o.order_number,
  JSON_VALUE(r, '$.id') AS refund_id,
  SAFE_CAST(JSON_VALUE(r, '$.createdAt') AS TIMESTAMP) AS refunded_at,
  DATE(SAFE_CAST(JSON_VALUE(r, '$.createdAt') AS TIMESTAMP), 'America/New_York') AS refund_date,
  JSON_VALUE(r, '$.note') AS note,
  SAFE_CAST(JSON_VALUE(r, '$.totalRefundedSet.shopMoney.amount') AS FLOAT64) AS refund_total,
  SAFE_CAST(JSON_VALUE(r, '$.totalRefundedSet.shopMoney.amount') AS FLOAT64)
    - IFNULL((SELECT SUM(SAFE_CAST(JSON_VALUE(s, '$.subtotalAmountSet.shopMoney.amount') AS FLOAT64)
                       + SAFE_CAST(JSON_VALUE(s, '$.taxAmountSet.shopMoney.amount') AS FLOAT64))
              FROM UNNEST(JSON_QUERY_ARRAY(r, '$.refundShippingLines')) s), 0)
    - IFNULL((SELECT SUM(SAFE_CAST(JSON_VALUE(li, '$.totalTaxSet.shopMoney.amount') AS FLOAT64))
              FROM UNNEST(JSON_QUERY_ARRAY(r, '$.refundLineItems')) li), 0) AS items_refund,
  (SELECT SUM(SAFE_CAST(JSON_VALUE(li, '$.subtotalSet.shopMoney.amount') AS FLOAT64))
     FROM UNNEST(JSON_QUERY_ARRAY(r, '$.refundLineItems')) li) AS items_subtotal,
  IFNULL((SELECT SUM(SAFE_CAST(JSON_VALUE(li, '$.totalTaxSet.shopMoney.amount') AS FLOAT64))
     FROM UNNEST(JSON_QUERY_ARRAY(r, '$.refundLineItems')) li), 0)
  + IFNULL((SELECT SUM(SAFE_CAST(JSON_VALUE(s, '$.taxAmountSet.shopMoney.amount') AS FLOAT64))
     FROM UNNEST(JSON_QUERY_ARRAY(r, '$.refundShippingLines')) s), 0) AS tax_refund,
  (SELECT SUM(SAFE_CAST(JSON_VALUE(s, '$.subtotalAmountSet.shopMoney.amount') AS FLOAT64))
     FROM UNNEST(JSON_QUERY_ARRAY(r, '$.refundShippingLines')) s) AS shipping_refund,
  (SELECT SUM(SAFE_CAST(JSON_VALUE(li, '$.quantity') AS INT64))
     FROM UNNEST(JSON_QUERY_ARRAY(r, '$.refundLineItems')) li) AS units_returned
FROM latest o, UNNEST(JSON_QUERY_ARRAY(o.payload, '$.refunds')) r;

-- One row per refunded line.
CREATE OR REPLACE VIEW `punlabs.ShopifySales.v_shopify_refund_items`
OPTIONS (description = "Shopify refunded lines (which SKU, how many, restocked or not) from the latest version of each order.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, pulled_at DESC) AS rn
    FROM `punlabs.ShopifySales.shopify_orders_raw`)
  WHERE rn = 1 AND NOT (IFNULL(test, FALSE) OR EXISTS (SELECT 1 FROM UNNEST(JSON_VALUE_ARRAY(payload, '$.discountCodes')) AS code WHERE LOWER(code) LIKE 'test%'))
)
SELECT
  o.order_id, o.order_number,
  JSON_VALUE(r, '$.id') AS refund_id,
  DATE(SAFE_CAST(JSON_VALUE(r, '$.createdAt') AS TIMESTAMP), 'America/New_York') AS refund_date,
  JSON_VALUE(li, '$.lineItem.id')  AS line_item_id,
  JSON_VALUE(li, '$.lineItem.sku') AS sku,
  SAFE_CAST(JSON_VALUE(li, '$.quantity') AS INT64) AS quantity,
  JSON_VALUE(li, '$.restockType') AS restock_type,
  SAFE_CAST(JSON_VALUE(li, '$.subtotalSet.shopMoney.amount') AS FLOAT64) AS subtotal,
  SAFE_CAST(JSON_VALUE(li, '$.totalTaxSet.shopMoney.amount') AS FLOAT64) AS tax
FROM latest o, UNNEST(JSON_QUERY_ARRAY(o.payload, '$.refunds')) r, UNNEST(JSON_QUERY_ARRAY(r, '$.refundLineItems')) li;

-- Payment transactions with processor fees.
CREATE OR REPLACE VIEW `punlabs.ShopifySales.v_shopify_transactions`
OPTIONS (description = "Shopify order transactions (SALE, CAPTURE, REFUND, ...) with Shopify Payments fees, from the latest version of each order.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, pulled_at DESC) AS rn
    FROM `punlabs.ShopifySales.shopify_orders_raw`)
  WHERE rn = 1 AND NOT (IFNULL(test, FALSE) OR EXISTS (SELECT 1 FROM UNNEST(JSON_VALUE_ARRAY(payload, '$.discountCodes')) AS code WHERE LOWER(code) LIKE 'test%'))
)
SELECT
  o.order_id, o.order_number,
  JSON_VALUE(t, '$.id')        AS transaction_id,
  JSON_VALUE(t, '$.kind')      AS kind,
  JSON_VALUE(t, '$.status')    AS status,
  JSON_VALUE(t, '$.gateway')   AS gateway,
  SAFE_CAST(JSON_VALUE(t, '$.processedAt') AS TIMESTAMP) AS processed_at,
  SAFE_CAST(JSON_VALUE(t, '$.amountSet.shopMoney.amount') AS FLOAT64) AS amount,
  (SELECT SUM(SAFE_CAST(JSON_VALUE(f, '$.amount.amount') AS FLOAT64)) FROM UNNEST(JSON_QUERY_ARRAY(t, '$.fees')) f) AS fees,
  (SELECT STRING_AGG(JSON_VALUE(f, '$.rateName')) FROM UNNEST(JSON_QUERY_ARRAY(t, '$.fees')) f) AS fee_rate_name
FROM latest o, UNNEST(JSON_QUERY_ARRAY(o.payload, '$.transactions')) t;

-- Latest catalog: one row per variant.
CREATE OR REPLACE VIEW `punlabs.ShopifySales.v_shopify_products`
OPTIONS (description = "Shopify catalog from the most recent pull, one row per product variant, with price, cost and total inventory.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY pulled_at DESC) AS rn
    FROM `punlabs.ShopifySales.shopify_products_raw`)
  WHERE rn = 1
)
SELECT
  p.product_id, p.title AS product_title, p.status AS product_status,
  JSON_VALUE(p.payload, '$.handle')      AS handle,
  JSON_VALUE(p.payload, '$.vendor')      AS vendor,
  JSON_VALUE(p.payload, '$.productType') AS product_type,
  ARRAY_TO_STRING(JSON_VALUE_ARRAY(p.payload, '$.tags'), ',') AS tags,
  JSON_VALUE(v, '$.id')      AS variant_id,
  JSON_VALUE(v, '$.title')   AS variant_title,
  JSON_VALUE(v, '$.sku')     AS sku,
  JSON_VALUE(v, '$.barcode') AS barcode,
  SAFE_CAST(JSON_VALUE(v, '$.position') AS INT64)          AS position,
  SAFE_CAST(JSON_VALUE(v, '$.price') AS FLOAT64)           AS price,
  SAFE_CAST(JSON_VALUE(v, '$.compareAtPrice') AS FLOAT64)  AS compare_at_price,
  SAFE_CAST(JSON_VALUE(v, '$.inventoryItem.unitCost.amount') AS FLOAT64) AS unit_cost,
  SAFE_CAST(JSON_VALUE(v, '$.inventoryItem.tracked') AS BOOL) AS inventory_tracked,
  SAFE_CAST(JSON_VALUE(v, '$.inventoryQuantity') AS INT64) AS inventory_quantity,
  JSON_VALUE(v, '$.inventoryItem.id') AS inventory_item_id,
  SAFE_CAST(JSON_VALUE(v, '$.updatedAt') AS TIMESTAMP) AS variant_updated_at,
  p.pulled_at
FROM latest p, UNNEST(JSON_QUERY_ARRAY(p.payload, '$.variants')) v;

-- Latest inventory by variant and location.
CREATE OR REPLACE VIEW `punlabs.ShopifySales.v_shopify_inventory`
OPTIONS (description = "Shopify inventory from the most recent pull, one row per variant per location.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY pulled_at DESC) AS rn
    FROM `punlabs.ShopifySales.shopify_products_raw`)
  WHERE rn = 1
),
q AS (
  SELECT
    p.product_id, p.title AS product_title, JSON_VALUE(v, '$.id') AS variant_id, JSON_VALUE(v, '$.sku') AS sku,
    JSON_VALUE(l, '$.location.id') AS location_id, JSON_VALUE(l, '$.location.name') AS location_name,
    JSON_VALUE(x, '$.name') AS name, SAFE_CAST(JSON_VALUE(x, '$.quantity') AS INT64) AS quantity, p.pulled_at
  FROM latest p,
       UNNEST(JSON_QUERY_ARRAY(p.payload, '$.variants')) v,
       UNNEST(JSON_QUERY_ARRAY(v, '$.inventoryItem.inventoryLevels')) l,
       UNNEST(JSON_QUERY_ARRAY(l, '$.quantities')) x
)
SELECT product_id, product_title, variant_id, sku, location_id, location_name,
  MAX(IF(name = 'available', quantity, NULL)) AS available,
  MAX(IF(name = 'on_hand',   quantity, NULL)) AS on_hand,
  MAX(IF(name = 'committed', quantity, NULL)) AS committed,
  MAX(IF(name = 'incoming',  quantity, NULL)) AS incoming,
  MAX(IF(name = 'reserved',  quantity, NULL)) AS reserved,
  pulled_at
FROM q
GROUP BY product_id, product_title, variant_id, sku, location_id, location_name, pulled_at;

-- Payouts, latest status per payout.
CREATE OR REPLACE VIEW `punlabs.ShopifySales.v_shopify_payouts`
OPTIONS (description = "Shopify Payments payouts (latest status), with the gross and fee breakdown behind the net deposit.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY payout_id ORDER BY pulled_at DESC) AS rn
    FROM `punlabs.ShopifySales.shopify_payouts_raw`)
  WHERE rn = 1
)
SELECT
  payout_id, DATE(issued_at, 'America/New_York') AS payout_date, issued_at, status, transaction_type, currency, net,
  SAFE_CAST(JSON_VALUE(payload, '$.summary.chargesGross.amount') AS FLOAT64)        AS charges_gross,
  SAFE_CAST(JSON_VALUE(payload, '$.summary.chargesFee.amount') AS FLOAT64)          AS charges_fee,
  SAFE_CAST(JSON_VALUE(payload, '$.summary.refundsFeeGross.amount') AS FLOAT64)     AS refunds_gross,
  SAFE_CAST(JSON_VALUE(payload, '$.summary.refundsFee.amount') AS FLOAT64)          AS refunds_fee,
  SAFE_CAST(JSON_VALUE(payload, '$.summary.adjustmentsGross.amount') AS FLOAT64)    AS adjustments_gross,
  SAFE_CAST(JSON_VALUE(payload, '$.summary.adjustmentsFee.amount') AS FLOAT64)      AS adjustments_fee,
  SAFE_CAST(JSON_VALUE(payload, '$.summary.reservedFundsGross.amount') AS FLOAT64)  AS reserved_funds_gross,
  SAFE_CAST(JSON_VALUE(payload, '$.summary.reservedFundsFee.amount') AS FLOAT64)    AS reserved_funds_fee,
  SAFE_CAST(JSON_VALUE(payload, '$.summary.retriedPayoutsGross.amount') AS FLOAT64) AS retried_payouts_gross,
  SAFE_CAST(JSON_VALUE(payload, '$.summary.retriedPayoutsFee.amount') AS FLOAT64)   AS retried_payouts_fee,
  pulled_at
FROM latest;

-- Shopify's "Sales by day" report, rebuilt from the raw orders.
CREATE OR REPLACE VIEW `punlabs.ShopifySales.v_shopify_sales_by_day`
OPTIONS (description = "Same columns as Shopify's Sales by day report and the legacy PL-ShopifySales-SalesbyDay table: gross_sales - discounts - returns = net_sales; net_sales + shipping_charges + duties + additional_fees + taxes = total_sales. Sales on the order's processed date, returns on the refund date, discounts and returns shown negative, test orders excluded.") AS
WITH latest AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, pulled_at DESC) AS rn
    FROM `punlabs.ShopifySales.shopify_orders_raw`)
  WHERE rn = 1 AND NOT (IFNULL(test, FALSE) OR EXISTS (SELECT 1 FROM UNNEST(JSON_VALUE_ARRAY(payload, '$.discountCodes')) AS code WHERE LOWER(code) LIKE 'test%'))
),
sales AS (
  SELECT
    DATE(processed_at, 'America/New_York') AS day,
    COUNT(*) AS orders,
    SUM(IFNULL((SELECT SUM(SAFE_CAST(JSON_VALUE(i, '$.originalTotalSet.shopMoney.amount') AS FLOAT64))
                FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.lineItems')) i), 0))                    AS gross_sales,
    -SUM(SAFE_CAST(JSON_VALUE(payload, '$.totalDiscountsSet.shopMoney.amount') AS FLOAT64)
         - IFNULL((SELECT SUM(SAFE_CAST(JSON_VALUE(l, '$.originalPriceSet.shopMoney.amount') AS FLOAT64)
                            - SAFE_CAST(JSON_VALUE(l, '$.discountedPriceSet.shopMoney.amount') AS FLOAT64))
                   FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.shippingLines')) l), 0))           AS discounts,
    SUM(SAFE_CAST(JSON_VALUE(payload, '$.totalShippingPriceSet.shopMoney.amount') AS FLOAT64)
        - IFNULL((SELECT SUM(SAFE_CAST(JSON_VALUE(l, '$.originalPriceSet.shopMoney.amount') AS FLOAT64)
                           - SAFE_CAST(JSON_VALUE(l, '$.discountedPriceSet.shopMoney.amount') AS FLOAT64))
                  FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.shippingLines')) l), 0))            AS shipping_charges,
    SUM(IFNULL(SAFE_CAST(JSON_VALUE(payload, '$.currentTotalDutiesSet.shopMoney.amount') AS FLOAT64), 0)) AS duties,
    SUM(IFNULL(SAFE_CAST(JSON_VALUE(payload, '$.currentTotalAdditionalFeesSet.shopMoney.amount') AS FLOAT64), 0)) AS additional_fees,
    SUM(SAFE_CAST(JSON_VALUE(payload, '$.totalTaxSet.shopMoney.amount') AS FLOAT64))          AS taxes
  FROM latest
  GROUP BY day
),
-- Returns = refund total less refunded shipping and tax, so refunds issued
-- without line items (a plain amount) still count, as they do in Shopify's report.
returns AS (
  SELECT
    DATE(SAFE_CAST(JSON_VALUE(r, '$.createdAt') AS TIMESTAMP), 'America/New_York') AS day,
    -SUM(SAFE_CAST(JSON_VALUE(r, '$.totalRefundedSet.shopMoney.amount') AS FLOAT64)
         - IFNULL((SELECT SUM(SAFE_CAST(JSON_VALUE(s, '$.subtotalAmountSet.shopMoney.amount') AS FLOAT64)
                            + SAFE_CAST(JSON_VALUE(s, '$.taxAmountSet.shopMoney.amount') AS FLOAT64))
                   FROM UNNEST(JSON_QUERY_ARRAY(r, '$.refundShippingLines')) s), 0)
         - IFNULL((SELECT SUM(SAFE_CAST(JSON_VALUE(li, '$.totalTaxSet.shopMoney.amount') AS FLOAT64))
                   FROM UNNEST(JSON_QUERY_ARRAY(r, '$.refundLineItems')) li), 0)) AS returns,
    -SUM(IFNULL((SELECT SUM(SAFE_CAST(JSON_VALUE(s, '$.subtotalAmountSet.shopMoney.amount') AS FLOAT64))
                 FROM UNNEST(JSON_QUERY_ARRAY(r, '$.refundShippingLines')) s), 0)) AS shipping_refunds,
    -SUM(IFNULL((SELECT SUM(SAFE_CAST(JSON_VALUE(li, '$.totalTaxSet.shopMoney.amount') AS FLOAT64))
                 FROM UNNEST(JSON_QUERY_ARRAY(r, '$.refundLineItems')) li), 0)
         + IFNULL((SELECT SUM(SAFE_CAST(JSON_VALUE(s, '$.taxAmountSet.shopMoney.amount') AS FLOAT64))
                 FROM UNNEST(JSON_QUERY_ARRAY(r, '$.refundShippingLines')) s), 0)) AS tax_refunds
  FROM latest o, UNNEST(JSON_QUERY_ARRAY(o.payload, '$.refunds')) r
  GROUP BY day
),
joined AS (
  SELECT
    COALESCE(s.day, r.day) AS day,
    IFNULL(s.orders, 0) AS orders,
    IFNULL(s.gross_sales, 0) AS gross_sales,
    IFNULL(s.discounts, 0) AS discounts,
    IFNULL(r.returns, 0) AS returns,
    IFNULL(s.shipping_charges, 0) + IFNULL(r.shipping_refunds, 0) AS shipping_charges,
    IFNULL(s.duties, 0) AS duties,
    IFNULL(s.additional_fees, 0) AS additional_fees,
    IFNULL(s.taxes, 0) + IFNULL(r.tax_refunds, 0) AS taxes
  FROM sales s FULL OUTER JOIN returns r USING (day)
)
SELECT
  day, orders,
  ROUND(gross_sales, 2) AS gross_sales,
  ROUND(discounts, 2) AS discounts,
  ROUND(returns, 2) AS returns,
  ROUND(gross_sales + discounts + returns, 2) AS net_sales,
  ROUND(shipping_charges, 2) AS shipping_charges,
  ROUND(duties, 2) AS duties,
  ROUND(additional_fees, 2) AS additional_fees,
  ROUND(taxes, 2) AS taxes,
  ROUND(gross_sales + discounts + returns + shipping_charges + duties + additional_fees + taxes, 2) AS total_sales
FROM joined;
