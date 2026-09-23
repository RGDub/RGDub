-- FBA stock by fulfillment center. Loaded by pipelines/spapi/fba_inventory_by_fc.py
-- (Daily Inventory pipeline, via the FBA ledger step). Idempotent: safe to re-run.
--
-- Source: SP-API GET_LEDGER_SUMMARY_VIEW_DATA with aggregateByLocation=FC, DAILY.
-- Amazon publishes FC-level days roughly 10 days late, so the newest date here
-- trails the country-level ledger (PL-AMZSales-INVLedger) by about that much.

CREATE TABLE IF NOT EXISTS `punlabs.AMZSales.fba_inventory_by_fc` (
  date                          DATE      NOT NULL OPTIONS(description = 'Ledger day (Amazon report Date).'),
  fc                            STRING    NOT NULL OPTIONS(description = 'Amazon fulfillment center code, e.g. DET3 (report column Location).'),
  msku                          STRING    OPTIONS(description = 'Merchant SKU.'),
  parent_sku                    STRING    OPTIONS(description = 'Parent SKU, same rule as PL-AMZSales-INVLedger (pipelines.lib.sku.parent_sku_legacy).'),
  fnsku                         STRING    OPTIONS(description = 'Amazon fulfillment network SKU.'),
  asin                          STRING,
  title                         STRING,
  disposition                   STRING    OPTIONS(description = 'SELLABLE, WAREHOUSE_DAMAGED, CUSTOMER_DAMAGED, ...'),
  starting_balance              INT64     OPTIONS(description = 'Units in this FC at the start of the day.'),
  in_transit_between_warehouses INT64     OPTIONS(description = 'Units moving between Amazon warehouses, attributed to this FC.'),
  receipts                      INT64     OPTIONS(description = 'Units received into this FC (inbound shipments).'),
  customer_shipments            INT64     OPTIONS(description = 'Units shipped to customers from this FC (negative).'),
  customer_returns              INT64,
  vendor_returns                INT64     OPTIONS(description = 'Removals/returns to seller (negative).'),
  warehouse_transfers           INT64     OPTIONS(description = 'Net units transferred in (+) or out (-) between FCs.'),
  found                         INT64,
  lost                          INT64,
  damaged                       INT64,
  disposed                      INT64,
  other_events                  INT64,
  ending_balance                INT64     OPTIONS(description = 'Units in this FC at the end of the day. Sum over fc = country-level Ending Warehouse Balance.'),
  unknown_events                INT64,
  store                         STRING,
  loaded_at                     TIMESTAMP OPTIONS(description = 'When the loader wrote this row.')
)
PARTITION BY date
CLUSTER BY fc, msku
OPTIONS(description = 'FBA inventory ledger by fulfillment center, one row per date x fnsku x msku x disposition x fc. Arrives ~10 days behind the country ledger.');

-- Where stock sits as of the newest published FC day.
CREATE OR REPLACE VIEW `punlabs.AMZSales.v_fba_stock_by_fc_latest`
OPTIONS(description = 'FBA units on hand by fulfillment center and SKU on the newest day Amazon has published at FC level (as_of_date), rows with stock only.') AS
WITH latest AS (SELECT MAX(date) AS as_of_date FROM `punlabs.AMZSales.fba_inventory_by_fc`)
SELECT
  l.as_of_date,
  DATE_DIFF(CURRENT_DATE('America/Los_Angeles'), l.as_of_date, DAY) AS days_behind,
  f.fc, f.parent_sku, f.msku, f.asin, f.title, f.disposition,
  f.ending_balance AS units,
  SAFE_DIVIDE(f.ending_balance, SUM(f.ending_balance) OVER (PARTITION BY f.msku, f.disposition)) AS share_of_sku
FROM `punlabs.AMZSales.fba_inventory_by_fc` f
JOIN latest l ON f.date = l.as_of_date
WHERE f.ending_balance <> 0;

-- One row per FC per day: how concentrated the network is and how it moves.
CREATE OR REPLACE VIEW `punlabs.AMZSales.v_fba_stock_by_fc_daily`
OPTIONS(description = 'Daily sellable units, SKU count and movements per fulfillment center.') AS
SELECT
  date, fc,
  SUM(IF(disposition = 'SELLABLE', ending_balance, 0))   AS sellable_units,
  SUM(IF(disposition <> 'SELLABLE', ending_balance, 0))  AS unsellable_units,
  COUNT(DISTINCT IF(ending_balance > 0, msku, NULL))     AS skus_in_stock,
  SUM(receipts)            AS receipts,
  SUM(customer_shipments)  AS customer_shipments,
  SUM(warehouse_transfers) AS warehouse_transfers
FROM `punlabs.AMZSales.fba_inventory_by_fc`
GROUP BY date, fc;
