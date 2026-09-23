-- FC-level ending balance summed over fulfillment centers must equal the
-- country-level ledger for the same day, SKU and disposition. Rows returned =
-- mismatches (expect none). Checked 2026-09-12 by hand: 0 of 62 SKU rows differed.
WITH fc AS (
  SELECT date, msku, disposition, SUM(ending_balance) AS fc_units
  FROM `punlabs.AMZSales.fba_inventory_by_fc` GROUP BY 1, 2, 3
),
country AS (
  SELECT Date AS date, MSKU AS msku, Disposition AS disposition, SUM(`Ending Warehouse Balance`) AS country_units
  FROM `punlabs.AMZSales.PL-AMZSales-INVLedger` GROUP BY 1, 2, 3
)
SELECT date, msku, disposition, country_units, fc_units, country_units - fc_units AS diff
FROM fc JOIN country USING (date, msku, disposition)
WHERE country_units <> fc_units
ORDER BY date DESC, ABS(country_units - fc_units) DESC;
