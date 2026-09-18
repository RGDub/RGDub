-- Post-load grain assertion for sp_performance_master.
--
-- Once adGroupId is populated, the full key must be unique. Any row returned
-- here is a real load defect. Schedule after the daily load; alert on rows.
-- Rows with NULL adGroupId predate the 2026-09-18 migration and are excluded.
SELECT date, campaignId, adGroupId, advertisedSku, COUNT(*) AS rows_in_group
FROM `punlabs.AMZSales.sp_performance_master`
WHERE adGroupId IS NOT NULL
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
ORDER BY date DESC;
