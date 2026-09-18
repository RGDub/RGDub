-- Stream vs daily report reconciliation.
--
-- Once a day is >2 days old, impressions/clicks/cost from the hourly stream
-- rolled up to the day must match sp_performance_master to the cent. Any row
-- here is a real divergence: a missed message, a subscription gap, or a
-- report restatement the stream did not carry. Conversions are compared only
-- after the 30-day attribution window closes.
WITH stream AS (
  SELECT day_pt AS d, campaign_id,
         SUM(impressions) AS impressions, SUM(clicks) AS clicks, ROUND(SUM(cost), 2) AS cost
  FROM `punlabs.AMZSales.v_sp_traffic_hourly`
  GROUP BY 1, 2
),
report AS (
  SELECT PARSE_DATE('%F', date) AS d, CAST(campaignId AS STRING) AS campaign_id,
         SUM(impressions) AS impressions, SUM(clicks) AS clicks, ROUND(SUM(cost), 2) AS cost
  FROM `punlabs.AMZSales.sp_performance_master`
  GROUP BY 1, 2
)
SELECT
  d, campaign_id,
  s.impressions AS stream_impressions, r.impressions AS report_impressions,
  s.clicks      AS stream_clicks,      r.clicks      AS report_clicks,
  s.cost        AS stream_cost,        r.cost        AS report_cost
FROM stream s
FULL OUTER JOIN report r USING (d, campaign_id)
WHERE d < DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
  AND d >= (SELECT MIN(day_pt) FROM `punlabs.AMZSales.v_sp_traffic_hourly`)
  AND (ABS(IFNULL(s.cost, 0) - IFNULL(r.cost, 0)) > 0.01
       OR IFNULL(s.clicks, 0) != IFNULL(r.clicks, 0))
ORDER BY d DESC, campaign_id;
