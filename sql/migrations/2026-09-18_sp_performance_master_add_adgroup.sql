-- Carry the ad-group grain into sp_performance_master.
--
-- Why: the Sponsored Products advertised-product report is emitted per
-- (date, campaign, ad group, advertised SKU). Without adGroupId the table
-- cannot represent its own grain, which is what made 401 legitimate rows look
-- like duplicates (docs/RUNBOOK.md §2). Run once, before the first run of
-- pipelines/sp_ads_daily.py. Historical rows keep NULL adGroupId; the loader
-- fills it for every day it re-pulls (trailing 31 days per run), and a one-off
-- backfill run for older dates fills the rest within the 95-day API lookback.

ALTER TABLE `punlabs.AMZSales.sp_performance_master`
  ADD COLUMN IF NOT EXISTS adGroupName STRING,
  ADD COLUMN IF NOT EXISTS adGroupId   INT64,
  ADD COLUMN IF NOT EXISTS adId        INT64;

ALTER TABLE `punlabs.AMZSales._sp_load_staging`
  ADD COLUMN IF NOT EXISTS adGroupName STRING,
  ADD COLUMN IF NOT EXISTS adGroupId   INT64,
  ADD COLUMN IF NOT EXISTS adId        INT64;
