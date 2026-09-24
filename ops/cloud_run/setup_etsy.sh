#!/usr/bin/env bash
# Etsy daily load on Cloud Run. Idempotent.
# Run from the repository root:  bash ops/cloud_run/setup_etsy.sh
#
# Same image as the other jobs (repo-root Dockerfile), entrypoint overridden to
# pipelines.etsy.load. Tables and views: sql/etsy/ddl.sql.
#
# Prerequisites: an Etsy app (etsy.com/developers/your-apps) with callback URL
# http://localhost:3003/oauth/redirect, its keystring and shared secret in
# Secret Manager as etsy-api-keystring / etsy-api-shared-secret, and one run of
# ops/etsy_authorize.py, which stores etsy-oauth-refresh-token and etsy-shop-id.
set -euo pipefail
PROJECT=punlabs; REGION=us-central1; SA=amzsales@punlabs.iam.gserviceaccount.com
JOB=etsy-daily

echo "== 0. secrets readable by the job (and the refresh token writable: Etsy rotates it)"
for s in etsy-api-keystring etsy-api-shared-secret etsy-oauth-refresh-token etsy-shop-id; do
  gcloud secrets describe $s --project $PROJECT >/dev/null
  gcloud secrets add-iam-policy-binding $s --project $PROJECT --member serviceAccount:$SA \
    --role roles/secretmanager.secretAccessor --quiet >/dev/null
done
gcloud secrets add-iam-policy-binding etsy-oauth-refresh-token --project $PROJECT --member serviceAccount:$SA \
  --role roles/secretmanager.secretVersionAdder --quiet >/dev/null

echo "== 1. tables and views"
bq query --use_legacy_sql=false --project_id=$PROJECT < sql/etsy/ddl.sql >/dev/null

echo "== 2. Cloud Run job"
gcloud run jobs deploy $JOB --source . --region $REGION --project $PROJECT \
  --service-account $SA \
  --command python --args="-m,pipelines.etsy.load" \
  --task-timeout 1800 --max-retries 1 --quiet

echo "== 3. Cloud Scheduler: daily 06:45 America/New_York"
URI="https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/jobs/$JOB:run"
if gcloud scheduler jobs describe $JOB --location $REGION --project $PROJECT >/dev/null 2>&1; then
  gcloud scheduler jobs update http $JOB --location $REGION --project $PROJECT --schedule "45 6 * * *" \
    --time-zone America/New_York --uri "$URI" --http-method POST --oauth-service-account-email $SA --quiet
else
  gcloud scheduler jobs create http $JOB --location $REGION --project $PROJECT --schedule "45 6 * * *" \
    --time-zone America/New_York --uri "$URI" --http-method POST --oauth-service-account-email $SA --quiet
fi

echo "== 4. Run once now and wait (first run pulls the shop's whole history)"
gcloud run jobs execute $JOB --region $REGION --project $PROJECT --wait --quiet
echo "done. Verify with: SELECT order_date, COUNT(*) orders, ROUND(SUM(grand_total),2) sales FROM punlabs.EtsySales.v_etsy_receipts GROUP BY 1 ORDER BY 1 DESC LIMIT 10"
