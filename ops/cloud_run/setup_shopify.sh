#!/usr/bin/env bash
# Shopify daily load on Cloud Run. Idempotent.
# Run from the repository root:  bash ops/cloud_run/setup_shopify.sh
#
# Same image as the other jobs (repo-root Dockerfile), entrypoint overridden to
# pipelines.shopify.load. Tables and views: sql/shopify/ddl.sql.
#
# Prerequisite: the app's client credentials in Secret Manager as
# shopify-client-id and shopify-client-secret. The app (punlabs-data) lives in
# the Shopify Dev Dashboard (dev.shopify.com > Apps), with Admin API scopes
# read_orders, read_all_orders, read_products, read_inventory, read_locations,
# read_shopify_payments_payouts, read_shopify_payments_accounts on its active
# version, installed on Pop Colors. Settings > Credentials > copy each:
#   pbpaste | tr -d '[:space:]' | gcloud secrets create shopify-client-id     --project punlabs --data-file=- --replication-policy=automatic
#   pbpaste | tr -d '[:space:]' | gcloud secrets create shopify-client-secret --project punlabs --data-file=- --replication-policy=automatic
# The job exchanges them for a 24-hour access token on every run.
set -euo pipefail
PROJECT=punlabs; REGION=us-central1; SA=amzsales@punlabs.iam.gserviceaccount.com
JOB=shopify-daily

echo "== 0. secrets readable by the job"
for s in shopify-client-id shopify-client-secret; do
  gcloud secrets describe $s --project $PROJECT >/dev/null
  gcloud secrets add-iam-policy-binding $s --project $PROJECT --member serviceAccount:$SA \
    --role roles/secretmanager.secretAccessor --quiet >/dev/null
done

echo "== 1. tables and views"
bq query --use_legacy_sql=false --project_id=$PROJECT < sql/shopify/ddl.sql >/dev/null

echo "== 2. Cloud Run job"
gcloud run jobs deploy $JOB --source . --region $REGION --project $PROJECT \
  --service-account $SA \
  --command python --args="-m,pipelines.shopify.load" \
  --task-timeout 3600 --max-retries 1 --quiet

echo "== 3. Cloud Scheduler: daily 07:00 America/New_York"
URI="https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/jobs/$JOB:run"
if gcloud scheduler jobs describe $JOB --location $REGION --project $PROJECT >/dev/null 2>&1; then
  gcloud scheduler jobs update http $JOB --location $REGION --project $PROJECT --schedule "0 7 * * *" \
    --time-zone America/New_York --uri "$URI" --http-method POST --oauth-service-account-email $SA --quiet
else
  gcloud scheduler jobs create http $JOB --location $REGION --project $PROJECT --schedule "0 7 * * *" \
    --time-zone America/New_York --uri "$URI" --http-method POST --oauth-service-account-email $SA --quiet
fi

echo "== 4. Run once now and wait"
gcloud run jobs execute $JOB --region $REGION --project $PROJECT --wait --quiet
echo "done. Verify with: SELECT day, orders, gross_sales, net_sales, total_sales FROM punlabs.ShopifySales.v_shopify_sales_by_day ORDER BY day DESC LIMIT 10"
