#!/usr/bin/env bash
# Faire daily load on Cloud Run. Idempotent.
# Run from the repository root:  bash ops/cloud_run/setup_faire.sh
#
# Same image as the other jobs (repo-root Dockerfile), entrypoint overridden to
# pipelines.faire.load. Tables and views: sql/faire/ddl.sql.
#
# Prerequisite: the brand access token in Secret Manager as FAIRE-API-ACCESS-TOKEN,
# generated in the Faire Brand Portal (Settings > Integrations > "Have an
# unpublished integration?" > enter the app token > Generate API key). It is a
# classic brand token sent as X-FAIRE-ACCESS-TOKEN; the FAIRE-API-APP-ID /
# FAIRE-API-SECRET-ID pair is not used.
set -euo pipefail
PROJECT=punlabs; REGION=us-central1; SA=amzsales@punlabs.iam.gserviceaccount.com
JOB=faire-daily

echo "== 0. secrets readable by the job"
for s in FAIRE-API-ACCESS-TOKEN; do
  gcloud secrets describe $s --project $PROJECT >/dev/null
  gcloud secrets add-iam-policy-binding $s --project $PROJECT --member serviceAccount:$SA \
    --role roles/secretmanager.secretAccessor --quiet >/dev/null
done

echo "== 1. tables and views"
bq query --use_legacy_sql=false --project_id=$PROJECT < sql/faire/ddl.sql >/dev/null

echo "== 2. Cloud Run job"
gcloud run jobs deploy $JOB --source . --region $REGION --project $PROJECT \
  --service-account $SA \
  --command python --args="-m,pipelines.faire.load" \
  --task-timeout 900 --max-retries 1 --quiet

echo "== 3. Cloud Scheduler: daily 06:30 America/New_York"
URI="https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/jobs/$JOB:run"
if gcloud scheduler jobs describe $JOB --location $REGION --project $PROJECT >/dev/null 2>&1; then
  gcloud scheduler jobs update http $JOB --location $REGION --project $PROJECT --schedule "30 6 * * *" \
    --time-zone America/New_York --uri "$URI" --http-method POST --oauth-service-account-email $SA --quiet
else
  gcloud scheduler jobs create http $JOB --location $REGION --project $PROJECT --schedule "30 6 * * *" \
    --time-zone America/New_York --uri "$URI" --http-method POST --oauth-service-account-email $SA --quiet
fi

echo "== 4. Run once now and wait"
gcloud run jobs execute $JOB --region $REGION --project $PROJECT --wait --quiet
echo "done. Verify with: SELECT order_date, COUNT(*) orders, SUM(total_payout) payout FROM punlabs.FaireSales.v_faire_orders GROUP BY 1 ORDER BY 1 DESC LIMIT 10"
