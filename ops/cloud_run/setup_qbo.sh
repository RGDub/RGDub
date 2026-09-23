#!/usr/bin/env bash
# QuickBooks Online daily load on Cloud Run. Idempotent.
# Run from the repository root:  bash ops/cloud_run/setup_qbo.sh
#
# Uses the same image as the Marketing Stream poller (repo-root Dockerfile) with
# the entrypoint overridden to pipelines.qbo.load. Tables and views: sql/qbo/ddl.sql.
#
# Prerequisite (once, by a project owner): the job must be able to save the
# refresh token QuickBooks rotates, so amzsales@ needs
# roles/secretmanager.secretVersionAdder on qbo-client-refreshtoken-production
# (see ops/cloud_run/README.md). Without it the first rotation fails the run.
set -euo pipefail
PROJECT=punlabs; REGION=us-central1; SA=amzsales@punlabs.iam.gserviceaccount.com
JOB=qbo-daily

echo "== 1. Cloud Run job"
gcloud run jobs deploy $JOB --source . --region $REGION --project $PROJECT \
  --service-account $SA \
  --command python --args="-m,pipelines.qbo.load" \
  --task-timeout 900 --max-retries 1 --quiet

echo "== 2. Cloud Scheduler: daily 06:00 America/New_York"
URI="https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/jobs/$JOB:run"
if gcloud scheduler jobs describe $JOB --location $REGION --project $PROJECT >/dev/null 2>&1; then
  gcloud scheduler jobs update http $JOB --location $REGION --project $PROJECT --schedule "0 6 * * *" \
    --time-zone America/New_York --uri "$URI" --http-method POST --oauth-service-account-email $SA --quiet
else
  gcloud scheduler jobs create http $JOB --location $REGION --project $PROJECT --schedule "0 6 * * *" \
    --time-zone America/New_York --uri "$URI" --http-method POST --oauth-service-account-email $SA --quiet
fi

echo "== 3. Run once now and wait"
gcloud run jobs execute $JOB --region $REGION --project $PROJECT --wait --quiet
echo "done. Verify with: SELECT txn_type, COUNT(*) FROM punlabs.QBO.v_transactions GROUP BY 1"
