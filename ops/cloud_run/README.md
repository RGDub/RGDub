# Stream poller on Cloud Run

The Marketing Stream poller is the one piece that cannot live in the daily
BigQuery pipelines: it has to drain the SQS queue every few minutes. It runs as
a Cloud Run job triggered by Cloud Scheduler.

## One-time setup

1. Enable the APIs (Cloud Scheduler is not enabled on punlabs as of 2026-09-21):

       gcloud services enable cloudscheduler.googleapis.com run.googleapis.com cloudbuild.googleapis.com --project punlabs

2. AWS credentials for the job. Stopgap: an access key for the `AMZ-Ads-Stream`
   IAM user in Secret Manager as `aws-ads-stream-key-id` and
   `aws-ads-stream-secret`, readable by `amzsales@`. That user currently holds
   AdministratorAccess; scope it to `sqs:ReceiveMessage`, `sqs:DeleteMessage`,
   `sqs:GetQueueAttributes` on the queue before the key goes anywhere. The
   better end state is web identity federation (AWS trusts the Cloud Run
   service account's identity token), which needs no long-lived key.

3. Deploy from the repository root:

       gcloud run jobs deploy ads-stream-poller --source . --region us-central1 --project punlabs \
         --service-account amzsales@punlabs.iam.gserviceaccount.com \
         --set-secrets AWS_ACCESS_KEY_ID=aws-ads-stream-key-id:latest,AWS_SECRET_ACCESS_KEY=aws-ads-stream-secret:latest \
         --set-env-vars AWS_DEFAULT_REGION=us-east-1 \
         --args=--queue-url,https://sqs.us-east-1.amazonaws.com/483692969999/amazon-marketing-stream,--max-seconds,240 \
         --task-timeout 600 --max-retries 1

   `--source .` uses ops/cloud_run/Dockerfile via the repo-root `Dockerfile`
   symlink.

4. Schedule it every 5 minutes:

       gcloud scheduler jobs create http ads-stream-poller --location us-central1 --project punlabs \
         --schedule "*/5 * * * *" \
         --uri "https://run.googleapis.com/v2/projects/punlabs/locations/us-central1/jobs/ads-stream-poller:run" \
         --http-method POST --oauth-service-account-email amzsales@punlabs.iam.gserviceaccount.com

Overlapping runs are harmless: SQS visibility timeouts stop two pollers
processing one message, and inserts are keyed on `idempotency_id`.

## Verify

    SELECT pipeline, status, MAX(started_at) FROM `punlabs.AMZSales.pipeline_run_log`
    WHERE pipeline = 'ads_stream_poller' GROUP BY 1, 2;

and the queue depth should stay near zero:

    aws sqs get-queue-attributes --region us-east-1 \
      --queue-url https://sqs.us-east-1.amazonaws.com/483692969999/amazon-marketing-stream \
      --attribute-names ApproximateNumberOfMessages

# QuickBooks Online daily load (qbo-daily)

A second Cloud Run job built from the same image, running
`python -m pipelines.qbo.load` daily at 06:00 America/New_York. It pulls every
QuickBooks transaction and list record into `punlabs.QBO.qbo_raw`; the views
in `sql/qbo/ddl.sql` flatten it (`v_transactions`, `v_transaction_lines`, ...).

One-time prerequisite, by a project owner. QuickBooks rotates the refresh token
about daily and the job must save the new one, so the service account needs to
add versions to that one secret:

    gcloud secrets add-iam-policy-binding qbo-client-refreshtoken-production --project punlabs \
      --member serviceAccount:amzsales@punlabs.iam.gserviceaccount.com \
      --role roles/secretmanager.secretVersionAdder

Then `bash ops/cloud_run/setup_qbo.sh` (deploy, schedule, run once).

If a run fails with `invalid_grant`, the refresh token has expired or been
revoked: get a new one from the Intuit OAuth 2.0 Playground (app PunData,
Production) and save it with
`pbpaste | tr -d '[:space:]' | gcloud secrets versions add qbo-client-refreshtoken-production --project punlabs --data-file=-`.

Verify:

    SELECT status, started_at, rows_written, error FROM `punlabs.AMZSales.pipeline_run_log`
    WHERE pipeline = 'qbo_daily' ORDER BY started_at DESC LIMIT 5;

# Faire daily load (faire-daily)

A third Cloud Run job from the same image, running `python -m pipelines.faire.load`
daily at 06:30 America/New_York. It pulls every order updated since the last
run (two-day overlap) plus the product catalog into `punlabs.FaireSales`
(`faire_orders_raw`, `faire_products_raw`); the views in `sql/faire/ddl.sql`
flatten them (`v_faire_orders`, `v_faire_order_items`, `v_faire_shipments`,
`v_faire_products`).

Auth is the Faire External API v2's two headers: app credentials
(`FAIRE-API-APP-ID`, `FAIRE-API-SECRET-ID`, created 2026-06) and a brand access
token, `FAIRE-API-ACCESS-TOKEN`. The token is generated once in the Brand
Portal (Settings > Integrations > "Have an unpublished integration?" > enter the
app's token) and stored with

    pbpaste | tr -d '[:space:]' | gcloud secrets create FAIRE-API-ACCESS-TOKEN --project punlabs --data-file=- --replication-policy=automatic

Then `bash ops/cloud_run/setup_faire.sh` (grants, DDL, deploy, schedule, run once).

Verify:

    SELECT status, started_at, rows_written, error FROM `punlabs.AMZSales.pipeline_run_log`
    WHERE pipeline = 'faire_daily' ORDER BY started_at DESC LIMIT 5;
