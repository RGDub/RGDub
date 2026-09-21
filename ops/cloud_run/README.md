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
