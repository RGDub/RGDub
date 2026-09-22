#!/usr/bin/env bash
# One-time setup for the Marketing Stream poller on Cloud Run. Idempotent.
# Run from the repository root:  bash ops/cloud_run/setup.sh
set -euo pipefail
PROJECT=punlabs; REGION=us-central1; SA=amzsales@punlabs.iam.gserviceaccount.com
AWS_ACCOUNT=483692969999; AWS_REGION=us-east-1
QUEUE_URL=https://sqs.us-east-1.amazonaws.com/$AWS_ACCOUNT/amazon-marketing-stream
QUEUE_ARN=arn:aws:sqs:$AWS_REGION:$AWS_ACCOUNT:amazon-marketing-stream
DLQ_ARN=arn:aws:sqs:$AWS_REGION:$AWS_ACCOUNT:amazon-marketing-stream-dlq
POLLER_USER=ads-stream-poller

echo "== 1. Google APIs"
gcloud services enable cloudscheduler.googleapis.com run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com --project $PROJECT

echo "== 2. AWS least-privilege user for the poller"
aws iam get-user --user-name $POLLER_USER >/dev/null 2>&1 || aws iam create-user --user-name $POLLER_USER >/dev/null
aws iam put-user-policy --user-name $POLLER_USER --policy-name sqs-marketing-stream --policy-document "{
  \"Version\": \"2012-10-17\",
  \"Statement\": [{\"Effect\": \"Allow\",
    \"Action\": [\"sqs:ReceiveMessage\", \"sqs:DeleteMessage\", \"sqs:DeleteMessageBatch\", \"sqs:GetQueueAttributes\", \"sqs:ChangeMessageVisibility\"],
    \"Resource\": [\"$QUEUE_ARN\", \"$DLQ_ARN\"]}]}"

echo "== 3. Access key -> Secret Manager (never printed)"
if ! gcloud secrets describe aws-ads-stream-key-id --project $PROJECT >/dev/null 2>&1; then
  KEYJSON=$(aws iam create-access-key --user-name $POLLER_USER --output json)
  python3 -c 'import json,sys; print(json.load(sys.stdin)["AccessKey"]["AccessKeyId"], end="")' <<<"$KEYJSON" \
    | gcloud secrets create aws-ads-stream-key-id --project $PROJECT --data-file=- --replication-policy=automatic
  python3 -c 'import json,sys; print(json.load(sys.stdin)["AccessKey"]["SecretAccessKey"], end="")' <<<"$KEYJSON" \
    | gcloud secrets create aws-ads-stream-secret --project $PROJECT --data-file=- --replication-policy=automatic
  unset KEYJSON
  echo "   created key for $POLLER_USER and stored both halves"
else
  echo "   secrets already exist; keeping the existing key"
fi
for s in aws-ads-stream-key-id aws-ads-stream-secret; do
  gcloud secrets add-iam-policy-binding $s --project $PROJECT --member serviceAccount:$SA --role roles/secretmanager.secretAccessor --quiet >/dev/null
done

echo "== 4. Cloud Run job (builds the image from the repo root Dockerfile)"
gcloud run jobs deploy ads-stream-poller --source . --region $REGION --project $PROJECT \
  --service-account $SA \
  --set-secrets AWS_ACCESS_KEY_ID=aws-ads-stream-key-id:latest,AWS_SECRET_ACCESS_KEY=aws-ads-stream-secret:latest \
  --set-env-vars AWS_DEFAULT_REGION=$AWS_REGION \
  --args="--queue-url,$QUEUE_URL,--max-seconds,240" \
  --task-timeout 600 --max-retries 1 --quiet

echo "== 5. Cloud Scheduler: every 5 minutes"
URI="https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/jobs/ads-stream-poller:run"
if gcloud scheduler jobs describe ads-stream-poller --location $REGION --project $PROJECT >/dev/null 2>&1; then
  gcloud scheduler jobs update http ads-stream-poller --location $REGION --project $PROJECT --schedule "*/5 * * * *" \
    --uri "$URI" --http-method POST --oauth-service-account-email $SA --quiet
else
  gcloud scheduler jobs create http ads-stream-poller --location $REGION --project $PROJECT --schedule "*/5 * * * *" \
    --uri "$URI" --http-method POST --oauth-service-account-email $SA --quiet
fi

echo "== 6. Run once now and wait"
gcloud run jobs execute ads-stream-poller --region $REGION --project $PROJECT --wait --quiet
echo "done. Verify with: SELECT * FROM punlabs.AMZSales.pipeline_run_log WHERE pipeline='ads_stream_poller' ORDER BY started_at DESC LIMIT 3"
