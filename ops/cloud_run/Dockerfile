# Marketing Stream poller as a Cloud Run job.
#   gcloud run jobs deploy ads-stream-poller --source . --region us-central1 \
#     --service-account amzsales@punlabs.iam.gserviceaccount.com \
#     --set-secrets AWS_ACCESS_KEY_ID=aws-ads-stream-key-id:latest,AWS_SECRET_ACCESS_KEY=aws-ads-stream-secret:latest \
#     --set-env-vars AWS_DEFAULT_REGION=us-east-1 \
#     --args=--queue-url,https://sqs.us-east-1.amazonaws.com/483692969999/amazon-marketing-stream,--max-seconds,240
# Build context is the repository root (see ops/cloud_run/README.md).
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY pipelines ./pipelines
RUN pip install --no-cache-dir ".[stream]"
ENTRYPOINT ["python", "-m", "pipelines.ads_stream.poller"]
