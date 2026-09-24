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

Auth is a single brand access token, `FAIRE-API-ACCESS-TOKEN`, sent as
`X-FAIRE-ACCESS-TOKEN`. It is generated once in the Brand Portal (Settings >
Integrations > "Have an unpublished integration?" > enter the app token from
`FAIRE-API-APP-ID` > Generate API key, or go straight to
`/brand-portal/integrations/<app token>`) and stored with. The app id/secret
pair is only for OAuth-flow tokens and is not used.

    pbpaste | tr -d '[:space:]' | gcloud secrets create FAIRE-API-ACCESS-TOKEN --project punlabs --data-file=- --replication-policy=automatic

Then `bash ops/cloud_run/setup_faire.sh` (grants, DDL, deploy, schedule, run once).

Verify:

    SELECT status, started_at, rows_written, error FROM `punlabs.AMZSales.pipeline_run_log`
    WHERE pipeline = 'faire_daily' ORDER BY started_at DESC LIMIT 5;

# Etsy daily load (etsy-daily)

A fourth Cloud Run job from the same image, running `python -m pipelines.etsy.load`
daily at 06:45 America/New_York. It pulls receipts modified since the last run
(two-day overlap; the first run pulls the whole history), the last 45 days of
payment-ledger entries (replaced each run), and a snapshot of every listing
with its inventory, into `punlabs.EtsySales` (`etsy_receipts_raw`,
`etsy_ledger_raw`, `etsy_listings_raw`). Views in `sql/etsy/ddl.sql` flatten
them (`v_etsy_receipts`, `v_etsy_transactions`, `v_etsy_ledger`,
`v_etsy_listings`) and `v_etsy_sold_items_legacy` reproduces the old CSV
export's columns.

Auth is Etsy Open API v3: an app API key (`etsy-api-keystring` +
`etsy-api-shared-secret`, sent as `x-api-key: keystring:secret`) plus an OAuth
2.0 token for the shop. The refresh token lasts 90 days and rotates on every
refresh, so the job saves the new one to `etsy-oauth-refresh-token` (it needs
`secretVersionAdder` on that secret; the setup script grants it).

One-time, by the shop owner:

1. Create an app at etsy.com/developers/your-apps with callback URL exactly
   `http://localhost:3003/oauth/redirect`. Copy the keystring and shared secret:

       pbpaste | tr -d '[:space:]' | gcloud secrets create etsy-api-keystring     --project punlabs --data-file=- --replication-policy=automatic
       pbpaste | tr -d '[:space:]' | gcloud secrets create etsy-api-shared-secret --project punlabs --data-file=- --replication-policy=automatic

2. Authorize the shop (opens a browser, stores the refresh token and shop id):

       .venv/bin/python ops/etsy_authorize.py

3. `bash ops/cloud_run/setup_etsy.sh` (grants, DDL, deploy, schedule, run once).

If a run fails with `invalid_grant`, re-run step 2.

Verify:

    SELECT status, started_at, rows_written, error FROM `punlabs.AMZSales.pipeline_run_log`
    WHERE pipeline = 'etsy_daily' ORDER BY started_at DESC LIMIT 5;

# Shopify daily load (shopify-daily)

A fifth Cloud Run job from the same image, running `python -m pipelines.shopify.load`
daily at 07:00 America/New_York. It pulls orders updated since the last run
(two-day overlap; the first run pulls the whole history) as one bulk
operation, a snapshot of every product with its variants and inventory by
location as a second bulk operation, and Shopify Payments payouts, into
`punlabs.ShopifySales` (`shopify_orders_raw`, `shopify_products_raw`,
`shopify_payouts_raw`). Views in `sql/shopify/ddl.sql` flatten them
(`v_shopify_orders`, `v_shopify_order_items`, `v_shopify_refunds`,
`v_shopify_refund_items`, `v_shopify_transactions`, `v_shopify_products`,
`v_shopify_inventory`, `v_shopify_payouts`) and `v_shopify_sales_by_day`
rebuilds Shopify's Sales by day report (the legacy
`PL-ShopifySales-SalesbyDay` columns).

Auth is the client credentials grant. Shopify stopped allowing new
admin-created custom apps on 2026-01-01, so the app (`punlabs-data`) lives in
the Dev Dashboard (dev.shopify.com > Apps) and has no static token: the job
exchanges the app's client id and secret for a 24-hour Admin API access token
at the start of every run. Setup, once: Dev Dashboard > Create app > Versions
> Create version with Admin API scopes `read_orders`, `read_all_orders`
(without it only 60 days of orders are visible), `read_products`,
`read_inventory`, `read_locations`, `read_shopify_payments_payouts`,
`read_shopify_payments_accounts` > Release > Install app on Pop Colors. Then
App settings > Credentials, copy the client id and the secret:

    pbpaste | tr -d '[:space:]' | gcloud secrets create shopify-client-id     --project punlabs --data-file=- --replication-policy=automatic
    pbpaste | tr -d '[:space:]' | gcloud secrets create shopify-client-secret --project punlabs --data-file=- --replication-policy=automatic

Then `bash ops/cloud_run/setup_shopify.sh` (grants, DDL, deploy, schedule, run once).

Size, 2026-09-24: 59,666 orders back to the store's start (about 100 a year
lately), 453 with refunds, 75 products / 78 variants, 1,368 payouts. The
full-history pull takes about 12 minutes (the orders bulk operation alone is
8), so the first load was run locally with `python -m pipelines.shopify.load`
before the job was deployed; the daily incremental run takes under a minute.

If a run fails with "client credentials grant failed", the secret was rotated
in the Dev Dashboard or the app was uninstalled: `gcloud secrets versions add
shopify-client-secret ...` with the new secret, or reinstall the app. Scope
changes need a new app version (Versions > Create version) and a reinstall.
The API version is pinned in `pipelines/lib/shopify.py` (`API_VERSION`);
Shopify supports each version for 12 months, so bump it yearly.

Verify:

    SELECT status, started_at, rows_written, error FROM `punlabs.AMZSales.pipeline_run_log`
    WHERE pipeline = 'shopify_daily' ORDER BY started_at DESC LIMIT 5;
