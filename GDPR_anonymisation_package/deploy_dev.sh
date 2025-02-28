#!/bin/bash

# Step 1: Run the topic schema update first
echo "Running the pubsub schema manager script..."
export ENV='dev'
python3 anonymisation_service/pubsub_schema_manager.py

# Step 2: Deploy the Google Cloud Function
echo "Deploying the Google Cloud Function..."
gcloud functions deploy GDPR_anonymization_dev \
    --runtime python310 \
    --trigger-http \
    --region europe-west1 \
    --entry-point run_anonymization \
    --service-account gcs-data-handler@sambla-data-staging-compliance.iam.gserviceaccount.com \
    --set-env-vars ENV=dev

# Step 3: Deploy the Google Cloud Scheduler
echo "Deploying the Google Cloud Scheduler..."
gcloud scheduler jobs update http gdpr_anonymize_dev_trigger \
  --location "europe-west1" \
  --description "Triggers the dev anonymisation service" \
  --schedule "5 0 * * *" \
  --uri "https://europe-west1-sambla-data-staging-compliance.cloudfunctions.net/GDPR_anonymization_dev" \
  --http-method POST \
  --time-zone "Europe/Stockholm" \
  --max-retry-attempts 2 \
  --oidc-service-account-email "gcs-data-handler@sambla-data-staging-compliance.iam.gserviceaccount.com" \