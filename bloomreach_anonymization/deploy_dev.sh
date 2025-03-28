#!/bin/bash

# Step 1: Run the topic schema update first
echo "Running the pubsub schema manager script..."
export ENV='dev'
python3 pubsub_subscriber_manager.py

# Step 2: Deploy the Google Cloud Function
echo "Deploying the Google Cloud Function..."
gcloud functions deploy bloomreach_anonymization_dev \
    --runtime python310 \
    --trigger-http \
    --region europe-west1 \
    --entry-point run_br_anonymization \
    --service-account gcs-data-handler@sambla-data-staging-compliance.iam.gserviceaccount.com \
    --set-env-vars ENV=dev

# Step 3: Deploy the Google Cloud Scheduler
echo "Deploying the Google Cloud Scheduler..."
gcloud scheduler jobs update http bloomreach_anonymize_dev_trigger \
  --location "europe-west1" \
  --description "Triggers the dev anonymisation service" \
  --schedule "5 1 * * *" \
  --uri "https://europe-west1-sambla-data-staging-compliance.cloudfunctions.net/bloomreach_anonymization_dev" \
  --http-method POST \
  --time-zone "Europe/Stockholm" \
  --max-retry-attempts 2 \
  --oidc-service-account-email "gcs-data-handler@sambla-data-staging-compliance.iam.gserviceaccount.com" \