gcloud functions deploy GDPR_anonymization \
    --runtime python310 \
    --trigger-http \
    --region europe-west1 \
    --entry-point run_anonymization \
    --service-account gcs-data-handler@sambla-data-staging-compliance.iam.gserviceaccount.com \
    --set-env-vars ENV=prod
