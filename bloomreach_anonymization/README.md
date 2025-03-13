# Bloomreach Anonymization Service

## Overview
This project is a Google Cloud-based anonymization service that processes requests via Pub/Sub and anonymizes customer data using the Bloomreach API. It includes:

- A **Pub/Sub subscription manager** that ensures required topics and subscriptions are correctly configured.
- A **Cloud Function** that processes anonymization requests.
- **deployment scripts** to manage environment setup and deployment.
- A **Google Cloud Scheduler** job to trigger the anonymization service daily.

## Components

### 1. Pub/Sub Subscription Manager (`pubsub_subscriber_manager.py`)
Ensures that the required Pub/Sub subscription exists. If not, it creates one with appropriate settings such as retention duration and retry policies.

### 2. Bloomreach Anonymizer (`bloomreach_anonymizer.py`)
- Pulls anonymization requests from the Pub/Sub subscription.
- Extracts relevant customer identifiers.
- Hashes sensitive information before sending it to Bloomreach.
- Determines Bloomreach projects based on customer market
- Sends anonymization requests to Bloomreach API with retries and exponential backoff.

### 3. Deployment Scripts (`deploy_dev.sh` & `deploy_prod.sh`)
Automates the deployment of the PubSub Subscription Manager, Google Cloud Function and Cloud Scheduler job.

## Installation & Setup

### Prerequisites for local testing
- Google Cloud SDK installed and configured
- Python 3.10 or later
- Required Python dependencies installed

```bash
pip install -r requirements.txt
```

### Configuration
The service is configured via a YAML file (`config.yaml`). This file defines:
- Project IDs
- Pub/Sub topic and subscription names
- Bloomreach API credentials (stored in Google Secret Manager)

Example configuration:
```yaml
dev:
  anonymization_service:
    raw_layer_project: my-gcp-project
    pubsub_topic: anonymization-topic
    pubsub_subscriber: anonymization-subscription
    topic_retention_days: 1
    secret_name: projects/my-gcp-project/secrets/bloomreach-api-secrets
    anonymize_markets:
      - market: SE
        bloomreach_projects: my-bloomreach-project
```

## Deployment

### Deploy to Development
```bash
bash deploy_dev.sh
```

### Deploy to Production
```bash
bash deploy_prod.sh
```

## How It Works
1. **Pub/Sub Subscription Management**: Ensures the subscription exists.
2. **Message Processing**:
   - Pulls messages from Pub/Sub.
   - Extracts and hashes relevant customer identifiers.
   - Sends anonymization requests to Bloomreach.
   - Acknowledges successful messages to prevent reprocessing.
3. **Automatic Scheduling**: The Cloud Scheduler triggers the anonymization function daily at 01:05.

## Logging & Monitoring
Logs are captured in Google Cloud Logging and can be accessed via:
```bash
gcloud logging read "resource.type=cloud_function AND resource.labels.function_name=bloomreach_anonymization_dev" --limit 100
```

OR

```bash
gcloud logging read "resource.type=cloud_function AND resource.labels.function_name=bloomreach_anonymization" --limit 100
```

## Troubleshooting
- **Subscription Not Found**: Ensure the correct Pub/Sub topic is configured and deployed.
- **Permission Issues**: Ensure the correct IAM roles are assigned to the service account.
- **Function Deployment Errors**: Check the Cloud Build logs for deployment failures.