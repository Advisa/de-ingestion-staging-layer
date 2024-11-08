# de-ingestion-staging-layer

This repository contains the Terraform configuration for managing resources in Google Cloud Platform for the **Staging Compliance Project**. The infrastructure is designed to handle multiple services, including data source migrations, authorized views, policy tags, and schema management, while ensuring compliance with organizational requirements.

### Folder Structure
    .
    ├── anonymisation_service
    │   └── templates
    └── prod
        ├── authorized_view_service
        │   └── templates
        ├── modules
        │   ├── auth_views
        │   ├── lvs
        │   │   ├── bigquery
        │   │   └── cloudrun
        │   ├── rahalaitos
        │   │   ├── bigquery
        │   │   └── gcs
        │   └── taxonomy
        ├── policy_tags_service
        │   ├── csv_exporter
        │   │   └── outputs
        │   └── policy_assignment
        │       └── templates
        ├── schemas
        │   ├── lvs
        │   ├── rahalaitos
        │   └── taxonomy
        └── service_accounts_keyfiles