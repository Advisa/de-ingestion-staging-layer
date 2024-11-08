# de-ingestion-staging-layer

This repository contains the Terraform configuration for managing resources in Google Cloud Platform for the **Staging Compliance Project**. The infrastructure is designed to handle multiple services, including data source migrations, authorized views, policy tags, and schema management, while ensuring compliance with organizational requirements.

## Folder Structure
.
├── authorized_view_service
│   ├── __pycache__
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
│   │   ├── __pycache__
│   │   └── outputs
│   └── policy_assignment
│       ├── __pycache__
│       └── templates
├── schemas
│   ├── lvs
│   ├── rahalaitos
│   └── taxonomy
└── service_accounts_keyfiles


### Key Folders and Files

1. **authorized_view_service/**
   - Contains Python scripts and templates for managing the creation of authorized views. This service works in conjunction with BigQuery and Cloud Run for securing and processing data.
   - **templates/**: Includes configuration and SQL templates used for generating authorized views.

2. **modules/**
   - Contains reusable Terraform modules that define the resources and configurations needed for the project.
   - **auth_views/**: Module for managing authorized views.
   - **lvs/**: Includes configurations for BigQuery and Cloud Run services used in the authorized view service.
   - **rahalaitos/**: Modules for creating and managing BigQuery datasets and Google Cloud Storage (GCS) resources.
   - **taxonomy/**: Modules for managing taxonomies and their associated resources in GCP.

3. **policy_tags_service/**
   - Contains Terraform configurations and Python scripts for handling policy tags and their assignments within GCP.
   - **csv_exporter/**: Used for exporting policy tag details to CSV files.
   - **policy_assignment/**: Module for assigning policy tags to resources within GCP.

4. **schemas/**
   - Contains schema definitions for different services in the project.
   - **lvs/**, **rahalaitos/**, **taxonomy/**: Schema files for different resources within each respective service.

5. **service_accounts_keyfiles/**
   - Stores service account key files for authentication with GCP. These keys are necessary for Terraform and other services to interact with GCP resources.

---

## Dependencies

### Terraform
Make sure you have Terraform installed on your local machine or CI/CD pipeline. The recommended version is listed in the `.terraform-version` file.

To install Terraform, follow the [official documentation](https://www.terraform.io/downloads.html).

### GCP Credentials
To authenticate with GCP, ensure that you have the appropriate service account keyfiles located in the `service_accounts_keyfiles/` directory. These keys are required for Terraform to interact with your GCP environment.

You can create a service account in GCP and download the corresponding key from the [IAM & Admin Console](https://console.cloud.google.com/iam-admin/serviceaccounts).

### Install Dependencies
Terraform modules are used throughout the project, and these dependencies should be installed and initialized before running any Terraform commands.

### Important Notes on Running Bash Commands

- **Obtain the Terraform state file** (`terraform.tfstate`) before applying any commands below. This state file contains the latest information about your infrastructure and is essential to ensure changes are tracked correctly.
  
- **No changes should be feasible** in the `terraform plan` and `terraform apply` commands after pulling this project straight from the main branch. This helps ensure that no unintended changes are made to your environment.

- **Only apply changes** once you have reviewed and approved them. Make sure the proposed changes do not conflict with the current production configuration. This is crucial to avoid any disruptions or unwanted changes in the production environment.


```bash
terraform init
terraform validate
terraform plan -var-file="values.tfvars" 
terraform apply -var-file="values.tfvars"