# de-ingestion-staging-layer

This repository contains the Terraform configuration for managing resources in Google Cloud Platform for the **Staging Compliance Project**. The infrastructure is designed to handle multiple services, including data source migrations, authorized views, policy tags, and schema management, while ensuring compliance with organizational requirements.

## Folder Structure
```bash
.
├── anonymisation_service
│   └── templates
└── prod
    ├── authorized_view_service
    │   └── templates
    ├── modules
    │   ├── auth_views
    │   ├── lvs
    │   │   └── bigquery
    │   ├── rahalaitos
    │   │   ├── bigquery
    │   │   └── gcs
    │   └── taxonomy
    │       └── assign_policies_scripts
    │           ├── schemas
    │           └── template_sql_files
    └── schemas
        ├── lvs
        └── rahalaitos


### Key Folders and Files

1. **authorized_view_service/**
   - Contains Python scripts and templates for managing the creation of authorized views. This service works in conjunction with BigQuery and Cloud Run for securing and processing data.
   - **templates/**: Includes configuration and SQL templates used for generating authorized views.

2. **modules/**
   - Contains reusable Terraform modules that define the resources and configurations needed for the project.
   - **auth_views/**: Module for managing authorized views.
   - **lvs/**: Includes configurations for BigQuery and Cloud Run services used in the authorized view service.
   - **rahalaitos/**: Modules for creating and managing BigQuery datasets and Google Cloud Storage (GCS) resources.
   - **taxonomy/**: Contains Terraform configurations and Python scripts for handling policy tags and their assignments within GCP.
      - **assign_policies_scripts/**
         - Used for exporting policy tag details to CSV files.
         - Used for assigning policy tags to resources within GCP.
      - **schemas/**
         - Contains schema definitions for taxonomy and policy tags

4. **schemas/**
   - Contains schema definitions for different services in the project.
   - **lvs/**, **rahalaitos/**: Schema files for different resources within each respective service.
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