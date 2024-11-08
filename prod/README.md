# de-ingestion-staging-layer

This repository contains the Terraform configuration for managing resources in Google Cloud Platform for the **Staging Compliance Project**. The infrastructure is designed to handle multiple services, including data source migrations, authorized views, policy tags, and schema management, while ensuring compliance with organizational requirements.

## Folder Structure

.
├── authorized_view_service
│ └── templates
├── modules
│ ├── auth_views
│ ├── lvs
│ │ ├── bigquery
│ │ └── cloudrun
│ ├── rahalaitos
│ │ ├── bigquery
│ │ └── gcs
│ └── taxonomy
├── policy_tags_service
│ ├── csv_exporter
│ │ └── outputs
│ └── policy_assignment
│ └── templates
├── schemas
│ ├── lvs
│ ├── rahalaitos
│ └── taxonomy
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

To authenticate with GCP, ensure that you have the appropriate terraform admin service account key files located in your local directory. This key is required for Terraform to interact with your GCP environment.

### Python Dependencies

This project may require certain Python libraries. To ensure that you have the correct libraries installed, you can download and install the necessary dependencies from the `requirements.txt` file.

1. **Download the `requirements.txt` File**:

   - First, ensure the `requirements.txt` file is available in the root directory of the project. This file contains a list of all the required Python libraries.

2. **Install Dependencies**:
   - Install the required Python dependencies by running the following command:
     ```bash
     pip install -r requirements.txt
     ```

### Set Up Google Cloud Credentials

In order to authenticate with Google Cloud, you need to configure your environment to use the service account credentials. To do this, follow these steps:

1. **Place the Service Account Key File in Your Project Directory**

   - Download the terraform admin service account key file.
   - Place the downloaded JSON key file in your local directory.

2. **Set the `GOOGLE_APPLICATION_CREDENTIALS` Environment Variable**

   To authenticate Terraform with Google Cloud, you need to set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to the service account key file. You can set this environment variable once for your terminal session or permanently by editing your shell configuration file.

   **For Bash (macOS/Linux)**:

   - Open your terminal and run:
     ```bash
     sudo nano ~/.bash_profile
     ```
   - Add the following line to set the `GOOGLE_APPLICATION_CREDENTIALS` variable:
     ```bash
     export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
     ```
   - Replace `/path/to/your/service-account-key.json` with the actual path to your downloaded service account key file.
   - Save the file (`CTRL + X`, then `Y`, then `Enter`).
   - Apply the changes by running:
     ```bash
     source ~/.bash_profile
     ```

   **For Zsh (macOS)**:

   - If you're using Zsh, edit the `~/.zshrc` file instead:
     ```bash
     nano ~/.zshrc
     ```
   - Add the same `export GOOGLE_APPLICATION_CREDENTIALS` line and apply the changes:
     ```bash
     source ~/.zshrc
     ```

3. **Verify the Environment Variable**

   - After setting the environment variable, verify that it is set correctly by running:
     ```bash
     echo $GOOGLE_APPLICATION_CREDENTIALS
     ```
   - This should return the path to the service account key file. If it returns nothing, check the previous steps for any errors.

4. **Run Terraform**
   - Once the credentials are set, you should be able to run Terraform commands like `terraform init` without any issues:
     ```bash
     terraform init
     terraform validate
     terraform plan -var-file="values.tfvars"
     terraform apply -var-file="values.tfvars"
     ```

---

## Important Notes on Running Bash Commands

- **Obtain the Terraform state file** (`terraform.tfstate`) before applying any commands below. This state file contains the latest information about your infrastructure and is essential to ensure changes are tracked correctly.

- **No changes should be feasible** in the `terraform plan` and `terraform apply` commands after pulling this project straight from the main branch. This helps ensure that no unintended changes are made to your environment.

- **Only apply changes** once you have reviewed and approved them. Make sure the proposed changes do not conflict with the current production configuration. This is crucial to avoid any disruptions or unwanted changes in the production environment.

---

This setup ensures that anyone cloning this project can easily install the necessary dependencies and authenticate with GCP, allowing them to run Terraform commands without issues.
