# Initializing a service account for terraform to run and deploy resources in sambla-data-staging-compliance project effortlessly
resource "google_service_account" "sa_terraform_admin" {
  account_id   = "de-compliance-terraform-admin"
  display_name = "terraform-adm"
  description = "A service account for terraform to manage and create resources within GCP"
}
# This account is intended for external applications that need to read and edit masked data within compliance project. 
resource "google_service_account" "sa_masked_reader" {
  account_id   = "de-compliance-masked"
  display_name = "compliance-masked-reader"
  description = "A service account for restricted reader access to masked LVS data for approved external users"
}

# This account is intended for external applications that need to read and edit raw data within compliance project. 
resource "google_service_account" "sa_fine_grained_reader" {
  account_id   = "de-compliance-fine-grained"
  display_name = "compliance-fine-grained-reader"
  description = "A service account for reader access to raw/unmasked data for approved external users"
}

resource "google_project_iam_member" "project_permissions_owner" {
  project    = var.project_id
  role   = "roles/owner" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa_terraform_admin.email}"  # Grant access to the service account
}

# Provides permissions to query raw (unmasked) data within the project.
resource "google_project_iam_member" "project_permissions_fine_grained_reader" {
  project    = var.project_id
  role               = "roles/datacatalog.categoryFineGrainedReader" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa_fine_grained_reader.email}"  # Grant access to the service account
}
# Provides permissions to read data and metadata from the table or view in BigQuery
resource "google_project_iam_member" "bq_permissions_fine_grained_reader" {
  project    = var.project_id
  role               = "roles/bigquery.dataViewer" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa_fine_grained_reader.email}"  # Grant access to the service account
}
# Provides permissions to masked read access to columns associated with a data policy.
resource "google_project_iam_member" "project_permissions_masked_reader" {
  project    = var.project_id
  role               = "roles/bigquerydatapolicy.maskedReader" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa_masked_reader.email}"  # Grant access to the service account 
}
# Provides permissions to read data and metadata from the table or view in BigQuery
resource "google_project_iam_member" "bq_permissions_masked_reader" {
  project    = var.project_id
  role               = "roles/bigquery.dataViewer" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa_masked_reader.email}"  # Grant access to the service account
}

# Provides permissions to run jobs, including queries, within the project.
resource "google_project_iam_member" "project_permissions_masked_reader_job_user" {
  project    = var.project_id
  role               = "roles/bigquery.jobUser" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa_masked_reader.email}"  # Grant access to the service account
}
# Provides permissions to run jobs, including queries, within the project.
resource "google_project_iam_member" "project_permissions_fine_grained_reader_job_user" {
  project    = var.project_id
  role               = "roles/bigquery.jobUser" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa_fine_grained_reader.email}"  # Grant access to the service account
}

data "google_project" "project" {
  project_id = var.project_id
}
/*
resource "google_kms_crypto_key_iam_member" "bigquery_kms_key_access_terraform" {
  crypto_key_id = var.kms_crypto_key_id
  role          = "roles/cloudkms.cryptoKeyAdmin"
  member        =  "serviceAccount:${google_service_account.sa_terraform_admin.email}"
}

resource "google_kms_crypto_key_iam_member" "bigquery_kms_key_access" {
  crypto_key_id = var.kms_crypto_key_id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:bq-${data.google_project.project.number}@bigquery-encryption.iam.gserviceaccount.com"
}
*/