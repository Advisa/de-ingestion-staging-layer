
# This account is intended for external applications that need to read raw LVS data. 
resource "google_service_account" "sa_reader" {
  account_id   = "de-compliance-lvs-reader"
  display_name = "lvs-reader"
  description = "A service account for read-only access to raw LVS data for approved external users"
}

# This account is intended for external applications that need to read and edit raw LVS data. 
resource "google_service_account" "sa_editor" {
  account_id   = "de-compliance-lvs-editor"
  display_name = "lvs-editor"
  description = "A service account for editor (developer) access to raw LVS data for approved external users"
}

# Provides permissions to run jobs, including queries, within the project.
resource "google_project_iam_member" "bq_permissions_reader_job_user" {
  project    = var.project_id
  role               = "roles/bigquery.jobUser" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa_reader.email}"  # Grant access to the service account
  
}
# Provides permissions to run jobs, including queries, within the project.
resource "google_project_iam_member" "bq_permissions_editor_job_user" {
  project    = var.project_id
  role               = "roles/bigquery.jobUser" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa_editor.email}"  # Grant access to the service account
  
}

# Add individual users to IAM roles on read-only service accounts
resource "google_service_account_iam_binding" "sa_permissions_reader_account_user" {
  service_account_id = google_service_account.sa_reader.name
  role               = "roles/iam.serviceAccountUser"  # Grant permission to use the service account

  members = [
    "user:adam.svenson@samblagroup.com",
    "user:aruldharani.kumar@samblagroup.com",
    "user:duygu.genc@samblagroup.com",
  ]
}
# Add individual users to IAM roles on developer service accounts
resource "google_service_account_iam_binding" "sa_permissions_editor_account_user" {
  service_account_id = google_service_account.sa_editor.name
  role               = "roles/iam.serviceAccountUser"  # Grant permission to use the service account

  members = [
    "user:adam.svenson@samblagroup.com",
    "user:aruldharani.kumar@samblagroup.com",
    "user:duygu.genc@samblagroup.com",
  ]
}

# Adding iam policy to manage permissions & access to dataset using reader service account 
resource "google_bigquery_dataset_iam_binding" "bq_permissions_reader" {
  dataset_id = google_bigquery_dataset.lvs_dataset.dataset_id
  role       = "roles/bigquery.dataViewer"

  members = [
    "serviceAccount:${google_service_account.sa_reader.email}",
    "user:aruldharani.kumar@samblagroup.com",
  ]
  depends_on = [ google_bigquery_dataset.lvs_dataset ]
}
#  Adding iam policy to manage permissions & access to dataset using editor service account 
resource "google_bigquery_dataset_iam_binding" "bq_permissions_editor" {
  dataset_id = google_bigquery_dataset.lvs_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  project = var.project_id
  members = [
    "serviceAccount:${google_service_account.sa_editor.email}",  # Grant access to the service account
    "user:aruldharani.kumar@samblagroup.com",
  ]
  depends_on = [ google_bigquery_dataset.lvs_dataset ]
   lifecycle {
      prevent_destroy = true
      ignore_changes = [
        role,  # Ignore changes to prevent recreation
        dataset_id,
        project,
        members,
      ]
    }

}

# Creating a service account to trigger and manage the data transfer scheduled query 
resource "google_service_account" "sa_data_transfer" {
  account_id   = "de-compliance-lvs-data-trans"
  display_name = "lvs-data-transfer"
  description = "A service account to create & trigger data transfer job for scheduled queries"
}
# Add BigQuery editor permissions to the service account so that scheduled query can run
resource "google_bigquery_dataset_iam_member" "bq_permissions_data_transfer_editor" {
  depends_on = [google_service_account.sa_data_transfer]
  dataset_id    = google_bigquery_dataset.lvs_dataset.dataset_id
  role               = "roles/bigquery.dataEditor" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa_data_transfer.email}"  # Grant access to the service account
  
}
# Add IAM permissions to the service account in order to run the scheduled query against bigquery
resource "google_project_iam_member" "bq_permissions_data_transfer_service_agent" {
  project    = var.project_id
  role               = "roles/bigquerydatatransfer.serviceAgent" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa_data_transfer.email}"  # Grant access to the service account
  
}
resource "google_project_iam_member" "bq_permissions_data_transfer_job_user" {
  project    = var.project_id
  role               = "roles/bigquery.jobUser" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa_data_transfer.email}"  # Grant access to the service account
  
}
resource "google_project_iam_member" "gcs_permissions_data_transfer_bject_viewer" {
  project    = var.project_id
  role               = "roles/storage.objectViewer" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa_data_transfer.email}"  # Grant access to the service account
  
}