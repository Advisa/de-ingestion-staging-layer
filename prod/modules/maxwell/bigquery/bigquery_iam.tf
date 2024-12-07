# Creating a service account for external applications to manage read-only access
resource "google_service_account" "sa_reader" {
  account_id   = "de-compliance-maxwell-reader"
  display_name = "maxwell-reader"
  description = "A service account that only approved users can use to access maxwell raw data"
}
# Creating a service account for external applications to manage developer access
resource "google_service_account" "sa_editor" {
  account_id   = "de-compliance-maxwell-editor"
  display_name = "maxwell-editor"
  description = "A service account that only approved users can use to access & edit maxwell raw data"
}

# Attach IAM roles for read-only access
resource "google_project_iam_member" "sa_reader_storage" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.sa_reader.email}"
}

# Add individual users to IAM roles on read-only service accounts
resource "google_service_account_iam_binding" "sa_permissions_reader_account_user" {
  service_account_id = google_service_account.sa_reader.name
  role               = "roles/iam.serviceAccountUser"  # Grant permission to use the service account

  members = [
    "group:data_de@samblagroup.com"
  ]
}
# Add individual users to IAM roles on developer service accounts
resource "google_service_account_iam_binding" "sa_permissions_editor_account_user" {
  service_account_id = google_service_account.sa_editor.name
  role               = "roles/iam.serviceAccountUser" 

  members = [
    "group:data_de@samblagroup.com"

  ]
}


/*# Creating a service account to trigger and manage the data transfer scheduled query
resource "google_service_account" "sa_data_transfer" {
  account_id   = "de-compliance-maxwell-data-trans"
  display_name = "maxwell-data-transfer"
  description  = "A service account to create & trigger data transfer job for scheduled queries"
}

# Grant BigQuery editor permissions to the service account for the destination dataset
resource "google_bigquery_dataset_iam_member" "destination_permissions" {
  depends_on = [google_service_account.sa_data_transfer,google_bigquery_dataset.maxwell_dataset]
  dataset_id = "maxwell_integration_legacy"  
  project = var.project_id
  role       = "roles/bigquery.dataEditor" 
  member     = "serviceAccount:${google_service_account.sa_data_transfer.email}"
}

# Grant BigQuery reader permissions to the service account for the source dataset
resource "google_bigquery_dataset_iam_member" "source_permissions" {
  depends_on = [google_service_account.sa_data_transfer]
  dataset_id = "sambla_internal_streaming" 
  project = var.data_domain_project_id
  role       = "roles/bigquery.dataViewer" 
  member     = "serviceAccount:${google_service_account.sa_data_transfer.email}"
}

resource "google_project_iam_member" "bq_permissions_data_transfer_service_agent_destination" {
  project = var.project_id # The destination project
  role    = "roles/bigquerydatatransfer.serviceAgent"
  member  = "serviceAccount:${google_service_account.sa_data_transfer.email}"
}

# Add IAM permissions to run BigQuery jobs
resource "google_project_iam_member" "bq_permissions_data_transfer_job_user_destination" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.sa_data_transfer.email}" 
  lifecycle {
    ignore_changes = [role]
  }
}*/


