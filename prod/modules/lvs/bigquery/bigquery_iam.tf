
# Creating a service account for external applications to manage read-only access
resource "google_service_account" "sa_reader" {
  account_id   = "de-compliance-lvs-reader"
  display_name = "lvs-reader"
  description = "A service account that only approved users can use to access lvs raw data"
}
# Creating a service account for external applications to manage developer access
resource "google_service_account" "sa_editor" {
  account_id   = "de-compliance-lvs-editor"
  display_name = "lvs-editor"
  description = "A service account that only approved users can use to access & edit lvs raw data"
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


