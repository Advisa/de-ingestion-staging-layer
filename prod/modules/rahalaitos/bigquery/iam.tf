# Creating a service account for external applications to manage read-only access
resource "google_service_account" "sa_reader" {
  account_id   = "de-compliance-raha-reader"
  display_name = "raha-reader"
  description = "A service account that only approved users can use to access rahalaitos raw data"
}
# Creating a service account for external applications to manage developer access
resource "google_service_account" "sa_editor" {
  account_id   = "de-compliance-raha-editor"
  display_name = "raha-editor"
  description = "A service account that only approved users can use to access & edit rahalaitos raw data"
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
  role               = "roles/iam.serviceAccountUser"  # Grant permission to use the service account

  members = [
    "group:data_de@samblagroup.com"
  ]
}

# Adding iam policy to manage permissions & access to dataset using reader service account 
resource "google_bigquery_dataset_iam_binding" "bq_permissions_reader" {
  dataset_id = google_bigquery_dataset.rahalaitos_dataset.dataset_id
  role       = "roles/bigquery.dataViewer"

  members = [
    "serviceAccount:${google_service_account.sa_reader.email}"
  ]
  depends_on = [ google_bigquery_dataset.rahalaitos_dataset ]
}
#  Adding iam policy to manage permissions & access to dataset using editor service account 
resource "google_bigquery_dataset_iam_binding" "bq_permissions_editor" {
  dataset_id = google_bigquery_dataset.rahalaitos_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  project = var.project_id
  members = [
    "serviceAccount:${google_service_account.sa_editor.email}"
  ]
  depends_on = [ google_bigquery_dataset.rahalaitos_dataset ]

}