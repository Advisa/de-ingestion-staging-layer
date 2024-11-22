# Creating a service account for external applications to manage read-only access
resource "google_service_account" "sa_reader" {
  account_id   = "de-compliance-salus-reader"
  display_name = "salus-reader"
  description = "A service account that only approved users can use to access salus raw data"
}
# Creating a service account for external applications to manage developer access
resource "google_service_account" "sa_editor" {
  account_id   = "de-compliance-salus-editor"
  display_name = "salus-editor"
  description = "A service account that only approved users can use to access & edit salus raw data"
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
  dataset_id = google_bigquery_dataset.salus_dataset.dataset_id
  role       = "roles/bigquery.dataViewer"

  members = [
    "serviceAccount:${google_service_account.sa_reader.email}",
  ]
  depends_on = [ google_bigquery_dataset.salus_dataset ]
}
#  Adding iam policy to manage permissions & access to dataset using editor service account 
resource "google_bigquery_dataset_iam_binding" "bq_permissions_editor" {
  dataset_id = google_bigquery_dataset.salus_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  project = var.project_id
  members = [
    "serviceAccount:${google_service_account.sa_editor.email}"
  ]
  depends_on = [ google_bigquery_dataset.salus_dataset ]

}