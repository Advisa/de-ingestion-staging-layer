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