# Create a default biglake connection 
resource "google_bigquery_connection" "default" {
  connection_id = "biglake-conn"
  location      = var.region
  cloud_resource {
  }
}
# Grant the previous connection with storage object viewer role to access the buckets (for external data).
resource "google_project_iam_member" "default" {
  role    = "roles/storage.objectViewer"
  project = var.project_id
  member  = "serviceAccount:${google_bigquery_connection.default.cloud_resource[0].service_account_id}"
}