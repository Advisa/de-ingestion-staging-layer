resource "google_storage_bucket" "gcs_advisa_history_bucket" {
  name          = "sambla-group-advisa-history-integration-legacy"
  storage_class = "STANDARD"
  project       = var.project_id
  location      = var.region
  lifecycle {
    prevent_destroy = true
  }
  
}