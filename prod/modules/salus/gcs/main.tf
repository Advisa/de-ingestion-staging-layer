resource "google_storage_bucket" "gcs_salus_bucket" {
  name          = "sambla-group-salus-integration-legacy"
  storage_class = "STANDARD"
  project       = var.project_id
  location      = var.region
  lifecycle {
    prevent_destroy = true
  }
  
}