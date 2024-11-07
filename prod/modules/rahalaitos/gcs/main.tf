resource "google_storage_bucket" "gcs_rahalaitos_bucket" {
  name          = "sambla-group-rahalaitos-integration-legacy"
  storage_class = "STANDARD"
  project       = var.project_id
  location      = var.region
  lifecycle {
    prevent_destroy = true
  }
  
}