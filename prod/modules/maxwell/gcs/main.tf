resource "google_storage_bucket" "gcs_maxwell_bucket" {
  name          = "sambla-group-maxwell-integration-legacy"
  storage_class = "STANDARD"
  project       = var.project_id
  location      = var.region
  lifecycle {
    prevent_destroy = true
  }  
}