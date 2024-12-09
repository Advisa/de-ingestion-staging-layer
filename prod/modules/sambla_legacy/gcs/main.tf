resource "google_storage_bucket" "gcs_sambla_legacy_bucket" {
  name          = "sambla-group-sambla-legacy-integration-legacy"
  storage_class = "STANDARD"
  project       = var.project_id
  location      = var.region
  lifecycle {
    prevent_destroy = true
  }  
}