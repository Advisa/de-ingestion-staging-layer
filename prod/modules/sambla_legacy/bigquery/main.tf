# Creating the dataset for lvs
resource "google_bigquery_dataset" "sambla_legacy_dataset" {
  dataset_id                  = "sambla_legacy_integration_legacy"
  description                 = "Integration legacy dataset for sambla legacy stack"
  friendly_name               = "sambla legacy Integration Legacy"
  location                    = var.region
  lifecycle {
    prevent_destroy = true
  }
}