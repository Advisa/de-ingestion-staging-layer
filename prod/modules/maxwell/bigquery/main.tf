# Creating the dataset for maxwell
resource "google_bigquery_dataset" "maxwell_dataset" {
  dataset_id                  = "maxwell_integration_legacy"
  description                 = "Integration legacy dataset for maxwell stack"
  friendly_name               = "maxwell Integration Legacy"
  location                    = var.region
  lifecycle {
    prevent_destroy = true
  }
}