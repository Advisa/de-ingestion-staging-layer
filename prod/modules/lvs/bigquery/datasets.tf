# Creating the dataset for lvs
resource "google_bigquery_dataset" "lvs_dataset" {
  dataset_id                  = "lvs_integration_legacy"
  description                 = "Integration legacy dataset for lvs"
  friendly_name               = "LVS Integration Legacy"
  location                    = var.region
  # default_encryption_configuration {
  #   kms_key_name = var.kms_crypto_key_id
  # }
  lifecycle {
    prevent_destroy = true
  }
  
}