
# Creating the dataset for auth views
resource "google_bigquery_dataset" "auth_view_dataset" {
  dataset_id                  = "authorized_views"
  description                 = "Dataset for authorized views"
  friendly_name               = "Authorized view dataset"
  location                    = var.region
  # default_encryption_configuration {
  #   kms_key_name = var.kms_crypto_key_id
  # }
  lifecycle {
    prevent_destroy = true
  }
  
}
# Create encrypted auth views
resource "google_bigquery_table" "dynamic_auth_views" {
  for_each = local.schema_table_queries

  dataset_id         = google_bigquery_dataset.auth_view_dataset.dataset_id
  table_id           = "view_${each.value.table}"
  deletion_protection = true

  view {
    # Replace schema and table placeholders for each entry
      query = each.value.query
      # Use standart query instead
      use_legacy_sql  = false
  }
  
}