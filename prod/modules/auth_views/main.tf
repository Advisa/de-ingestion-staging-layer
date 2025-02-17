
# Creating the dataset for auth views
resource "google_bigquery_dataset" "auth_view_dataset" {
  dataset_id                  = "authorized_views"
  description                 = "Dataset for authorized views"
  friendly_name               = "Authorized view dataset"
  location                    = var.region
  lifecycle {
    prevent_destroy = true
  }
  
}

# Create non-encrypted auth views (only if view_type is "non_encrypted")
resource "google_bigquery_table" "dynamic_auth_views_non_encrypted" {
  for_each = local.unencrypted_schema_table_queries

  dataset_id         = google_bigquery_dataset.auth_view_dataset.dataset_id
  table_id           = "view_${each.value.table}"
  deletion_protection = true

  view {
    query           = each.value.query
    use_legacy_sql  = false
  }
}

# Create lvs auth views in testing dataset
resource "google_bigquery_table" "lvs_auth_views_test" {
  for_each = local.lvs_schema_table_queries

  dataset_id         = "auth_view_testing"
  table_id           = "view_${each.value.table}"
  deletion_protection = false

  view {
    query           = each.value.query
    use_legacy_sql  = false
  }

}