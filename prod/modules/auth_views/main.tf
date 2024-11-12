# Create dataset for encrypted auth views (only if view_type is "encrypted")
resource "google_bigquery_dataset" "auth_view_dataset" {
  for_each = var.view_type == "encrypted" ? { for schema in local.unique_schemas : schema => schema } : {}

  dataset_id    = "authorized_view_${each.key}"
  description   = "Dataset for ${each.key} authorized views"
  friendly_name = "Authorized ${each.key} dataset"
  location      = var.region

  lifecycle {
    prevent_destroy = false
  }
}

# Create encrypted auth views (only if view_type is "encrypted")
resource "google_bigquery_table" "dynamic_auth_views" {
  for_each = var.view_type == "encrypted" ? local.schema_table_queries : {}

  dataset_id         = google_bigquery_dataset.auth_view_dataset[each.value.schema].dataset_id
  table_id           = "view_${each.value.table}"
  deletion_protection = true

  view {
    query           = each.value.query
    use_legacy_sql  = false
  }
}

# Create dataset for non-encrypted auth views (only if view_type is "non_encrypted")
resource "google_bigquery_dataset" "auth_view_non_encrypted_dataset" {
  for_each = var.view_type == "non_encrypted" ? { for schema in local.unique_schemas : schema => schema } : {}

  dataset_id    = "non_encrypted_authorized_view_${each.key}"
  description   = "Non-encrypted dataset for ${each.key} authorized views"
  friendly_name = "Non-Encrypted Authorized ${each.key} dataset"
  location      = var.region

  lifecycle {
    prevent_destroy = false
  }
}

# Create non-encrypted auth views (only if view_type is "non_encrypted")
resource "google_bigquery_table" "dynamic_auth_views_non_encrypted" {
  for_each = var.view_type == "non_encrypted" ? local.schema_table_queries : {}

  dataset_id         = google_bigquery_dataset.auth_view_non_encrypted_dataset[each.value.schema].dataset_id
  table_id           = "view_${each.value.table}"
  deletion_protection = true

  view {
    query           = each.value.query
    use_legacy_sql  = false
  }
}
