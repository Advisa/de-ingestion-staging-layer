
# old dataset for auth views / must be deleted after fully go live!
resource "google_bigquery_dataset" "auth_view_dataset" {
  dataset_id                  = "authorized_views"
  description                 = "Dataset for authorized views"
  friendly_name               = "Authorized view dataset"
  location                    = var.region
  lifecycle {
    prevent_destroy = true
  }
}

# prod dataset fir auth views
resource "google_bigquery_dataset" "auth_view_dataset_prod" {
  dataset_id                  = "prod_authorized_views"
  description                 = "Dataset for prod authorized views"
  friendly_name               = "Authorized view dataset"
  location                    = var.region
  lifecycle {
    prevent_destroy = true
  }
  
}

# old production auth views without encryption / should be deleted after fully go live!
resource "google_bigquery_table" "dynamic_auth_views_non_encrypted" {
  for_each = local.unencrypted_schema_table_queries

  dataset_id         = google_bigquery_dataset.auth_view_dataset.dataset_id
  table_id           = "view_${each.value.table}"
  deletion_protection = true

  view {
    query           = each.value.query
    use_legacy_sql  = false
  }
  lifecycle {
    ignore_changes = [ table_id, view ]
  }
}


# main production auth views
resource "google_bigquery_table" "dynamic_auth_views_prod" {
  for_each = local.prod_schema_table_queries

  dataset_id         = google_bigquery_dataset.auth_view_dataset_prod.dataset_id
  table_id           = "view_${each.value.table}"
  deletion_protection = false

  view {
    query           = each.value.query
    use_legacy_sql  = false
  }
  #lifecycle {
  #  ignore_changes = [ table_id, view ]
  #}
}

# main production auth views fir cdc
resource "google_bigquery_table" "dynamic_auth_views_cdc_prod" {
  for_each = local.cdc_schema_table_queries_prod

  dataset_id         = google_bigquery_dataset.auth_view_dataset_prod.dataset_id
  table_id           = "view_${each.value.table_id}"
  deletion_protection = false

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
  deletion_protection = true

  view {
    query           = each.value.query
    use_legacy_sql  = false
  }


}

# Create salus auth views in testing dataset
resource "google_bigquery_table" "salus_auth_views_test" {
  for_each = local.salus_schema_table_queries

  dataset_id         = "auth_view_testing"
  table_id           = "view_${each.value.table}"
  deletion_protection = true

  view {
    query           = each.value.query
    use_legacy_sql  = false
  }

}

# Create sambla_legacy auth views in testing dataset
resource "google_bigquery_table" "sambla_legacy_auth_views_test" {
  for_each = local.sambla_legacy_schema_table_queries

  dataset_id         = "auth_view_testing"
  table_id           = "view_${each.value.table}"
  deletion_protection = true

  view {
    query           = each.value.query
    use_legacy_sql  = false
  }

}

# Create advisa_history auth views in testing dataset
resource "google_bigquery_table" "advisa_history_auth_views_test" {
  for_each = local.advisa_history_schema_table_queries

  dataset_id         = "auth_view_testing"
  table_id           = "view_${each.value.table}"
  deletion_protection = true

  view {
    query           = each.value.query
    use_legacy_sql  = false
  }

}

# Create maxwell auth views in testing dataset
resource "google_bigquery_table" "maxwell_auth_views_test" {
  for_each = local.maxwell_schema_table_queries

  dataset_id         = "auth_view_testing"
  table_id           = "view_${each.value.table}"
  deletion_protection = true

  view {
    query           = each.value.query
    use_legacy_sql  = false
  }

}

# Create rahalaitos auth views in testing dataset
resource "google_bigquery_table" "rahalaitos_auth_views_test" {
  for_each = local.rahalaitos_schema_table_queries

  dataset_id         = "auth_view_testing"
  table_id           = "view_${each.value.table}"
  deletion_protection = true

  view {
    query           = each.value.query
    use_legacy_sql  = false
  }

}

# Create cdc auth views in testing dataset
resource "google_bigquery_table" "cdc_auth_views_test" {
  for_each = local.cdc_schema_table_queries

  dataset_id         = "auth_view_testing"
  table_id           = "view_${each.value.table_id}"
  deletion_protection = true

  view {
    query           = each.value.query
    use_legacy_sql  = false
  }

}