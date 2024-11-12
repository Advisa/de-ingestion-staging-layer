locals {
  service_account = "authorised-view-service-acc@data-domain-data-warehouse.iam.gserviceaccount.com"
}

# IAM Policy for Project Permissions (BigQuery Job User)
resource "google_project_iam_member" "project_permissions_bq_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${local.service_account}"
}

# IAM Policy for service account to read Authorized Views (Encrypted)
resource "google_bigquery_table_iam_member" "auth_view_iam_paccess_encrypted" {
  for_each = var.view_type == "encrypted" ? local.schema_table_queries : {}

  project    = var.project_id
  dataset_id = "authorized_view_${each.value.schema}"
  table_id   = "view_${each.value.table}"
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${local.service_account}"

  depends_on = [google_bigquery_table.dynamic_auth_views]
}

# IAM Policy for service account to read Authorized Views (Non-Encrypted)
resource "google_bigquery_table_iam_member" "auth_view_iam_paccess_non_encrypted" {
  for_each = var.view_type == "non_encrypted" ? local.schema_table_queries : {}

  project    = var.project_id
  dataset_id = "non_encrypted_authorized_view_${each.value.schema}"
  table_id   = "view_${each.value.table}"
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${local.service_account}"

  depends_on = [google_bigquery_table.dynamic_auth_views_non_encrypted]
}

# Dataset Access for GDPR Vault (Encrypted)
resource "google_bigquery_dataset_access" "auth_dataset_access_to_gdpr_vault_encrypted" {
  for_each = var.view_type == "encrypted" ? { for schema in local.unique_schemas : schema => schema } : {}

  dataset_id = "compilance_database"
  project    = "sambla-group-compliance-db"
  dataset {
    dataset {
      project_id = var.project_id
      dataset_id = "authorized_view_${each.key}"
    }
    target_types = ["VIEWS"]
  }

  depends_on = [google_bigquery_table.dynamic_auth_views]
}

# Dataset Access for GDPR Vault (Non-Encrypted)
resource "google_bigquery_dataset_access" "auth_dataset_access_to_gdpr_vault_non_encrypted" {
  for_each = var.view_type == "non_encrypted" ? { for schema in local.unique_schemas : schema => schema } : {}

  dataset_id = "compilance_database"
  project    = "sambla-group-compliance-db"
  dataset {
    dataset {
      project_id = var.project_id
      dataset_id = "non_encrypted_authorized_view_${each.key}"
    }
    target_types = ["VIEWS"]
  }

  depends_on = [google_bigquery_table.dynamic_auth_views_non_encrypted]
}

# Dataset Access for Legacy Stack (Encrypted)
resource "google_bigquery_dataset_access" "auth_view_access_legacy_dataset_encrypted" {
  for_each = var.view_type == "encrypted" ? { for schema in local.unique_schemas : schema => schema } : {}

  dataset_id = "${each.key}"
  project    = var.project_id
  dataset {
    dataset {
      project_id = var.project_id
      dataset_id = "authorized_view_${each.key}"
    }
    target_types = ["VIEWS"]
  }

  depends_on = [google_bigquery_table.dynamic_auth_views]
}

# Dataset Access for Legacy Stack (Non-Encrypted)
resource "google_bigquery_dataset_access" "auth_view_access_legacy_dataset_non_encrypted" {
  for_each = var.view_type == "non_encrypted" ? { for schema in local.unique_schemas : schema => schema } : {}

  dataset_id = "${each.key}"
  project    = var.project_id
  dataset {
    dataset {
      project_id = var.project_id
      dataset_id = "non_encrypted_authorized_view_${each.key}"
    }
    target_types = ["VIEWS"]
  }

  depends_on = [google_bigquery_table.dynamic_auth_views_non_encrypted]
}
