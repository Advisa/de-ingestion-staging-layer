
locals {
  service_account = "dbt-cloud-job-runner@data-domain-data-warehouse.iam.gserviceaccount.com"
}


# IAM Policy for service account to read Authorized Views from the authorized_views dataset
resource "google_bigquery_dataset_iam_binding" "auth_view_iam_access" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.auth_view_dataset.dataset_id 
  role = "roles/bigquery.dataViewer"
  members =  [
    "serviceAccount:${local.service_account}",
    "group:data_de@samblagroup.com",
    "group:data@samblagroup.com",
    "serviceAccount:data-flow-pipeline@data-domain-data-warehouse.iam.gserviceaccount.com"
  ]                 
  depends_on = [google_bigquery_table.dynamic_auth_views_prod] 
}

# Dataset Access for GDPR Vault
resource "google_bigquery_dataset_access" "auth_dataset_access_to_gdpr_vault" {
  dataset_id = "compilance_database"                          
  project    = "sambla-group-compliance-db"                    
  dataset {
    dataset{
      project_id = var.project_id
      dataset_id = google_bigquery_dataset.auth_view_dataset.dataset_id 
    }
    target_types = ["VIEWS"]
  }

  depends_on = [google_bigquery_table.dynamic_auth_views_prod] 
}

# Dataset Access for legacy stack
resource "google_bigquery_dataset_access" "auth_view_access_legacy_dataset" {
  for_each = { for schema in local.unique_schemas : schema => schema }

  dataset_id = "${each.key}"                          
  project    = var.project_id                   
  dataset {
    dataset{
      project_id = var.project_id
      dataset_id = google_bigquery_dataset.auth_view_dataset.dataset_id  
    }
    target_types = ["VIEWS"]
  }

  depends_on = [google_bigquery_table.dynamic_auth_views_prod,google_bigquery_dataset.auth_view_dataset] 
}


# IAM Policy for service account to read Authorized Views from the auth_view_testing dataset
resource "google_bigquery_dataset_iam_binding" "auth_view_iam_access_test" {
  project    = var.project_id
  dataset_id = var.auth_view_test_dataset_id
  role = "roles/bigquery.dataViewer"
  members =  [
    "serviceAccount:${local.service_account}",
    "group:data_de@samblagroup.com",
    "group:data@samblagroup.com",
    "serviceAccount:data-flow-pipeline@data-domain-data-warehouse.iam.gserviceaccount.com"
  ]                 
  depends_on = [
    google_bigquery_table.lvs_auth_views_test,
    google_bigquery_table.maxwell_auth_views_test,
    google_bigquery_table.rahalaitos_auth_views_test,
    google_bigquery_table.advisa_history_auth_views_test,
    google_bigquery_table.salus_auth_views_test,
    google_bigquery_table.sambla_legacy_auth_views_test
  ] 
}

# Dataset Access for GDPR Vault
resource "google_bigquery_dataset_access" "auth_dataset_access_to_gdpr_vault_test" {
  dataset_id = "compilance_database"                          
  project    = "sambla-group-compliance-db"                    
  dataset {
    dataset{
      project_id = var.project_id
      dataset_id = var.auth_view_test_dataset_id
    }
    target_types = ["VIEWS"]
  }

  depends_on = [
    google_bigquery_table.lvs_auth_views_test,
    google_bigquery_table.maxwell_auth_views_test,
    google_bigquery_table.rahalaitos_auth_views_test,
    google_bigquery_table.advisa_history_auth_views_test,
    google_bigquery_table.salus_auth_views_test,
    google_bigquery_table.sambla_legacy_auth_views_test
  ] 
}

# Dataset Access for legacy stack
resource "google_bigquery_dataset_access" "auth_view_access_legacy_dataset_test" {
  for_each = { for schema in local.unique_schemas : schema => schema }

  dataset_id = "${each.key}"                          
  project    = var.project_id                   
  dataset {
    dataset{
      project_id = var.project_id
      dataset_id = var.auth_view_test_dataset_id
    }
    target_types = ["VIEWS"]
  }

  depends_on = [
    google_bigquery_table.lvs_auth_views_test,
    google_bigquery_table.maxwell_auth_views_test,
    google_bigquery_table.rahalaitos_auth_views_test,
    google_bigquery_table.advisa_history_auth_views_test,
    google_bigquery_table.salus_auth_views_test,
    google_bigquery_table.sambla_legacy_auth_views_test
  ] 
}