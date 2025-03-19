
locals {
  service_account_dbt = "dbt-cloud-job-runner@data-domain-data-warehouse.iam.gserviceaccount.com"
  service_account_prefect = "data-flow-pipeline@data-domain-data-warehouse.iam.gserviceaccount.com"
}


# IAM Policy for service account to read Authorized Views from the authorized_views dataset
resource "google_bigquery_dataset_iam_binding" "auth_view_iam_access_prod" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.auth_view_dataset_prod.dataset_id 
  role = "roles/bigquery.dataViewer"
  members =  [
    "serviceAccount:${local.service_account_dbt}",
    "group:data_de@samblagroup.com",
    "group:data@samblagroup.com",
    "serviceAccount:${local.service_account_prefect}"
  ]                 
  depends_on = [google_bigquery_table.dynamic_auth_views_prod] 
}

# Dataset Access for GDPR Vault
resource "google_bigquery_dataset_access" "auth_dataset_access_to_gdpr_vault_prod" {
  dataset_id = var.complaince_db_dataset_id                        
  project    = var.complaince_db_project_id               
  dataset {
    dataset{
      project_id = var.project_id
      dataset_id = google_bigquery_dataset.auth_view_dataset_prod.dataset_id 
    }
    target_types = ["VIEWS"]
  }

  depends_on = [google_bigquery_table.dynamic_auth_views_prod] 
}

# Dataset Access for legacy stack except for salus
# Salus auth views are based on data-domain-data-warehouse.salus_group_integration incremental r tables
# Therefore, we have to define a diff project_id for those
resource "google_bigquery_dataset_access" "auth_view_access_legacy_dataset_prod" {
  for_each = { for schema in local.unique_schemas_prod : schema => schema }

  dataset_id = "${each.key}"                          
  project = each.key == "salus_group_integration" ? var.data-warehouse-project_id : var.project_id                  
  dataset {
    dataset{
      project_id = var.project_id
      dataset_id = google_bigquery_dataset.auth_view_dataset_prod.dataset_id  
    }
    target_types = ["VIEWS"]
  }

  depends_on = [google_bigquery_table.dynamic_auth_views_prod, google_bigquery_dataset.auth_view_dataset_prod] 
}

# Dataset Access for cdc
resource "google_bigquery_dataset_access" "auth_view_access_cdc_dataset_prod" {
  for_each = { for schema in local.unique_schemas_cdc_prod : schema => schema }

  dataset_id = "${each.key}"                          
  project    = var.data-warehouse-project_id                   
  dataset {
    dataset{
      project_id = var.project_id
      dataset_id = google_bigquery_dataset.auth_view_dataset_prod.dataset_id  
    }
    target_types = ["VIEWS"]
  }

  depends_on = [google_bigquery_table.dynamic_auth_views_cdc_prod, google_bigquery_dataset.auth_view_dataset_prod] 
}