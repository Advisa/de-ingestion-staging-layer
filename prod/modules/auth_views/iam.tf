
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
  depends_on = [google_bigquery_table.dynamic_auth_views] 
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

  depends_on = [google_bigquery_table.dynamic_auth_views] 
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

  depends_on = [google_bigquery_table.dynamic_auth_views,google_bigquery_dataset.auth_view_dataset] 
}