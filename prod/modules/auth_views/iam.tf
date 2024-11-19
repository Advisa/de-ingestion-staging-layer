
locals {
  service_account = "authorised-view-service-acc@data-domain-data-warehouse.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "project_permissions_bq_user" {
  project    = var.project_id
  role               = "roles/bigquery.jobUser" 
  member =  "serviceAccount:${local.service_account}" 
}

# IAM Policy for service account to read Authorized Views from the authorized_views dataset
resource "google_bigquery_dataset_iam_member" "auth_view_iam_access" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.auth_view_dataset.dataset_id 
  role = "roles/bigquery.dataViewer"
  member =  "serviceAccount:${local.service_account}"                  
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