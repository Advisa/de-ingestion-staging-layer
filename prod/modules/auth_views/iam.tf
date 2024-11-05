data "google_iam_policy" "viewer"{
  binding {
    role = "roles/bigquery.dataViewer"
    members = [ "serviceAccount:authorised-view-service-acc@data-domain-data-warehouse.iam.gserviceaccount.com" ]
  }
}

resource "google_project_iam_member" "project_permissions_fine_grained_reader" {
  project    = var.project_id
  role               = "roles/datacatalog.categoryFineGrainedReader" # Grant permission to use the service account
  member = "serviceAccount:authorised-view-service-acc@data-domain-data-warehouse.iam.gserviceaccount.com"  # Grant access to the service account
}
resource "google_bigquery_dataset_iam_member" "dataset_permissions_vault_reader" {
  dataset_id = "compilance_database"                          
  project    = "sambla-group-compliance-db"  
  role               = "roles/bigquery.dataViewer" # Grant permission to use the service account
  member = "serviceAccount:authorised-view-service-acc@data-domain-data-warehouse.iam.gserviceaccount.com"  # Grant access to the service account
}


# IAM Policy for Authorized Views
resource "google_bigquery_table_iam_policy" "auth_view_iam_policy" {
  for_each = local.schema_table_queries
  project    = var.project_id
  dataset_id = "authorized_view_test_${each.value.schema}" 
  table_id   = "view_${each.value.table}"                   
  policy_data = data.google_iam_policy.viewer.policy_data    
  depends_on = [google_bigquery_table.dynamic_auth_views] 
}

# Dataset Access for Authorized Views
resource "google_bigquery_dataset_access" "auth_view_access_dataset"{
  for_each = local.schema_table_queries
  project = var.project_id
  dataset_id = "${each.value.schema}"
  view{
    project_id = var.project_id
    dataset_id = "authorized_view_test_${each.value.schema}"
    table_id = "view_${each.value.table}" 
  }
}

# Dataset Access for GDPR Vault
resource "google_bigquery_dataset_access" "auth_view_access_to_gdpr_vault" {
  for_each = local.schema_table_queries

  dataset_id = "compilance_database"                          
  project    = "sambla-group-compliance-db"                    

  view {
    project_id = var.project_id
    dataset_id = "authorized_view_test_${each.value.schema}"   
    table_id   = "view_${each.value.table}"                   
  }
  depends_on = [google_bigquery_table.dynamic_auth_views] 
}