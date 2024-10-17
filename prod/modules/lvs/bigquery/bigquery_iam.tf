
# Creating a service account for external applications to manage read-only access
resource "google_service_account" "sa-reader" {
  account_id   = "de-compliance-lvs-reader"
  display_name = "A service account that only approved users can use to access lvs raw data"
}
# Creating a service account for external applications to manage developer access
resource "google_service_account" "sa-editor" {
  account_id   = "de-compliance-lvs-editor"
  display_name = "A service account that only approved users can use to access & edit lvs raw data"
}
# Add individual users to IAM roles on read-only service accounts
resource "google_service_account_iam_binding" "bq-reader-account-sa-iam" {
  service_account_id = google_service_account.sa-reader.name
  role               = "roles/iam.serviceAccountUser"  # Grant permission to use the service account

  members = [
    "user:adam.svenson@samblagroup.com",
    "user:aruldharani.kumar@samblagroup.com",
    "user:duygu.genc@samblagroup.com",
  ]
}
# Add individual users to IAM roles on developer service accounts
resource "google_service_account_iam_binding" "bq-editor-account-sa-iam" {
  service_account_id = google_service_account.sa-editor.name
  role               = "roles/iam.serviceAccountUser"  # Grant permission to use the service account

  members = [
    "user:adam.svenson@samblagroup.com",
    "user:aruldharani.kumar@samblagroup.com",
    "user:duygu.genc@samblagroup.com",
  ]
}

# Adding iam policy to manage permissions & access to dataset using reader service account 
resource "google_bigquery_dataset_iam_binding" "bq-reader-iam" {
  dataset_id = google_bigquery_dataset.lvs_dataset.dataset_id
  role       = "roles/bigquery.dataViewer"

  members = [
    "serviceAccount:${google_service_account.sa-reader.email}",
    "user:aruldharani.kumar@samblagroup.com",
  ]
  depends_on = [ google_bigquery_dataset.lvs_dataset ]
}
#  Adding iam policy to manage permissions & access to dataset using editor service account 
resource "google_bigquery_dataset_iam_binding" "bq-editor-iam" {
  dataset_id = google_bigquery_dataset.lvs_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  project = var.project_id
  members = [
    "serviceAccount:${google_service_account.sa-editor.email}",  # Grant access to the service account
    "user:aruldharani.kumar@samblagroup.com",
  ]
  depends_on = [ google_bigquery_dataset.lvs_dataset ]
   lifecycle {
      prevent_destroy = true
      ignore_changes = [
        role,  # Ignore changes to prevent recreation
        dataset_id,
        project,
        members,
      ]
    }

}
data "google_project" "project" {
}
# Creating a service account to trigger and manage the data transfer scheduled query 
resource "google_service_account" "sa-data-transfer" {
  account_id   = "de-compliance-lvs-data-trans"
  display_name = "A service account to create & trigger data transfer job for scheduled queries"
}
# Add BigQuery editor permissions to the service account so that scheduled query can run
resource "google_bigquery_dataset_iam_member" "data-transfer-scheduled-query-sa-iam" {
  depends_on = [google_service_account.sa-data-transfer]
  dataset_id    = google_bigquery_dataset.lvs_dataset.dataset_id
  role               = "roles/bigquery.dataEditor" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa-data-transfer.email}"  # Grant access to the service account
  
}
# Add BigQuery editor permissions to the service account so that scheduled query can run
resource "google_bigquery_dataset_iam_member" "data-transfer-scheduled-query-sa-editor-iam" {
  depends_on = [google_service_account.sa-data-transfer]
  dataset_id    = google_bigquery_dataset.lvs_dataset.dataset_id
  role               = "roles/bigquery.admin" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa-data-transfer.email}"  # Grant access to the service account
  lifecycle {
      prevent_destroy = true
      ignore_changes = [
        role,  # Ignore changes to prevent recreation
        dataset_id,
        member,

      ]
    }
}

# Add IAM permissions to the service account in order to run the scheduled query against bigquery
resource "google_project_iam_member" "project-permissions-data-transfer-agent-iam" {
  project    = var.project_id
  role               = "roles/bigquerydatatransfer.serviceAgent" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa-data-transfer.email}"  # Grant access to the service account
  
}
resource "google_project_iam_member" "project-permissions-bigquery-job-user-iam" {
  project    = var.project_id
  role               = "roles/bigquery.jobUser" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa-data-transfer.email}"  # Grant access to the service account
  
}
resource "google_project_iam_member" "project-permissions-storage-object-viewer-iam" {
  project    = var.project_id
  role               = "roles/storage.objectViewer" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa-data-transfer.email}"  # Grant access to the service account
  
}


