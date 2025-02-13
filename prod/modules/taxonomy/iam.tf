# Dataset Access for SA's to read tables in sambla-data-staging-compliance.policy_tags_metadata
resource "google_bigquery_dataset_iam_binding" "storage_object_viewer" {
  dataset_id = var.policy_dataset_id
  role       = "roles/bigquery.dataViewer"
  members = [
    "serviceAccount:data-flow-pipeline@data-domain-data-warehouse.iam.gserviceaccount.com",
    "serviceAccount:dbt-cloud-job-runner@data-domain-data-warehouse.iam.gserviceaccount.com"
  ]
}


resource "google_storage_bucket_iam_binding" "binding" {
  bucket = google_storage_bucket.taxonomy_bucket.name
  role =  "roles/storage.objectViewer"
  members = [
    "serviceAccount:data-flow-pipeline@data-domain-data-warehouse.iam.gserviceaccount.com",
    "serviceAccount:dbt-cloud-job-runner@data-domain-data-warehouse.iam.gserviceaccount.com"
  ]
}

 