data "google_storage_transfer_project_service_account" "default" {
    project = var.project_id

}

# Grant bucket writer permissions for the data transfer service account to access the sink bucket
resource "google_storage_bucket_iam_member" "transfer_service_permissions_sink_writer" {
  bucket = google_storage_bucket.gcs_rahalaitos_bucket.name
  role   = "roles/storage.legacyBucketWriter"  
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}
# Grant bucket reader permissions for the data transfer service account to access the sink bucket
resource "google_storage_bucket_iam_member" "transfer_service_permissions_sink_reader" {
  bucket = google_storage_bucket.gcs_rahalaitos_bucket.name
  role   = "roles/storage.legacyBucketReader"  
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}
# Grant bucket reader permissions for the data transfer service account to access the source bucket
resource "google_storage_bucket_iam_member" "transfer_service_permissions_source_reader" {
  bucket = var.rahalaitos_bucket_name
  role   = "roles/storage.legacyBucketReader"  
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}

# Grant object reader permissions for the data transfer service account to access the source bucket
resource "google_storage_bucket_iam_member" "transfer_service_permissions_source_object_reader" {
  bucket = var.rahalaitos_bucket_name
  role   = "roles/storage.legacyObjectReader"  
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}
