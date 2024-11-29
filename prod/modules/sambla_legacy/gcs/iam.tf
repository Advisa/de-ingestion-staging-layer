data "google_storage_transfer_project_service_account" "default" {
    project = var.project_id

}

# Grant bucket writer permissions for the data transfer service account to access the sink bucket
resource "google_storage_bucket_iam_member" "gcs_permissions_transfer_service_sink_bucket_writer" {
  bucket = google_storage_bucket.gcs_sambla_legacy_bucket.name
  role   = "roles/storage.legacyBucketWriter"  
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}
# Grant bucket reader permissions for the data transfer service account to access the sink bucket
resource "google_storage_bucket_iam_member" "gcs_permissions_transfer_service_sink_bucket_reader" {
  bucket = google_storage_bucket.gcs_sambla_legacy_bucket.name
  role   = "roles/storage.legacyBucketReader"  
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}
# Grant bucket reader permissions for the data transfer service account to access the source bucket
resource "google_storage_bucket_iam_member" "gcs_permissions_transfer_service_source_bucket_reader" {
  bucket = var.sambla_legacy_bucket_name
  role   = "roles/storage.legacyBucketReader"  
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}

# Grant object reader permissions for the data transfer service account to access the source bucket
resource "google_storage_bucket_iam_member" "gcs_permissions_transfer_service_source_object_reader" {
  bucket = var.sambla_legacy_bucket_name
  role   = "roles/storage.legacyObjectReader"  
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}

/*
# Grant bucket reader permissions for the data transfer service account to access the source bucket
resource "google_storage_bucket_iam_member" "gcs_permissions_transfer_service_source_bucket_owner" {
  bucket = var.sambla_legacy_bucket_name
  role   = "roles/storage.legacyBucketOwner"
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}

# Grant bucket reader permissions for the data transfer service account to access the sink bucket
resource "google_storage_bucket_iam_member" "source-bucket-admin" {
  bucket = google_storage_bucket.gcs_sambla_legacy_bucket.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}
*/


