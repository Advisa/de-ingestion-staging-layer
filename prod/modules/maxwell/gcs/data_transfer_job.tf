# Data transfer job to replicate the maxwell data to new project bucket
resource "google_storage_transfer_job" "replicate-from-old-to-new-bucket-maxwell" {
  description = "Data Transfer job to replicate source maxwell data from domain-data-warehouse project to new staging-compliance project"
  project     = var.project_id
  

  transfer_spec {
    transfer_options {
      overwrite_when = "DIFFERENT"
    }
    gcs_data_source {
      bucket_name = var.maxwell_bucket_name 
    }
    gcs_data_sink {
      bucket_name = google_storage_bucket.gcs_maxwell_bucket.name
    }
  }
}