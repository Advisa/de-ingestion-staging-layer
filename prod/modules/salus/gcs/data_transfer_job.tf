# Data transfer job to replicate the rahalaitos data to new project bucket
resource "google_storage_transfer_job" "replicate-from-old-to-new-bucket" {
  description = "Data Transfer job to replicate source salus data from domain-data-warehouse project to new staging-compliance project"
  project     = var.project_id
  

  transfer_spec {
    transfer_options {
      overwrite_when = "DIFFERENT"
      delete_objects_unique_in_sink = true
    }
     object_conditions {
      exclude_prefixes = [
        "applicant-bank-transactions/clicks-url-shortener/",
      ]
    }
    aws_s3_data_source {
      bucket_name = var.salus_bucket_name
      aws_access_key {
        access_key_id     = var.aws_access_key
        secret_access_key = var.aws_secret_key
      }
    }
    gcs_data_sink {
      bucket_name = google_storage_bucket.gcs_salus_bucket.name
    }

  }

  schedule {
    schedule_start_date {
      year  = 2024
      month = 11
      day   = 20
    }
    start_time_of_day {
      hours   = 0
      minutes = 55
      seconds = 0
      nanos   = 0
    }
    repeat_interval = "3600s"
  }
  lifecycle {
    ignore_changes = [transfer_spec]
  }
}