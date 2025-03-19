# Data transfer job to replicate the rahalaitos data to new project bucket
resource "google_storage_transfer_job" "replicate-from-old-to-new-bucket" {
  description = "Data Transfer job to replicate source rahalaitos data from domain-data-warehouse project to new staging-compliance project"
  project     = var.project_id
  

  transfer_spec {
    transfer_options {
      overwrite_when = "DIFFERENT"
    }
    gcs_data_source {
      bucket_name = var.rahalaitos_bucket_name 
    }
    gcs_data_sink {
      bucket_name = google_storage_bucket.gcs_rahalaitos_bucket.name
    }

  }

  schedule {
    schedule_start_date {
      year  = 2024
      month = 10
      day   = 17
    }
    start_time_of_day {
      hours   = 06
      minutes = 00
      seconds = 0
      nanos   = 0
    } 
  }

   logging_config {
    enable_on_prem_gcs_transfer_logs = false
    log_action_states = ["SUCCEEDED", "FAILED"]
    log_actions = ["COPY", "DELETE", "FIND"]
  }


}