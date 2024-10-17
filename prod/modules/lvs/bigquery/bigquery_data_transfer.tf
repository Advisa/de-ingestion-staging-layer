# Configuration of the scheduled query for creating applications_r table 
resource "google_bigquery_data_transfer_config" "applications_query_config" {
  display_name           = "lvs-applications-query"
  location               = "europe-north1"
  data_source_id         = "scheduled_query"
  service_account_name   = google_service_account.sa-data-transfer.email
  schedule               = "every 24 hours"
  destination_dataset_id = google_bigquery_dataset.lvs_dataset.dataset_id
  params = {
    destination_table_name_template = google_bigquery_table.applications_r.table_id
    write_disposition               = "WRITE_TRUNCATE"
    # The SQL query to execute
    #${google_bigquery_table.applications_r.table_id}S
    query                           = "CREATE OR REPLACE TABLE `${google_bigquery_table.applications_r.dataset_id}.test_applications` AS SELECT * EXCEPT(applicants) FROM `${google_bigquery_table.applications.dataset_id}.${google_bigquery_table.applications.table_id}`"

  }

  depends_on = [ google_bigquery_table.applications_r ]
}

# Configuration of the scheduled query for creating applicants_r table 
resource "google_bigquery_data_transfer_config" "applicants_query_config" {
  display_name           = "lvs-applicants-query"
  location               = "europe-north1"
  data_source_id         = "scheduled_query"
  schedule               = "every 24 hours"
  service_account_name = google_service_account.sa-data-transfer.email
  destination_dataset_id = google_bigquery_dataset.lvs_dataset.dataset_id
  params = {
    destination_table_name_template = google_bigquery_table.applicants_r.table_id
    write_disposition               = "WRITE_TRUNCATE"
    query = templatefile("${path.module}/load_applicants.sql.tpl", {
        project_id = google_bigquery_table.applications.project
        dataset_id = google_bigquery_table.applications.dataset_id
        table_id = google_bigquery_table.applications.table_id
        table_name="test_applicants"
        #table_name = google_bigquery_table.applicants_r.table_id
      })  
  }
  depends_on = [ google_bigquery_table.applicants_r ]
}

# Resources to create a BigQuery job that loads data into the applicants and applications tables.
# Note: BigQuery jobs are immutable — they cannot be modified or deleted after creation.
# To prevent Terraform from recreating these jobs with each apply, we can use a lifecycle policy.
# If you need to run a new BigQuery SQL query for these tables or any others, uncomment the lifecycle policy and provide a unique `job_id`.

resource "google_bigquery_job" "load_job_to_applications" {
    job_id     = ""
    location   = var.region
    

    query {
      # SELECT * except(applicants) FROM sg-debi-key-management.sambla_group_pii.applications_lvs
      query = "SELECT * except(applicants) FROM `${google_bigquery_table.applications.dataset_id}.${google_bigquery_table.applications.table_id}`"

      destination_table {
        project_id = google_bigquery_table.applications_r.project
        dataset_id = google_bigquery_table.applications_r.dataset_id
        table_id   = google_bigquery_table.applications_r.table_id
      }

      write_disposition = "WRITE_TRUNCATE"

      allow_large_results = true
      flatten_results = true
    }
  
    depends_on = [ google_bigquery_table.applications_r ]
    lifecycle {
      prevent_destroy = true
      ignore_changes = [
        job_id,  # Ignore changes to job_id to prevent recreation
      ]
    }

  }