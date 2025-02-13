# Configuration of the scheduled query for creating applications_r table 
resource "google_bigquery_data_transfer_config" "applications_query_config" {
  display_name           = "lvs-applications-query"
  location               = "europe-north1"
  data_source_id         = "scheduled_query"
  service_account_name   = google_service_account.sa_data_transfer.email
  schedule               = "1st monday of january 00:00"
  destination_dataset_id = google_bigquery_dataset.lvs_dataset.dataset_id
  params = {
    # The SQL query to execute
    query                           = "CREATE OR REPLACE TABLE `${google_bigquery_table.applications_r.dataset_id}.${google_bigquery_table.applications_r.table_id}` AS SELECT * EXCEPT(applicants) FROM `${google_bigquery_table.applications.dataset_id}.${google_bigquery_table.applications.table_id}`"

  }
  lifecycle {
    prevent_destroy = true
  }

  depends_on = [ google_bigquery_table.applications_r ]
}

# Configuration of the scheduled query for creating applicants_r table 
resource "google_bigquery_data_transfer_config" "applicants_query_config" {
  display_name           = "lvs-applicants-query"
  location               = var.region
  data_source_id         = "scheduled_query"
  schedule               = "1st monday of january 00:00"
  service_account_name = google_service_account.sa_data_transfer.email
  destination_dataset_id = google_bigquery_dataset.lvs_dataset.dataset_id
  params = {
    query = templatefile("${path.module}/load_applicants.sql.tpl", {
        project_id = google_bigquery_table.applications.project
        dataset_id = google_bigquery_table.applications.dataset_id
        table_id = google_bigquery_table.applications.table_id
        table_name = google_bigquery_table.applicants_r.table_id
      })  
  }
  lifecycle {
    prevent_destroy = true
  }
  depends_on = [ google_bigquery_table.applicants_r ]
}

# Resources to create a BigQuery job that loads data into the applicants and applications tables.
# Note: BQ jobs are an alternative to the scheduled query, if needed please refer to this job resource.
# Note: BigQuery jobs are immutable — they cannot be modified or deleted after creation.
# To prevent Terraform from recreating these jobs with each apply, we can use a lifecycle policy.
# If you need to run a new BigQuery SQL query for these tables or any others, uncomment the lifecycle policy and provide a unique `job_id`.

resource "google_bigquery_job" "load_job_to_applications" {
    job_id     = "load_job_to_applications_5"
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


# Run SQL queries from templates to create the tables
resource "google_bigquery_job" "p_layer_tables_execute_sql" {
  for_each    = toset(var.sql_templates)
  job_id      = "create_${replace(each.key, ".sql", "")}_layer_tables_prod_live"
  project     = var.project_id
  location    = "europe-north1"


    query {
      query  = templatefile("${path.module}/p_layer_sql_templates/${each.key}", {
        project_id = var.project_id
        dataset_id = google_bigquery_dataset.lvs_dataset.dataset_id
      })

      destination_table {
      project_id = var.project_id
      dataset_id = google_bigquery_dataset.lvs_dataset.dataset_id
      table_id   = "${replace(each.key, ".sql", "")}"
  }

      use_legacy_sql = false
      write_disposition = "WRITE_TRUNCATE"
    }
    depends_on = [
    google_bigquery_table.applicants_r,
    google_bigquery_table.applications_r,
    google_bigquery_table.offers,
    google_bigquery_table.providers
  ]
  }