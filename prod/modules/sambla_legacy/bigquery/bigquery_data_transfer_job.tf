data "google_bigquery_tables" "source_tables" {
  dataset_id = "sambla_internal_streaming" 
  project    =  var.data_domain_project_id
}

locals {
  # List of table names excluding the ones you want to ignore
  table_names = [for table in data.google_bigquery_tables.source_tables.tables : table.table_id if length(regexall("gcs_streaming$", table.table_id)) > 0]
}

resource "null_resource" "create_table_jobs_sambla_legacy" {
  for_each = toset(local.table_names)

  provisioner "local-exec" {
    command = <<EOT
      bq cp --force --project_id=${var.project_id} \
        ${var.data_domain_project_id}:sambla_internal_streaming.${each.key} \
        ${var.project_id}:sambla_legacy_integration_legacy.${each.key}
    EOT
  }

  depends_on = [
    google_bigquery_dataset.sambla_legacy_dataset  # Ensure the dataset exists before the job runs
  ]
}

# Create partitioned BigQuery tables
resource "google_bigquery_table" "partitioned_tables" {
  for_each    = toset(var.sql_templates)
  dataset_id  = google_bigquery_dataset.sambla_legacy_dataset.dataset_id
  project     = var.project_id
  table_id    = replace(each.key, ".sql", "") # Table name from SQL file name

  schema      = file("../prod/schemas/sambla_legacy/${replace(each.key, ".sql", "_schema.json")}")

  time_partitioning {
    type  = "DAY"
    field = "time_archived"
  }

  depends_on = [
    google_bigquery_dataset.sambla_legacy_dataset
  ]

}

# Run SQL queries from templates to create the tables
resource "google_bigquery_job" "execute_sql" {
  for_each    = toset(var.sql_templates)
  job_id      = "create_${replace(each.key, ".sql", "")}_prod_tables_live_go"
  project     = var.project_id
  location    = "europe-north1"


    query {
      query  = templatefile("${path.module}/p_layer_sql_templates/${each.key}", {
        project_id = var.project_id
        dataset_id = google_bigquery_dataset.sambla_legacy_dataset.dataset_id
      })

      destination_table {
      project_id = var.project_id
      dataset_id = google_bigquery_dataset.sambla_legacy_dataset.dataset_id
      table_id   = "${replace(each.key, ".sql", "")}"
  }

      use_legacy_sql = false
      write_disposition = "WRITE_TRUNCATE"
    }
    depends_on = [google_bigquery_table.partitioned_tables]
    lifecycle {
      ignore_changes = [ query, job_id ]
    }
  }

# Run SQL query from template to create the table for new "applications_all_versions" model
resource "google_bigquery_job" "execute_sql_applications_all_versions" {
  job_id      = "create_applications_all_versions_remodeled_sambq_p_prod_tables_live_go"
  project     = var.project_id
  location    = "europe-north1"


    query {
      query  = templatefile("${path.module}/p_layer_sql_templates/applications_all_versions_sambq_p.sql", {
        project_id = var.project_id
        dataset_id = google_bigquery_dataset.sambla_legacy_dataset.dataset_id
      })

      destination_table {
      project_id = var.project_id
      dataset_id = google_bigquery_dataset.sambla_legacy_dataset.dataset_id
      table_id   = "applications_all_versions_sambq_p"
  }

      use_legacy_sql = false
      write_disposition = "WRITE_TRUNCATE"
    }
    depends_on = [google_bigquery_table.partitioned_tables]
  }


# Run SQL query from template to re-create the table for old "applications_all_versions" model
# This can be deleted later
resource "google_bigquery_job" "execute_sql_applications_all_versions_history" {
  job_id      = "create_applications_all_versions_history_sambq_p_prod_tables_live_go"
  project     = var.project_id
  location    = "europe-north1"


    query {
      query  = templatefile("${path.module}/p_layer_sql_templates/applications_all_versions_history_sambq_p.sql", {
        project_id = var.project_id
        dataset_id = google_bigquery_dataset.sambla_legacy_dataset.dataset_id
      })

      destination_table {
      project_id = var.project_id
      dataset_id = google_bigquery_dataset.sambla_legacy_dataset.dataset_id
      table_id   = "applications_all_versions_history_sambq_p"
  }

      use_legacy_sql = false
      write_disposition = "WRITE_TRUNCATE"
    }
    depends_on = [google_bigquery_table.partitioned_tables]

  }

# Run SQL query from template to create the table for new "applications_credit_reports" model
resource "google_bigquery_job" "execute_sql_applications_credit_reports" {
  job_id      = "create_applications_credit_reports_remodeled_sambq_p_prod_tables_live_go"
  project     = var.project_id
  location    = "europe-north1"


    query {
      query  = templatefile("${path.module}/p_layer_sql_templates/applications_credit_reports_sambq_p.sql", {
        project_id = var.project_id
        dataset_id = google_bigquery_dataset.sambla_legacy_dataset.dataset_id
      })

      destination_table {
      project_id = var.project_id
      dataset_id = google_bigquery_dataset.sambla_legacy_dataset.dataset_id
      table_id   = "applications_credit_reports_sambq_p"
  }

      use_legacy_sql = false
      write_disposition = "WRITE_TRUNCATE"
    }
    depends_on = [google_bigquery_table.partitioned_tables]
  }

#only for applications_loans_sambq as the current query logic doesnt work
resource "null_resource" "create_table_jobs_sambq_appl_loans_live_go" {

  provisioner "local-exec" {
    command = <<EOT
      bq cp --force --project_id=${var.project_id} \
        ${var.data_domain_project_id}:helios_staging.applications_loans_sambq_p \
        ${var.project_id}:sambla_legacy_integration_legacy.applications_loans_sambq_p
    EOT
  }

  depends_on = [
    google_bigquery_dataset.sambla_legacy_dataset
  ]
}