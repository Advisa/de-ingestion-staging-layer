data "google_bigquery_tables" "source_tables" {
  dataset_id = "sambla_internal_streaming" 
  project    =  var.data_domain_project_id
}

locals {
  table_names = [for table in data.google_bigquery_tables.source_tables.tables : table.table_id]
}

# Iterate over the table names and create a BigQuery job for each
resource "google_bigquery_job" "create_table_jobs" {
  for_each = toset(local.table_names)

  job_id   = "create_table_job-${each.key}-legacy" 
  location = "europe-north1"

  query {
    query = <<SQL
      SELECT * FROM `data-domain-data-warehouse.sambla_internal_streaming.${each.key}`
    SQL

    destination_table {
      project_id = "sambla-data-staging-compliance"
      dataset_id = "sambla_legacy_integration_legacy"
      table_id   = each.key
    }

    write_disposition = "WRITE_TRUNCATE"
  }

  depends_on = [
    google_bigquery_dataset.sambla_legacy_dataset,
  ]
}

