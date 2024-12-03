data "google_bigquery_tables" "source_tables" {
  dataset_id = "sambla_internal_streaming" 
  project    =  var.data_domain_project_id
}

locals {
  # List of table names excluding the ones you want to ignore
  table_names = [for table in data.google_bigquery_tables.source_tables.tables : table.table_id if length(regexall("gcs_streaming$", table.table_id)) > 0]
}

resource "null_resource" "create_table_jobs" {
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
