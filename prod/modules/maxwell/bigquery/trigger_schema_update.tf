data "google_bigquery_tables" "dataset_maxwell" {
  dataset_id = "maxwell_integration_legacy"
  project    = var.project_id
}


locals {
  schema_directory = "schemas/maxwell/"
}

locals {
  table_names_maxwell = [
    for table in data.google_bigquery_tables.dataset_maxwell.tables : table.table_id ]
}


resource "null_resource" "update_table_schema_maxwell_prod_go_live" {
  for_each = toset(local.table_names_maxwell)

  provisioner "local-exec" {
    command = <<EOT
      bq update --project_id=${var.project_id} \
        ${var.project_id}:maxwell_integration_legacy.${each.key} \
        ${local.schema_directory}${each.key}_schema.json
    EOT
  }

  depends_on = [
    null_resource.generate_schemas_maxwell_prod_go_live
  ]
}