data "google_bigquery_tables" "dataset_sambla_legacy" {
  dataset_id = "sambla_legacy_integration_legacy"
  project    = var.project_id
}

locals {
  schema_directory = "schemas/sambla_legacy/"
}

locals {
  table_names_sambla_legacy = [
    for table in data.google_bigquery_tables.dataset_sambla_legacy.tables : table.table_id ]
}


resource "null_resource" "update_table_schema_sambla_legacy_prod_live" {
  for_each = toset(local.table_names_sambla_legacy)

  provisioner "local-exec" {
    command = <<EOT
      bq update --project_id=${var.project_id} \
        ${var.project_id}:sambla_legacy_integration_legacy.${each.key} \
        ${local.schema_directory}${each.key}_schema.json
    EOT
  }

  depends_on = [
    null_resource.generate_schemas_sambla_legacy_go_live
  ]
}