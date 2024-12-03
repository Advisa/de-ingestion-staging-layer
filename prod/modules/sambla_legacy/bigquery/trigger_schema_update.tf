locals {
  schema_directory = "schemas/sambla_legacy/"
}


resource "null_resource" "update_table_schema_sambla_legacy" {
  for_each = toset(local.table_names)

  provisioner "local-exec" {
    command = <<EOT
      bq update --project_id=${var.project_id} \
        ${var.project_id}:sambla_legacy_integration_legacy.${each.key} \
        ${local.schema_directory}${each.key}_schema.json
    EOT
  }

  depends_on = [
    null_resource.generate_schemas_sambla_legacy
  ]
}