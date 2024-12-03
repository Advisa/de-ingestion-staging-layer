locals {
  # Define the path to your schema directory
  schema_directory = "schemas/sambla_legacy/"  # Update this path to where your schema files are stored
}


resource "null_resource" "update_sambla_legacy_table_schema" {
  for_each = toset(local.table_names)

  provisioner "local-exec" {
    command = <<EOT
      bq update --project_id=${var.project_id} \
        ${var.project_id}:sambla_legacy_integration_legacy.${each.key} \
        ${local.schema_directory}${each.key}_schema.json
    EOT
  }

  depends_on = [
    null_resource.generate_schemas
  ]
}