

locals {
  schema_directory = "schemas/advisa_history/applicants_adhis_r_schema.json"
  table_id = "advisa_history_integration_legacy.applicants_adhis_r"
}


resource "null_resource" "update_table_schema_applicants_adhis_r_prod_live" {

  provisioner "local-exec" {
    command = <<EOT
      bq update --project_id=${var.project_id} \
        ${var.project_id}:${local.table_id} \
        ${local.schema_directory}
    EOT
  }

}

output "bq_update_command" {
  value = <<EOT
    bq update --project_id=${var.project_id} \
      ${var.project_id}:${local.table_id} \
      ${local.schema_directory}
  EOT
}
