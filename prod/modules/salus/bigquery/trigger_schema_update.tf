data "google_bigquery_tables" "dataset_salus_production" {
  dataset_id = "salus_group_integration"
  project    = var.data_domain_project_id
}

locals {
  schema_directory = "schemas/salus/upstream/"
}

locals {
  table_names_salus_incremental_r = [
    for table in data.google_bigquery_tables.dataset_salus_production.tables :
    table.table_id
    if can(regex(".*_salus_incremental_r$", table.table_id))
  ]
}


resource "null_resource" "update_table_schema_salus_incremental_r_prod_v2" {
  for_each = toset(local.table_names_salus_incremental_r)

  provisioner "local-exec" {
    command = <<EOT
      bq update --project_id=${var.data_domain_project_id} \
        ${var.data_domain_project_id}:salus_group_integration.${each.key} \
        ${local.schema_directory}${each.key}_schema.json
    EOT
  }

  depends_on = [
    null_resource.assign_policy_tags_salus_incremental_r_schemas
  ]
}