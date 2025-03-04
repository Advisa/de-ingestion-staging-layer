data "google_bigquery_tables" "dataset_lvs_integration_legacy" {
  dataset_id = "lvs_integration_legacy"
  project    = var.project_id
}

locals {
  schema_directory = "schemas/lvs/p_layer/"
}

locals {
  table_names_lvs_tables = [
    "applicant_financials_lvs_p",
    "applicant_consents_lvs_p",
    "application_commissions_lvs_p",
    "credit_remarks_lvs_p",
    "offer_states_lvs_p",
    "providers_lvs_p",
    "applications_lvs_p",
    "applicant_cards_lvs_p",
    "offers_lvs_p",
    "clients_lvs_p",
    "provider_commissions_lvs_p",
    "applicants_lvs_p"
  ]
}


resource "null_resource" "update_table_schema_lvs_prod_live" {
  for_each = toset(local.table_names_lvs_tables)

  provisioner "local-exec" {
    command = <<EOT
      bq update --project_id=${var.project_id} \
        ${var.project_id}:${data.google_bigquery_tables.dataset_lvs_integration_legacy.dataset_id}.${each.key} \
        ${local.schema_directory}${each.key}_schema.json
    EOT
  }

  depends_on = [
    data.google_bigquery_tables.dataset_lvs_integration_legacy
  ]
}

