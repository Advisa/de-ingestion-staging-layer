# Creating the dataset for lvs
resource "google_bigquery_dataset" "advisa_history_dataset" {
  dataset_id                  = "advisa_history_integration_legacy"
  description                 = "Integration legacy dataset for advisa history"
  friendly_name               = "Advisa History Integration Legacy"
  location                    = var.region
  lifecycle {
    prevent_destroy = true
  } 
}

# generate a local variable that maps external table names to their corresponding GCS locations.
locals {
  table_mappings = tomap({
    for line in split("\n", trimspace(file("${path.module}/advisa_history_external_table_info.txt"))) :
    split(",", line)[0] => split(",", line)[1]
  })
}

# creating the external bigquery tables for the advisa history data
resource "google_bigquery_table" "external_tables" {
  # iterate over the local variable to get the table_name and gcs bucket path for each line in the txt file
  for_each = local.table_mappings 
  dataset_id                = google_bigquery_dataset.advisa_history_dataset.dataset_id
  # 'each.key' refers to the table name, and 'each.value' refers to the GCS location.
  table_id                  = "${each.key}"
  deletion_protection       = true
    external_data_configuration {
    autodetect    = false
    ignore_unknown_values = false
    compression = "GZIP"
    source_format = "NEWLINE_DELIMITED_JSON"
    connection_id = var.connection_id
    source_uris   = [each.value]
  }
    # must to define a schema when we create a table
    schema = file("schemas/advisa_history/${each.key}_schema.json")
    depends_on = [ google_bigquery_dataset.advisa_history_dataset ]
      
}
