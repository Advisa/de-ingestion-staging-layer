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
    split(",", line)[0] => {
       gcs_path = trimspace(split(",", line)[1])
       file_format = trimspace(split(",", line)[3])
       file_delimeter = trimspace(split(",", line)[4])

      }

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
    dynamic "csv_options" {
      for_each = each.value.file_format == "CSV" ? [1] : []
      content{
        quote = "\""
        field_delimiter = each.value.file_delimeter
      }
    }
    autodetect  = each.value.file_format == "CSV" ? true: false
    ignore_unknown_values = false
    compression = "GZIP" 
    source_format = each.value.file_format
    connection_id = var.connection_id
    source_uris   = [each.value.gcs_path]
  }
    # must to define a schema when we create a table
    schema = file("schemas/advisa_history/${each.key}_schema.json")
    depends_on = [ google_bigquery_dataset.advisa_history_dataset ]
      
}
