# Creating the dataset for lvs
resource "google_bigquery_dataset" "salus_dataset" {
  dataset_id                  = "salus_integration_legacy"
  description                 = "Integration legacy dataset for Salus stack"
  friendly_name               = "Salus Integration Legacy"
  location                    = var.region
  lifecycle {
    prevent_destroy = true
  }
  
  
}
locals {
  table_mappings = tomap({
    for line in split("\n", trimspace(file("${path.module}/salus_external_table_info.txt"))) :
    split(",", line)[0] => {
       gcs_path = trimspace(split(",", line)[1])
       max_bad_records = trimspace(split(",", line)[2])
    }
  })

   tables_with_partitioning = [
    "invitations_salus_r",
    "accounts_salus_r",
    "credit_reports_salus_r",
    "clicks_salus_r",
    "applicants_salus_r",
    "tracking_salus_r",
    "applicant-loans_salus_r",
    "applicant-jobs_salus_r",
    "bids_salus_r",
    "applicant-accommodations_salus_r",
    "clicks-applications_salus_r",
    "applications_salus_r"
]

}


# creating the external bigquery tables for the rahalaitos data
resource "google_bigquery_table" "external_tables" {
  # iterate over the local variable to get the table_name and gcs bucket path for each line in the txt file
  for_each = local.table_mappings  
  dataset_id = google_bigquery_dataset.salus_dataset.dataset_id
  # 'each.key' refers to the table name, and 'each.value' refers to the GCS location.
  table_id                  = "${each.key}"
  external_data_configuration {
    autodetect    = true
    source_format = "CSV"
    ignore_unknown_values = false
    csv_options {
      skip_leading_rows = 1
      quote = "\""
      field_delimiter = ";"
    }

    dynamic "hive_partitioning_options" {
      for_each = contains(local.tables_with_partitioning, each.key) ? [1] : []
      content {
        mode                  = "AUTO"
        source_uri_prefix     = replace(each.value.gcs_path, "*", "")
        require_partition_filter = false
      }
    }
    
    max_bad_records = each.value.max_bad_records
    connection_id = var.connection_id
    source_uris   = [each.value.gcs_path]
  }
    # must to define a schema when we create a table
    schema = file("schemas/salus/${each.key}_schema.json")
    depends_on = [ google_bigquery_dataset.salus_dataset,var.connection_id ]
    deletion_protection = true 
  
 

}



