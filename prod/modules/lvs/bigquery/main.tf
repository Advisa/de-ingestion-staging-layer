# Creating the external table for applications data of lvs
resource "google_bigquery_table" "applications" {
  dataset_id                = google_bigquery_dataset.lvs_dataset.dataset_id
  table_id                  = "applications_lvs"
  deletion_protection       = true
  external_data_configuration {
    autodetect    = false
    source_format = "NEWLINE_DELIMITED_JSON"
    connection_id = var.connection_id
    source_uris = [
      "gs://sambla-group-lvs-integration-legacy/applications/*"
        ]
    }
    # must to define a schema when we create a table
    schema = file("schemas/lvs/applications_lvs_schema.json")
    depends_on = [ google_bigquery_dataset.lvs_dataset ]
}

# Creating a table for applications data of lvs 
resource "google_bigquery_table" "applications_r" {
  dataset_id = google_bigquery_dataset.lvs_dataset.dataset_id
  table_id   = "applications_lvs_r"
  deletion_protection       = true
  schema = file("schemas/lvs/applications_lvs_r_schema.json")

  depends_on = [ google_bigquery_table.applications ]

}

# Creating a table for applicants data of lvs
resource "google_bigquery_table" "applicants_r" {
  dataset_id = google_bigquery_dataset.lvs_dataset.dataset_id
  table_id   = "applicants_lvs_r"
  deletion_protection       = true
  schema = file("schemas/lvs/applicants_lvs_r_schema.json")

  depends_on = [ google_bigquery_table.applications ]

}
# Creating the external table for credit remarks data of lvs
resource "google_bigquery_table" "credit_remarks" {
  dataset_id                = google_bigquery_dataset.lvs_dataset.dataset_id
  table_id                  = "credit_remarks_lvs_r"
  deletion_protection       = true
    external_data_configuration {
    autodetect    = false
    source_format = "NEWLINE_DELIMITED_JSON"
    connection_id = var.connection_id
    source_uris = [
      "gs://sambla-group-lvs-integration-legacy/credit_remarks/*"
        ]
    }
  
  schema = file("schemas/lvs/credit_remarks_lvs_r_schema.json")
  depends_on = [ google_bigquery_dataset.lvs_dataset ]
}

# Creating the external table for offers data of lvs
resource "google_bigquery_table" "offers" {
  dataset_id                = google_bigquery_dataset.lvs_dataset.dataset_id
  table_id                  = "offers_lvs_r"
  deletion_protection       = true
   external_data_configuration {
    autodetect    = false
    source_format = "NEWLINE_DELIMITED_JSON"
    connection_id = var.connection_id
    source_uris = [
      "gs://sambla-group-lvs-integration-legacy/offers/*"
        ]
    }


  schema = file("schemas/lvs/offers_lvs_r_schema.json")
  depends_on = [ google_bigquery_dataset.lvs_dataset ]
}

# Creating the external table for providers data of lvs
resource "google_bigquery_table" "providers" {
  dataset_id                = google_bigquery_dataset.lvs_dataset.dataset_id
  table_id                  = "providers_lvs_r"
  deletion_protection       = true
   external_data_configuration {
    autodetect    = false
    source_format = "NEWLINE_DELIMITED_JSON"
    connection_id = var.connection_id
    source_uris = [
      "gs://sambla-group-lvs-integration-legacy/providers/*"
        ]
    }
  
  schema = file("schemas/lvs/providers_lvs_r_schema.json")
  depends_on = [ google_bigquery_dataset.lvs_dataset ]
}


