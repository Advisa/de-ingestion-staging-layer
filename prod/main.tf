provider "google" {
  project     = var.project_id
  region      = var.region
}

data "google_bigquery_dataset" "sambla_group_lvs_integration_legacy" {
  dataset_id                  = "sambla_group_lvs_integration_legacy"
}

resource "google_bigquery_table" "applications" {
  dataset_id                = data.google_bigquery_dataset.sambla_group_lvs_integration_legacy.dataset_id
  table_id                  = "applications"
  deletion_protection       = false
  external_data_configuration {
    autodetect    = false
    source_format = "json"

    source_uris = [
      "gs://sambla-group-lvs-integration-legacy/applications/*"
        ]
    }
  
    time_partitioning {
        type = "DAY"
        field = "timestamp"
    }
    schema = file("schemas/applications_schema.json")
}

resource "google_bigquery_table" "credit_remarks" {
  dataset_id                = data.google_bigquery_dataset.sambla_group_lvs_integration_legacy.dataset_id
  table_id                  = "credit_remarks"
  deletion_protection       = false
    external_data_configuration {
    autodetect    = false
    source_format = "json"

    source_uris = [
      "gs://sambla-group-lvs-integration-legacy/credit_remarks/*"
        ]
    }
  
  schema = file("schemas/credit_remarks_schema.json")
}

resource "google_bigquery_table" "offers" {
  dataset_id                = data.google_bigquery_dataset.sambla_group_lvs_integration_legacy.dataset_id
  table_id                  = "offers"
  deletion_protection       = false
   external_data_configuration {
    autodetect    = false
    source_format = "json"

    source_uris = [
      "gs://sambla-group-lvs-integration-legacy/offers/*"
        ]
    }


  schema = file("schemas/offers_schema.json")
}

resource "google_bigquery_table" "providers" {
  dataset_id                = data.google_bigquery_dataset.sambla_group_lvs_integration_legacy.dataset_id
  table_id                  = "providers"
  deletion_protection       = false
   external_data_configuration {
    autodetect    = false
    source_format = "json"

    source_uris = [
      "gs://sambla-group-lvs-integration-legacy/providers/*"
        ]
    }
  
  schema = file("schemas/providers_schema.json")
}