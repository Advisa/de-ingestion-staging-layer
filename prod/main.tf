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
  
  schema = file("schemas/credit_remarks_schema.json")
}

resource "google_bigquery_table" "offers" {
  dataset_id                = data.google_bigquery_dataset.sambla_group_lvs_integration_legacy.dataset_id
  table_id                  = "offers"
  deletion_protection       = false
  
  schema = file("schemas/offers_schema.json")
}

resource "google_bigquery_table" "providers" {
  dataset_id                = data.google_bigquery_dataset.sambla_group_lvs_integration_legacy.dataset_id
  table_id                  = "providers"
  deletion_protection       = false
  
  schema = file("schemas/providers_schema.json")
}

resource "google_bigquery_table" "leaddesk_reasons_external" {
  dataset_id                = data.google_bigquery_dataset.leaddesk_integration.dataset_id
  table_id                  = "reasons_external"
  deletion_protection       = false
  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    compression   = "GZIP"

    source_uris = [
      "gs://leaddesk-integration-prod/staging_area_for_incremental_load/reasons/*"
    ]
  }
}

resource "google_bigquery_table" "leaddesk_contacts_external" {
  dataset_id                = data.google_bigquery_dataset.leaddesk_integration.dataset_id
  table_id                  = "contacts_external"
  deletion_protection       = false
  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    compression   = "GZIP"

    source_uris = [
      "gs://leaddesk-integration-prod/staging_area_for_incremental_load/contacts/*"
    ]
  }
}

resource "google_bigquery_table" "leaddesk_customers_external" {
  dataset_id                = data.google_bigquery_dataset.leaddesk_integration.dataset_id
  table_id                  = "customers_external"
  deletion_protection       = false
  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    compression   = "GZIP"

    source_uris = [
      "gs://leaddesk-integration-prod/staging_area_for_incremental_load/customers/*"
    ]
  }
}

resource "google_bigquery_table" "leaddesk_agent_groups_view" {
  dataset_id                = data.google_bigquery_dataset.leaddesk_integration.dataset_id
  table_id                  = "agent_groups"
  deletion_protection       = false
  view {
    query          = "SELECT * FROM `data-domain-data-warehouse.leaddesk_integration_raw.agent_groups`"
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "leaddesk_agents_view" {
  dataset_id                = data.google_bigquery_dataset.leaddesk_integration.dataset_id
  table_id                  = "agents"
  deletion_protection       = false
  view {
    query          = "SELECT * FROM `data-domain-data-warehouse.leaddesk_integration_raw.agents`"
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "leaddesk_calling_lists_view" {
  dataset_id                = data.google_bigquery_dataset.leaddesk_integration.dataset_id
  table_id                  = "calling_lists"
  deletion_protection       = false
  view {
    query          = "SELECT * FROM `data-domain-data-warehouse.leaddesk_integration_raw.calling_lists`"
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "leaddesk_campaigns_view" {
  dataset_id                = data.google_bigquery_dataset.leaddesk_integration.dataset_id
  table_id                  = "campaigns"
  deletion_protection       = false
  view {
    query          = "SELECT * FROM `data-domain-data-warehouse.leaddesk_integration_raw.campaigns`"
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "leaddesk_agent_groups_agents" {
  dataset_id                = data.google_bigquery_dataset.leaddesk_integration.dataset_id
  table_id                  = "agent_groups_agents"
  deletion_protection       = false
  
  time_partitioning {
    type = "DAY"
    field = "_airbyte_extracted_at"
  }

  clustering = ["hash_key", "id"]

  schema = file("schemas/agent_groups_agents.json")
}

resource "google_bigquery_table" "leaddesk_blacklist" {
  dataset_id                = data.google_bigquery_dataset.leaddesk_integration.dataset_id
  table_id                  = "blacklist"
  deletion_protection       = false
  
  time_partitioning {
    type = "DAY"
    field = "_airbyte_extracted_at"
  }

  clustering = ["hash_key", "phone"]

  schema = file("schemas/blacklist.json")
}

resource "google_bigquery_table" "leaddesk_call_logs" {
  dataset_id                = data.google_bigquery_dataset.leaddesk_integration.dataset_id
  table_id                  = "call_logs"
  deletion_protection       = false
  
  time_partitioning {
    type = "DAY"
    field = "_airbyte_extracted_at"
  }

  clustering = ["hash_key", "id"]

  schema = file("schemas/call_logs.json")
}

resource "google_bigquery_table" "leaddesk_calling_lists_campaigns" {
  dataset_id                = data.google_bigquery_dataset.leaddesk_integration.dataset_id
  table_id                  = "calling_lists_campaigns"
  deletion_protection       = false
  
  time_partitioning {
    type = "DAY"
    field = "_airbyte_extracted_at"
  }

  clustering = ["hash_key", "campaign_id", "calling_list_id"]

  schema = file("schemas/calling_lists_campaigns.json")
}

resource "google_bigquery_table" "leaddesk_reasons" {
  dataset_id                = data.google_bigquery_dataset.leaddesk_integration.dataset_id
  table_id                  = "reasons"
  deletion_protection       = false
  
  time_partitioning {
    type = "DAY"
    field = "_airbyte_extracted_at"
  }

  clustering = ["hash_key", "id"]

  schema = file("schemas/reasons.json")
}

resource "google_bigquery_table" "leaddesk_contacts" {
  dataset_id                = data.google_bigquery_dataset.leaddesk_integration.dataset_id
  table_id                  = "contacts"
  deletion_protection       = false
  
  time_partitioning {
    type = "DAY"
    field = "_airbyte_extracted_at"
  }

  clustering = ["hash_key", "id"]

  schema = file("schemas/contacts.json")
}

resource "google_bigquery_table" "leaddesk_customers" {
  dataset_id                = data.google_bigquery_dataset.leaddesk_integration.dataset_id
  table_id                  = "customers"
  deletion_protection       = false
  
  time_partitioning {
    type = "DAY"
    field = "_airbyte_extracted_at"
  }

  clustering = ["hash_key", "id"]

  schema = file("schemas/customers.json")
}