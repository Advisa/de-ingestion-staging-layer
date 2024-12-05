# Fetch the list of tables from the first dataset
data "google_bigquery_tables" "source_tables_events" {
  dataset_id = "maxwell_s3_data"
  project    = var.data_domain_project_id
}

# Fetch the list of tables from the second dataset
data "google_bigquery_tables" "source_tables_credits" {
  dataset_id = "helios_staging"
  project    = var.data_domain_project_id
}

data "google_bigquery_tables" "source_tables_events_test" {
  dataset_id = "playgrounds"
  project    = var.data_domain_project_id
}

# Combine tables from both datasets

locals {
  # List of tables to copy from both datasets (matching specific patterns)
  table_names_events = [
    for table in data.google_bigquery_tables.source_tables_events.tables : table.table_id 
    if table.table_id == "raw_data_s3_new" || table.table_id == "raw_data_snowflake_export_persisted"
  ]

  table_names_credits = [
    for table in data.google_bigquery_tables.source_tables_credits.tables : table.table_id 
    if length(regexall("xml_extract$", table.table_id)) > 0
  ]

   table_names_test = [
    for table in data.google_bigquery_tables.source_tables_events_test.tables : table.table_id 
    if table.table_id == "raw_data_s3_new_test"
  ]

  # Merge the two lists into one
  table_names = flatten([local.table_names_events, local.table_names_credits, local.table_names_test])

  # Create a map to associate each table with its dataset
  table_datasets = merge(
    { for table in local.table_names_events : table => "maxwell_s3_data" },
    { for table in local.table_names_credits : table => "helios_staging" },
    { for table in local.table_names_test : table => "playgrounds" }
  )
}


#Having 2 resources here just to make sure not to create_table_jobs since they process 280gb of data.

resource "null_resource" "copy_table_jobs" {
  for_each = tomap({
    for table_name in local.table_names : table_name => table_name
    if table_name == "raw_data_snowflake_export_persisted" || table_name == "credit_reports_xml_extract"
  })

  provisioner "local-exec" {
    command = <<EOT
      echo "Using Service Account: $(gcloud auth list --filter=status:ACTIVE --format='value(account)')"
      echo "Copying table: ${each.key}"
      bq cp --force --project_id=${var.data_domain_project_id} ${local.table_datasets[each.key]}.${each.key} ${var.project_id}:maxwell_integration_legacy.${each.key}
    EOT
  }
  depends_on = [
    google_bigquery_dataset.maxwell_dataset
  ]
}


resource "null_resource" "create_table_jobs" {
  for_each = tomap({
    for table_name in local.table_names : table_name => table_name
    if table_name == "raw_data_s3_new"
  })

  provisioner "local-exec" {
    command = <<EOT
      echo "Using Service Account: $(gcloud auth list --filter=status:ACTIVE --format='value(account)')"
      echo "Querying table: ${each.key}"
      bq query --use_legacy_sql=false \
        --destination_table=${var.project_id}:maxwell_integration_legacy.${each.key} \
        "SELECT * FROM \`${var.data_domain_project_id}.${local.table_datasets[each.key]}.${each.key}\`"
    EOT
  }
}


resource "null_resource" "create_table_jobs_test" {
  for_each = tomap({
    for table_name in local.table_names : table_name => table_name
    if table_name == "raw_data_s3_new_test"
  })

  provisioner "local-exec" {
    command = <<EOT
      echo "Using Service Account: $(gcloud auth list --filter=status:ACTIVE --format='value(account)')"
      echo "Querying table: ${each.key}"
      bq query --use_legacy_sql=false \
        --destination_table=${var.project_id}:maxwell_integration_legacy.${each.key} \
        "SELECT * FROM \`${var.data_domain_project_id}.${local.table_datasets[each.key]}.${each.key}\`"
    EOT
  }
}
#create a resource to create event_data_sgmw_r
resource "google_bigquery_job" "load_job_to_event_data_sgmw_r_test" {
    job_id     = "load_job_to_event_data_sgmw_r_test"
    location   = var.region
    
    query {
      query = templatefile("${path.module}/event_data_sgmw_r.sql.tpl", {
        project_id = var.project_id
        dataset_id = google_bigquery_dataset.maxwell_dataset.dataset_id
        table_id_s3 = local.table_names_events[0]
        table_id_snowflake = local.table_names_events[1]
        table_name = "event_data_sgmw_r"
      }) 

      write_disposition = "WRITE_TRUNCATE"

      allow_large_results = true
    }
  
    depends_on = [ null_resource.copy_table_jobs,null_resource.create_table_jobs ]
    lifecycle {
      prevent_destroy = true
      ignore_changes = [
        job_id,
      ]
    }

  }


