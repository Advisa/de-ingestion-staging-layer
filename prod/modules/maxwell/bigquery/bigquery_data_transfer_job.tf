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

  # Merge the two lists into one
  table_names = flatten([local.table_names_events, local.table_names_credits])

  # Create a map to associate each table with its dataset
  table_datasets = merge(
    { for table in local.table_names_events : table => "maxwell_s3_data" },
    { for table in local.table_names_credits : table => "helios_staging" }
  )
}


#Having 3 resources here just to make sure not to create_table_jobs since they process 280gb of data.

resource "null_resource" "copy_table_jobs_maxwell" {
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


resource "null_resource" "create_table_jobs_maxwell" {
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
        "SELECT *,_FILE_NAME file_name FROM \`${var.data_domain_project_id}.${local.table_datasets[each.key]}.${each.key}\`"
    EOT
  }
}


resource "null_resource" "copy_table_jobs_events_r" {
  provisioner "local-exec" {
    command = <<EOT
      bq cp --force --project_id=${var.data_domain_project_id} helios_staging.event_data_sgmw_r ${var.project_id}:maxwell_integration_legacy.event_data_sgmw_r
    EOT
  }
  depends_on = [
    google_bigquery_dataset.maxwell_dataset
  ]
}


#create a resource to create event_data_sgmw_r but this results a count difference, so copying the table but having this for tracking of the query.
resource "google_bigquery_job" "load_job_to_event_data_sgmw_r_maxwell" {
    job_id     = "load_job_to_event_data_sgmw_r_maxwell"
    location   = var.region
    
    query {
      query = templatefile("${path.module}/event_data_sgmw_r.sql.tpl", {
        project_id = var.project_id
        dataset_id = google_bigquery_dataset.maxwell_dataset.dataset_id
        table_id_s3 = local.table_names_events[0]
        table_id_snowflake = local.table_names_events[1]
      }) 

    destination_table {
        project_id = var.project_id
        dataset_id = google_bigquery_dataset.maxwell_dataset.dataset_id
        table_id   = google_bigquery_table.event_data_sgmw_r_maxwell.table_id
      }

      write_disposition = "WRITE_TRUNCATE"

      allow_large_results = true
    }
  
    depends_on = [ null_resource.copy_table_jobs_maxwell,null_resource.create_table_jobs_maxwell,google_bigquery_table.event_data_sgmw_r_maxwell ]
    lifecycle {
      prevent_destroy = false
      ignore_changes = [
        job_id,
      ]
    }

  }

# Create partitioned BigQuery tables
resource "google_bigquery_table" "partitioned_tables_maxwell" {
  for_each    = toset(var.sql_templates_maxwell)
  dataset_id  = google_bigquery_dataset.maxwell_dataset.dataset_id
  project     = var.project_id
  table_id    = replace(each.key, ".sql", "") # Table name from SQL file name

  schema      = file("../prod/schemas/maxwell/${replace(each.key, ".sql", "_schema.json")}")

  time_partitioning {
    type  = "DAY"
    field = "timestamp_ts"
  }

  clustering = ["id"]

  depends_on = [
    google_bigquery_dataset.maxwell_dataset
  ]
}

# Run SQL queries from templates to create the tables
resource "google_bigquery_job" "execute_sql_maxwell_live" {
  for_each    = toset(var.sql_templates_maxwell)
  job_id      = "create_${replace(each.key, ".sql", "")}_prod_tables_live"
  project     = var.project_id
  location    = "europe-north1"


    query {
      query  = templatefile("${path.module}/p_layer_sql_templates/${each.key}", {
        project_id = var.project_id
        dataset_id = google_bigquery_dataset.maxwell_dataset.dataset_id
      })

      destination_table {
      project_id = var.project_id
      dataset_id = google_bigquery_dataset.maxwell_dataset.dataset_id
      table_id   = "${replace(each.key, ".sql", "")}"
  }

      use_legacy_sql = false
      write_disposition = "WRITE_TRUNCATE"
    }
    depends_on = [
    google_bigquery_table.partitioned_tables_maxwell
  ]
  }

