resource "google_bigquery_dataset" "policy_tags_dataset" {
  dataset_id                  = "policy_tags_metadata"
  description                 = "Dataset for storing metadata tables related with policy tags"
  friendly_name               = "Policy tags metadata dataset"
  location                    = var.region
  # default_encryption_configuration {
  #   kms_key_name = var.kms_crypto_key_id
  # }
  lifecycle {
    prevent_destroy = true
  }
  
}

# Create a GCS bucket
resource "google_storage_bucket" "taxonomy_bucket" {
  name          = "taxonomy-policy-tags-data"  # Replace with a unique bucket name
  location      = "europe-north1"
  force_destroy = false  # Automatically delete bucket contents when destroyed
}
locals {
  folder = "csv-files/"
  root_schema_path = "schemas/taxonomy"
  root_csv_path = "../prod/policy_tags_service/csv_exporter/outputs"
}

# Upload taxonomy CSV file to the GCS bucket folder
resource "google_storage_bucket_object" "taxonomy_csv_file" {
  name   = "${local.folder}taxonomy.csv"  # GCS path with folder prefix
  bucket = google_storage_bucket.taxonomy_bucket.name
  source = abspath("${local.root_csv_path}/taxonomy.csv")  # Replace with your local CSV file path
  depends_on = [ google_storage_bucket.taxonomy_bucket ]
  lifecycle {
    ignore_changes = [ source, detect_md5hash ]  # if we make changes in this csv content, we have to comment out this lifecyle block before terraform apply 
  }
}

# Upload policy_tags CSV file to the GCS bucket folder
resource "google_storage_bucket_object" "policy_tags_csv_file" {
  name   = "${local.folder}policy_tags.csv"  # GCS path with folder prefix
  bucket = google_storage_bucket.taxonomy_bucket.name
  source = abspath("${local.root_csv_path}/policy_tags.csv")  # Replace with your local CSV file path
  depends_on = [ google_storage_bucket.taxonomy_bucket ]
   lifecycle {
    ignore_changes = [ source, detect_md5hash ] # if we make changes in this csv content, we have to comment out this lifecyle block before terraform apply 
  }
}

resource "google_bigquery_table" "taxonomy_bq_table" {
  dataset_id                = google_bigquery_dataset.policy_tags_dataset.dataset_id
  table_id                  = "taxonomy"
  deletion_protection       = false
  external_data_configuration {
    autodetect    = false
    source_format = "CSV"
    csv_options {
      skip_leading_rows = 1
      quote                = "\""   # Quoting character
      allow_quoted_newlines = true  # Set to true if your CSV contains quoted newlines
    }
    source_uris = ["gs://${google_storage_bucket.taxonomy_bucket.name}/${local.folder}taxonomy.csv"]
    }
    
    # must to define a schema when we create a table
    schema = file("${local.root_schema_path}/taxonomy_schema.json")
    depends_on = [ google_storage_bucket_object.taxonomy_csv_file, google_bigquery_dataset.policy_tags_dataset]
    # Uncomment this if csv file content is changed
    lifecycle {
      ignore_changes = [ schema ]
    }
}

resource "google_bigquery_table" "policy_tags_bq_table" {
  dataset_id                = google_bigquery_dataset.policy_tags_dataset.dataset_id
  table_id                  = "policy_tags"
  deletion_protection       = false
  external_data_configuration {
    autodetect    = false
    source_format = "CSV"
    csv_options {
      skip_leading_rows = 1
      quote                = "\""   # Quoting character
      allow_quoted_newlines = true  # Set to true if your CSV contains quoted newlines
    }
    source_uris = ["gs://${google_storage_bucket.taxonomy_bucket.name}/${local.folder}policy_tags.csv"]
    }
    # must to define a schema when we create a table
    schema = file("${local.root_schema_path}/policy_tags_schema.json")
    depends_on = [ google_storage_bucket_object.policy_tags_csv_file,google_bigquery_dataset.policy_tags_dataset]
    # Uncomment this if csv file content is changed
    lifecycle {
      ignore_changes = [ schema ]
    }
}
