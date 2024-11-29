provider "google" {
  project     = var.project_id
  region      = var.region
}


module "lvs_bigquery_config" {
  source = "./modules/lvs/bigquery"
  project_id = var.project_id
  region     = var.region
  kms_crypto_key_id = var.kms_crypto_key_id
  connection_id =google_bigquery_connection.default.name
}

module "rahalaitos_gcs_config" {
  source = "./modules/rahalaitos/gcs"
  project_id = var.project_id
  region     = var.region
  rahalaitos_bucket_name = var.rahalaitos_bucket_name
  data_domain_project_id = var.data_domain_project_id
}

module "rahalaitos_bq_config" {
  source = "./modules/rahalaitos/bigquery"
  project_id = var.project_id
  region     = var.region
  connection_id = google_bigquery_connection.default.name
}
module "taxonomy_config" {
  source = "./modules/taxonomy"
  region = var.region
  project_id = var.project_id
}

module "authorized_views_config" {
  source = "./modules/auth_views"
  project_id = var.project_id
  region     = var.region
  kms_crypto_key_id = var.kms_crypto_key_id
  connection_id = google_bigquery_connection.default.name

}

module "salus_bq_config" {
  source = "./modules/salus/bigquery"
  project_id = var.project_id
  region     = var.region
  connection_id = google_bigquery_connection.default.name
}
module "salus_gcs_config" {
  source = "./modules/salus/gcs"
  project_id = var.project_id
  region     = var.region
  data_domain_project_id = var.data_domain_project_id
  salus_bucket_name = var.salus_bucket_name
}

module "policy_tags_config" {
  source     = "./modules/policy_tags"
  project_id = var.project_id
  region     = var.region
}
