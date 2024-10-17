provider "google" {
  project     = var.project_id
  region      = var.region
}


module "lvs_bigquery_config" {
  source = "./modules/lvs/bigquery"
  project_id = var.project_id
  region     = var.region
}

module "rahalaitos_gcs_config" {
  source = "./modules/rahalaitos/gcs"
  project_id = var.project_id
  region     = var.region
  rahalaitos_bucket_name = var.rahalaitos_bucket_name
  data_domain_project_id = var.data_domain_project_id
}
