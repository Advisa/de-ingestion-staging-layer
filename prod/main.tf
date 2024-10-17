provider "google" {
  project     = var.project_id
  region      = var.region
}


module "lvs_bigquery_config" {
  source = "./modules/lvs/bigquery"
  project_id = var.project_id
  region     = var.region
}

