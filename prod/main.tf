provider "google" {
  project     = var.project_id
  region      = var.region
}


module "lvs_bigquery_config" {
  source = "./modules/lvs/bigquery"
  project_id = var.project_id
  region     = var.region
}
/*
  resource "google_bigquery_job" "load_job_to_applications" {
    job_id     = "load_job_to_applications_1"
    location   = var.region
    

    query {
      # SELECT * except(applicants) FROM sg-debi-key-management.sambla_group_pii.applications_lvs
      query = "SELECT * except(applicants) FROM `${google_bigquery_table.applications.dataset_id}.${google_bigquery_table.applications.table_id}`"

      destination_table {
        project_id = google_bigquery_table.applications_r.project
        dataset_id = google_bigquery_table.applications_r.dataset_id
        table_id   = google_bigquery_table.applications_r.table_id
      }


      write_disposition = "WRITE_TRUNCATE"

      allow_large_results = true
      flatten_results = true

    }

    
    depends_on = [ google_bigquery_table.applications_r ]
    
  }

  resource "google_bigquery_job" "load_job_to_applicants" {
    job_id     = "load_job_to_applicants_2"
    location   = var.region

    query {
      #SELECT * except(applicants) FROM sg-debi-key-management.sambla_group_pii.applications_lvs
      query = templatefile("${path.module}/load_applicants.sql.tpl", {
        project_id = google_bigquery_table.applications.project
        dataset_id = google_bigquery_table.applications.dataset_id
        table_id = google_bigquery_table.applications.table_id
      })
      #file("load_applicants.sql") 

      destination_table {
        project_id = google_bigquery_table.applicants_r.project
        dataset_id = google_bigquery_table.applicants_r.dataset_id
        table_id   = google_bigquery_table.applicants_r.table_id
      }

      write_disposition = "WRITE_TRUNCATE"

      allow_large_results = true
      flatten_results = true

    }

    depends_on = [ google_bigquery_table.applicants_r ]
  }

*/
