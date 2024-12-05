# Creating the dataset for maxwell
resource "google_bigquery_dataset" "maxwell_dataset" {
  dataset_id                  = "maxwell_integration_legacy"
  description                 = "Integration legacy dataset for maxwell stack"
  friendly_name               = "maxwell Integration Legacy"
  location                    = var.region
  lifecycle {
    prevent_destroy = true
  }
}


resource "google_bigquery_table" "event_data_sgmw_r_maxwell" {
  dataset_id = google_bigquery_dataset.maxwell_dataset.dataset_id
  table_id   = "event_data_sgmw_r"
  deletion_protection       = true
  schema = file("schemas/maxwell/event_data_sgmw_r_schema.json")
  time_partitioning {
    type          = "DAY"
    field         = "event_date"
  }

  clustering = ["table"]

  depends_on = [ google_bigquery_dataset.maxwell_dataset ]

}