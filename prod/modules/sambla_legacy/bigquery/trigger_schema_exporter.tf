# Trigger Python script to generate schema files after data transfer is done
resource "null_resource" "generate_schemas" {
  provisioner "local-exec" {
    command     = "python3 /Users/aruldharani/Sambla/de-ingestion-staging-layer-1/prod/policy_tags_service/policy_assignment/sambla_legacy/policy_tags_sambla_legacy.py"
    working_dir = "/Users/aruldharani/Sambla/de-ingestion-staging-layer-1/prod/policy_tags_service/policy_assignment/sambla_legacy"
  }

  depends_on = [
    google_bigquery_job.create_table_jobs
  ]
}

