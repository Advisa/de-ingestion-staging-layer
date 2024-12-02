# Trigger Python script to generate schema files after data transfer is done
resource "null_resource" "update_schemas" {
  provisioner "local-exec" {
    command     = "python3 /Users/aruldharani/Sambla/de-ingestion-staging-layer-1/prod/policy_tags_service/policy_assignment/update_sambla_legacy_table_schema.py"
    working_dir = "/Users/aruldharani/Sambla/de-ingestion-staging-layer-1/prod/policy_tags_service/policy_assignment"
  }

  depends_on = [
    null_resource.generate_schemas
  ]
}

