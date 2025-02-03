variable "datasets" {
  default = ["sambla_group_data_stream_deny"]
}

locals {
  root_python_path = abspath("${path.module}/../../../prod/deny_policy_creation_service")
}

# Create a null_resource for each dataset
resource "null_resource" "apply_tags_prod_test" {
  for_each = toset(var.datasets) # Iterate over the list of datasets

  provisioner "local-exec" {
    command = "python3 ${local.root_python_path}/attach_resource_to_tags.py"
    environment = {
      PROJECT_ID        = var.project_id
      DATASET_ID        = each.value
      TAG_KEY_ID        = google_tags_tag_key.tag_key_prod.short_name
      TAG_VALUE_ID      = google_tags_tag_value.tag_value_prod.short_name
    }
  }

  # Trigger the resource if any input variable changes
  triggers = {
    project_id      = var.project_id
    dataset_id      = each.value
    tag_key_id      = google_tags_tag_key.tag_key_prod.short_name
    tag_value_id    = google_tags_tag_value.tag_value_prod.short_name
  }
}
