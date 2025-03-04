locals {
  policy_service_python_path = abspath("${path.module}/../../../../prod/policy_tags_service/policy_assignment")
}

resource "null_resource" "assign_policy_tags_salus_incremental_r_schemas" {
   provisioner "local-exec" {
    command     = "python3 ${local.policy_service_python_path}/main.py"
    working_dir = local.policy_service_python_path
  }

  depends_on = [
    null_resource.generate_schemas_salus_incremenal_r
  ]
}