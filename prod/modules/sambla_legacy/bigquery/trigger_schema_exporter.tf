locals {
  root_python_path = abspath("${path.module}/../../../../prod/policy_tags_service/policy_assignment/sambla_legacy")
}

resource "null_resource" "generate_schemas_sambla_legacy_go_live" {
  provisioner "local-exec" {
    command     = "python3 ${local.root_python_path}/policy_tags_sambla_legacy.py"
    working_dir = local.root_python_path
  }

  depends_on = [
    null_resource.create_table_jobs_sambla_legacy
  ]
}
