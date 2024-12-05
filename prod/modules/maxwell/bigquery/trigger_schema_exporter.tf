locals {
  root_python_path = abspath("${path.module}/../../../../prod/policy_tags_service/policy_assignment/sambla_legacy")
}

resource "null_resource" "generate_schemas_maxwell_prod" {
  provisioner "local-exec" {
    command     = "python3 ${local.root_python_path}/policy_tags_sambla_legacy.py"
    working_dir = local.root_python_path
  }

  depends_on = [
    null_resource.create_table_jobs_maxwell,null_resource.copy_table_jobs_events_r,null_resource.copy_table_jobs_maxwell
  ]
}
