locals {
  root_python_path = abspath("${path.module}/../../../../prod/schema_exporter")
}

resource "null_resource" "generate_schemas_salus_incremenal_r" {
  provisioner "local-exec" {
    command     = "python3 ${local.root_python_path}/export_schemas.py"
    working_dir = local.root_python_path
  }

}
