# Since this is one time triggering without schedule creating a null resource
resource "null_resource" "trigger_transfer_job" {
  depends_on = [google_storage_transfer_job.replicate-from-old-to-new-bucket-one-time]

  provisioner "local-exec" {
    command = <<EOT
      gcloud transfer jobs run ${google_storage_transfer_job.replicate-from-old-to-new-bucket-one-time.name} \
        --project=${var.project_id}
    EOT
  }
}

