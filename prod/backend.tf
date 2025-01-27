terraform {
    backend "gcs" {
      bucket = "storage-terraform-remote-backend"
    }
}