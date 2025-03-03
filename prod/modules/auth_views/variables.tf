
variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
}
variable "kms_crypto_key_id" {
  description = "The name of the fefault customer-managed key"
  type        = string
}
variable "connection_id" {
  description = "The BigQuery connection ID"
  type        = string
  default = "biglake-conn"
}

variable "auth_view_test_dataset_id" {
  description = "The Dataset ID for auth_view_testing"
  type        = string
  default = "auth_view_testing"
}
