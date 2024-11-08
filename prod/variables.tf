
variable "project_id" {
  description = "The Staging Compliance GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
}

variable "rahalaitos_bucket_name" {
  description = "The name of the bucket where source rahalaitos data in data-domain-data-warehouse project resides"
  type        = string
}

variable "data_domain_project_id" {
  description = "The name of the data-domain-data-warehouse project"
  type        = string
}

variable "kms_crypto_key_id" {
  description = "The name of the fefault customer-managed key"
  type        = string
}
