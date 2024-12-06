
variable "project_id" {
  description = "The GCP project ID"
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

variable "compliance_project_id" {
  description = "The name of the sambla-group-compliance-db project"
  type        = string
}

variable "GCP_project_roles" {
  type        = list(string)
  description = "Roles for the service account in GCP Project"
}

variable "data_domain_project_roles" {
  type        = list(string)
  description = "Roles for the service account in data_domain_project Project"
}

variable "compliance_project_roles" {
  type        = list(string)
  description = "Roles for the service account in Compliance Project"
}

variable "salus_bucket_name" {
  description = "The name of the bucket where source salus data in data-domain-data-warehouse project resides"
  type        = string
  
}

variable "sambla_legacy_bucket_name" {
  description = "The name of the bucket where source sambla legacy data in data-domain-data-warehouse project resides"
  type        = string
}

variable "maxwell_bucket_name" {
  description = "The names of the buckets where source maxwell data in data-domain-data-warehouse project resides"
  type        = string
}
