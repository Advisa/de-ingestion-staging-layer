
variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
}
variable "sambla_legacy_bucket_name" {
  description = "The name of the bucket where source sambla legacy data in data-domain-data-warehouse project resides"
  type        = string
}
variable "data_domain_project_id" {
  description = "The name of the data-domain-data-warehouse project"
  type        = string
}