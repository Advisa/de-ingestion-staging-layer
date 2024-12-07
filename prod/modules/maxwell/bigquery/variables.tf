variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
}

variable "connection_id" {
  description = "The BigQuery connection ID"
  type        = string
  default = "biglake-conn"
}

variable "data_domain_project_id" {
  description = "The name of the data-domain-data-warehouse project"
  type        = string
}

variable "maxwell_bucket_name" {
  description = "The name of the bucket where source maxwell data in data-domain-data-warehouse project resides"
  type        = string
}