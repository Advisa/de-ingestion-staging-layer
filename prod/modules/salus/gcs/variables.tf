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
variable "salus_bucket_name" {
  description = "The name of the bucket where source salus data in data-domain-data-warehouse project resides"
  type        = string
}
variable "data_domain_project_id" {
  description = "The name of the data-domain-data-warehouse project"
  type        = string
}

variable "aws_access_key" {
  description = "access key of s3 bucket"
  type        = string
  default = "value_of_access_key"
}
variable "aws_secret_key" {
  description = "secret key of s3 bucket"
  type        = string
  default = "value_of_secret_key"
}

