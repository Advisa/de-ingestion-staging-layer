
variable "project_id" {
  description = "The Staging Compliance GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
}

variable "policy_dataset_id" {
  description = "The dataset name for policy tags metadata tables"
  type        = string
  default = "policy_tags_metadata"

  
}