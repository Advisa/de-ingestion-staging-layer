
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

variable "sql_templates" {
  description = "List of SQL template files"
  type        = list(string)
  default     = [
    "applicant_consents_lvs_p.sql",
    "providers_lvs_p.sql",
    "applicant_cards_lvs_p.sql",
    "applicant_financials_lvs_p.sql",
    "offer_states_lvs_p.sql",
    "application_commissions_lvs_p.sql",
    "provider_commissions_lvs_p.sql",
    "applicants_lvs_p.sql"
  ]
}