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

variable "sql_templates" {
  description = "List of SQL template files"
  type        = list(string)
  default     = [
    "applications_all_versions_sambq_p.sql",
    "applications_allpaidoutbysambla_sambq_p.sql",
    "applications_bids_sambq_p.sql",
    "applications_credit_reports_sambq_p.sql",
    "applications_customers_sambq_p.sql",
    "applications_estates_sambq_p.sql",
    "applications_excludebanks_sambq_p.sql",
    "applications_internalcomments_sambq_p.sql",
    "applications_invites_sambq_p.sql",
    #"applications_loans_sambq_p.sql",
    "applications_past_report_requests_sambq_p.sql",
    "applications_sambq_p.sql",
    "applications_scheduledcalls_sambq_p.sql",
    "applications_utmhistory_sambq_p.sql",
    "applications_version_sambq_p.sql",
    "banks_sambq_p.sql",
    "unsubscriptions_sambq_p.sql",
    "users_sambq_p.sql",
  ]
}