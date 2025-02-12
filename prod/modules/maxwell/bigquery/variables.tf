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

variable "sql_templates_maxwell" {
  description = "List of SQL template files"
  type        = list(string)
  default     = [
    "applicant_drafts_sgmw_p.sql",
    "applicants_sgmw_p.sql",
    "bid_accepts_sgmw_p.sql",
    "bid_additional_requirements_sgmw_p.sql",
    "bid_logs_sgmw_p.sql",
    "bids_sgmw_p.sql",
    "cookie_mappings_sgmw_p.sql",
    "cookies_sgmw_p.sql",
    "credit_report_latest_inquiries_sgmw_p.sql",
    "credit_report_xml_extract_sgmw_p.sql",
    "credit_reports_sgmw_p.sql",
    "creditor_products_sgmw_p.sql",
    "creditors_sgmw_p.sql",
    "current_loan_drafts_sgmw_p.sql",
    "current_loans_sgmw_p.sql",
    "customers_sgmw_p.sql",
    "invite_logs_sgmw_p.sql",
    "invites_sgmw_p.sql",
    "loan_application_drafts_sgmw_p.sql",
    "loan_application_versions_sgmw_p.sql",
    "loan_applications_sgmw_p.sql",
    "pageviews_sgmw_p.sql",
    "policies_sgmw_p.sql",
    "query_params_sgmw_p.sql",
    "sent_events_sgmw_p.sql",
  ]
}