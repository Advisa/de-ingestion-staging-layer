#create deny policy
resource "google_iam_deny_policy" "deny_policy_creation" {
  parent      = urlencode("cloudresourcemanager.googleapis.com/projects/${var.project_id}")  # Corrected parent format with urlencode
  name        = "gdpr-deny-policy"
  display_name = "GDPR deny policy"
  rules {
    description = "First rule"
    deny_rule {
      denied_principals = ["principal://goog/subject/duygu.genc@samblagroup.com","principal://goog/subject/adam.svenson@samblagroup.com"]  # Dynamic principal list
      denial_condition {
        title       = "denial condition expression"
        expression = "resource.matchTagId(\"tagKeys/281476893661836\", \"tagValues/281478644865264\")"
      }
      denied_permissions = ["bigquery.googleapis.com/datasets.get", "bigquery.googleapis.com/datasets.update", "bigquery.googleapis.com/tables.create", "bigquery.googleapis.com/tables.delete", "bigquery.googleapis.com/tables.get", "bigquery.googleapis.com/tables.update", "bigquery.googleapis.com/jobs.create", "bigquery.googleapis.com/datasets.create", "bigquery.googleapis.com/jobs.get", "bigquery.googleapis.com/jobs.list", "bigquery.googleapis.com/jobs.listAll", "bigquery.googleapis.com/jobs.delete", "bigquery.googleapis.com/jobs.update", "bigquery.googleapis.com/tables.createIndex", "bigquery.googleapis.com/tables.createSnapshot", "bigquery.googleapis.com/tables.createTagBinding", "bigquery.googleapis.com/tables.deleteIndex", "bigquery.googleapis.com/tables.deleteSnapshot", "bigquery.googleapis.com/tables.deleteTagBinding", "bigquery.googleapis.com/tables.export", "bigquery.googleapis.com/tables.getIamPolicy", "bigquery.googleapis.com/tables.updateData", "bigquery.googleapis.com/tables.getData"]  # Dynamic permissions list
    }
  }
}
