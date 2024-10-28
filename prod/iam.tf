# Initializing a service account for terraform to run and deploy resources in sambla-data-staging-compliance project effortlessly
resource "google_service_account" "sa_terraform_admin" {
  account_id   = "de-compliance-terraform-admin"
  display_name = "terraform-adm"
  description = "A service account for terraform to manage and create resources within GCP"
}
resource "google_project_iam_member" "project_permissions_owner" {
  project    = var.project_id
  role   = "roles/owner" # Grant permission to use the service account
  member = "serviceAccount:${google_service_account.sa_terraform_admin.email}"  # Grant access to the service account
  
}