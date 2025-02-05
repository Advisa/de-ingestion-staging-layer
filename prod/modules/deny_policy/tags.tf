# Create a Tag Key
resource "google_tags_tag_key" "tag_key_prod_live" {
  short_name = "gdpr_complaince_tag_prod"
  description  = "This Tag key is for tagging datasets and tables for GDPR purposes"
  parent       = "projects/${var.data_domain_project_id}"
  lifecycle {
    prevent_destroy = true  # Prevents Terraform from destroying this resource
  }
}

# Create a Tag Value for the created Tag Key
resource "google_tags_tag_value" "tag_value_prod_live" {
  parent = "tagKeys/${google_tags_tag_key.tag_key_prod_live.name}"
  short_name   = "gdpr_complaince_5year_prod"
  description = "This Tag value is for tagging datasets and tables for GDPR purposes"
  lifecycle {
    prevent_destroy = true  # Prevents Terraform from destroying this resource
  }
}