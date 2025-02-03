# Create a Tag Key
resource "google_tags_tag_key" "tag_key_prod" {
  short_name = "gdpr_complaince_tag_test"
  description  = "This Tag key is for tagging datasets and tables for GDPR purposes"
  parent       = "projects/${var.project_id}"
  lifecycle {
    prevent_destroy = true  # Prevents Terraform from destroying this resource
  }
}

# Create a Tag Value for the created Tag Key
resource "google_tags_tag_value" "tag_value_prod" {
  parent = "tagKeys/${google_tags_tag_key.tag_key_prod.name}"
  short_name   = "gdpr_complaince_5year_test"
  description = "This Tag value is for tagging datasets and tables for GDPR purposes"
  lifecycle {
    prevent_destroy = true  # Prevents Terraform from destroying this resource
  }
}