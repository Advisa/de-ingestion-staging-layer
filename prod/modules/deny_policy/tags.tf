# Create a Tag Key
resource "google_tags_tag_key" "tag_key" {
  short_name = "gdpr_complaince_tag"
  description  = "This Tag key is for tagging datasets and tables for GDPR purposes"
  parent       = "projects/${var.project_id}"
}

# Create a Tag Value for the created Tag Key
resource "google_tags_tag_value" "tag_value" {
  parent = "tagKeys/${google_tags_tag_key.tag_key.name}"
  short_name   = "gdpr_complaince_5year"
  description = "This Tag value is for tagging datasets and tables for GDPR purposes"
}