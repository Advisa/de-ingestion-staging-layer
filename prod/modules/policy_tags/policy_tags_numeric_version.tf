
locals {
  # Load JSON configuration for policy tags
  policy_tags_config = jsondecode(file("schemas/policy_tags/sensitive_fields_output.json"))

  # Flatten tags for high sensitivity
  high_sensitivity_tags = flatten([
    for category, category_config in local.policy_tags_config["high_sensitivity_tags"] : [
      for parent, config in category_config : [
        for child, child_config in config["children"] : {
          key          = "${tostring(category)}-${tostring(parent)}-${tostring(child)}"
          category     = category
          parent       = parent
          child        = child
          sensitivity  = "high"
          masking_rule = child_config["masking_rule"]
        }
      ]
    ]
  ])

  # Flatten tags for medium sensitivity
  medium_sensitivity_tags = flatten([
    for category, category_config in local.policy_tags_config["medium_sensitivity_tags"] : [
      for parent, config in category_config : [
        for child, child_config in config["children"] : {
          key          = "${tostring(category)}-${tostring(parent)}-${tostring(child)}"
          category     = category
          parent       = parent
          child        = child
          sensitivity  = "medium"
          masking_rule = child_config["masking_rule"]
        }
      ]
    ]
  ])

  # Flatten tags for low sensitivity
  low_sensitivity_tags = flatten([
    for category, category_config in local.policy_tags_config["low_sensitivity_tags"] : [
      for parent, config in category_config : [
        for child, child_config in config["children"] : {
          key          = "${tostring(category)}-${tostring(parent)}-${tostring(child)}"
          category     = category
          parent       = parent
          child        = child
          sensitivity  = "low"
          masking_rule = child_config["masking_rule"]
        }
      ]
    ]
  ])
}

# --------------------
# Taxonomy Resources
# --------------------
# High Sensitivity Taxonomy
resource "google_data_catalog_taxonomy" "high_sensitivity_taxonomy" {
  display_name           = "${local.policy_tags_config["taxonomy_name"]}_high"
  description            = "Taxonomy for high sensitivity data"
  project                = var.project_id
  region                 = var.region
  activated_policy_types = ["FINE_GRAINED_ACCESS_CONTROL"]
}

# Medium Sensitivity Taxonomy
resource "google_data_catalog_taxonomy" "medium_sensitivity_taxonomy" {
  display_name           = "${local.policy_tags_config["taxonomy_name"]}_medium"
  description            = "Taxonomy for medium sensitivity data"
  project                = var.project_id
  region                 = var.region
  activated_policy_types = ["FINE_GRAINED_ACCESS_CONTROL"]
}

# Low Sensitivity Taxonomy
resource "google_data_catalog_taxonomy" "low_sensitivity_taxonomy" {
  display_name           = "${local.policy_tags_config["taxonomy_name"]}_low"
  description            = "Taxonomy for low sensitivity data"
  project                = var.project_id
  region                 = var.region
  activated_policy_types = ["FINE_GRAINED_ACCESS_CONTROL"]
}

# ---------------------
# Category Tags
# ---------------------
# High Sensitivity Categories
resource "google_data_catalog_policy_tag" "high_category_tags" {
  for_each     = tomap(local.policy_tags_config["high_sensitivity_tags"])
  taxonomy     = google_data_catalog_taxonomy.high_sensitivity_taxonomy.id
  display_name = each.key

  lifecycle {
    ignore_changes = [display_name]
  }
}

# Medium Sensitivity Categories
resource "google_data_catalog_policy_tag" "medium_category_tags" {
  for_each     = tomap(local.policy_tags_config["medium_sensitivity_tags"])
  taxonomy     = google_data_catalog_taxonomy.medium_sensitivity_taxonomy.id
  display_name = each.key

  lifecycle {
    ignore_changes = [display_name]
  }
}

# Low Sensitivity Categories
resource "google_data_catalog_policy_tag" "low_category_tags" {
  for_each     = tomap(local.policy_tags_config["low_sensitivity_tags"])
  taxonomy     = google_data_catalog_taxonomy.low_sensitivity_taxonomy.id
  display_name = each.key

  lifecycle {
    ignore_changes = [display_name]
  }
}

# ---------------------
# Parent Tags
# ---------------------
# High Sensitivity Parents
resource "google_data_catalog_policy_tag" "high_parent_tags" {
  for_each = {
    for parent_tag in flatten([
      for category, category_config in local.policy_tags_config["high_sensitivity_tags"] : [
        for parent, config in category_config : {
          key       = "${category}-${parent}"
          category  = category
          parent    = parent
        }
      ]
    ]) : parent_tag.key => parent_tag
  }

  taxonomy          = google_data_catalog_taxonomy.high_sensitivity_taxonomy.id
  display_name      = each.value.parent
  parent_policy_tag = google_data_catalog_policy_tag.high_category_tags[each.value.category].id

  lifecycle {
    ignore_changes = [display_name]
  }

  depends_on = [google_data_catalog_policy_tag.high_category_tags]
}


# Medium Sensitivity Parents
resource "google_data_catalog_policy_tag" "medium_parent_tags" {
  for_each = {
    for parent_tag in flatten([
      for category, category_config in local.policy_tags_config["medium_sensitivity_tags"] : [
        for parent, config in category_config : {
          key       = "${category}-${parent}"
          category  = category
          parent    = parent
        }
      ]
    ]) : parent_tag.key => parent_tag
  }

  taxonomy          = google_data_catalog_taxonomy.medium_sensitivity_taxonomy.id
  display_name      = each.value.parent
  parent_policy_tag = google_data_catalog_policy_tag.medium_category_tags[each.value.category].id

  lifecycle {
    ignore_changes = [display_name]
  }

  depends_on = [google_data_catalog_policy_tag.medium_category_tags]
}


# Low Sensitivity Parents
resource "google_data_catalog_policy_tag" "low_parent_tags" {
  for_each = {
    for parent_tag in flatten([
      for category, category_config in local.policy_tags_config["low_sensitivity_tags"] : [
        for parent, config in category_config : {
          key       = "${category}-${parent}"
          category  = category
          parent    = parent
        }
      ]
    ]) : parent_tag.key => parent_tag
  }

  taxonomy          = google_data_catalog_taxonomy.low_sensitivity_taxonomy.id
  display_name      = each.value.parent
  parent_policy_tag = google_data_catalog_policy_tag.low_category_tags[each.value.category].id

  lifecycle {
    ignore_changes = [display_name]
  }

  depends_on = [google_data_catalog_policy_tag.low_category_tags]
}


# ---------------------
# Child Tags
# ---------------------
# High Sensitivity Children
resource "google_data_catalog_policy_tag" "high_child_tags" {
  for_each = { for tag in local.high_sensitivity_tags : tag.key => tag }

  taxonomy          = google_data_catalog_taxonomy.high_sensitivity_taxonomy.id
  display_name      = each.value.child
  parent_policy_tag = google_data_catalog_policy_tag.high_parent_tags["${each.value.category}-${each.value.parent}"].id
  description = "Masking Rule: ${each.value.masking_rule}"
  
  lifecycle {
    ignore_changes = [display_name]
  }
}

# Medium Sensitivity Children
resource "google_data_catalog_policy_tag" "medium_child_tags" {
  for_each = { for tag in local.medium_sensitivity_tags : tag.key => tag }

  taxonomy          = google_data_catalog_taxonomy.medium_sensitivity_taxonomy.id
  display_name      = each.value.child
  parent_policy_tag = google_data_catalog_policy_tag.medium_parent_tags["${each.value.category}-${each.value.parent}"].id

  lifecycle {
    ignore_changes = [display_name]
  }
}

# Low Sensitivity Children
resource "google_data_catalog_policy_tag" "low_child_tags" {
  for_each = { for tag in local.low_sensitivity_tags : tag.key => tag }

  taxonomy          = google_data_catalog_taxonomy.low_sensitivity_taxonomy.id
  display_name      = each.value.child
  parent_policy_tag = google_data_catalog_policy_tag.low_parent_tags["${each.value.category}-${each.value.parent}"].id

  lifecycle {
    ignore_changes = [display_name]
  }
}

# ---------------------
# Data Policies
# ---------------------
# Create data policy for high sensitivity tags (parents)
resource "google_bigquery_datapolicy_data_policy" "high_data_policy_parent" {
  for_each        = google_data_catalog_policy_tag.high_parent_tags
  
  location        = var.region
  data_policy_id  = "${replace(trimspace(each.key), "-", "_test")}"
  policy_tag      = each.value.name
  data_policy_type = "DATA_MASKING_POLICY"

  data_masking_policy {
     predefined_expression = "SHA256"
  }
}

# Medium Sensitivity Data Policy
resource "google_bigquery_datapolicy_data_policy" "medium_data_policy_parent" {
  for_each        = google_data_catalog_policy_tag.medium_parent_tags

  location        = var.region
  data_policy_id  = "${replace(trimspace(each.key), "-", "_test")}"
  policy_tag      = each.value.name
  data_policy_type = "DATA_MASKING_POLICY"

  data_masking_policy {
    predefined_expression = "DEFAULT_MASKING_VALUE"
  }
}

# Low Sensitivity Data Policy
resource "google_bigquery_datapolicy_data_policy" "low_data_policy_parent" {
  for_each        = google_data_catalog_policy_tag.low_parent_tags
  location        = var.region
  data_policy_id  = "${replace(trimspace(each.key), "-", "_test")}"
  policy_tag      = each.value.name
  data_policy_type = "DATA_MASKING_POLICY"

  data_masking_policy {
     predefined_expression = "DEFAULT_MASKING_VALUE"
  }
}

# output "child_masking_rule_map" {
#   value = {
#     for tag in local.medium_sensitivity_tags : tag.child => tag.masking_rule
#   }
# }

# output "child_masking_rule_lookup" {
#   value = {
#     for tag_key, tag_value in google_data_catalog_policy_tag.medium_child_tags : tag_key => lookup(
#       {
#         for tag in local.high_sensitivity_tags : tag.child => tag.masking_rule
#       },
#       tag_value.display_name,
#       "DEFAULT_MASKING_VALUE"
#     )
#   }
# }


# Create data policy for high sensitivity tags (children)
 resource "google_bigquery_datapolicy_data_policy" "high_data_policy_child" {
   for_each        = google_data_catalog_policy_tag.high_child_tags
 
   location        = var.region
   data_policy_id  = "${replace(trimspace(each.key), "-", "_test")}"
   policy_tag      = each.value.name
   data_policy_type = "DATA_MASKING_POLICY"
   data_masking_policy {
    predefined_expression = lookup(
      {
        for tag in local.high_sensitivity_tags : tag.child => tag.masking_rule
      },
      each.value.display_name,
      "SHA256"
    )
  }
 }

 # Create data policy for high sensitivity tags (children)
 resource "google_bigquery_datapolicy_data_policy" "medium_data_policy_child" {
   for_each        = google_data_catalog_policy_tag.medium_child_tags
 
   location        = var.region
   data_policy_id  = "${replace(trimspace(each.key), "-", "_test")}"
   policy_tag      = each.value.name
   data_policy_type = "DATA_MASKING_POLICY"
   data_masking_policy {
    predefined_expression = lookup(
      {
        for tag in local.medium_sensitivity_tags : tag.child => tag.masking_rule
      },
      each.value.display_name,
      "DEFAULT_MASKING_VALUE"
    )
  }
 }

  # Create data policy for high sensitivity tags (children)
 resource "google_bigquery_datapolicy_data_policy" "low_data_policy_child" {
   for_each        = google_data_catalog_policy_tag.low_child_tags
 
   location        = var.region
   data_policy_id  = "${replace(trimspace(each.key), "-", "_test")}"
   policy_tag      = each.value.name
   data_policy_type = "DATA_MASKING_POLICY"
   data_masking_policy {
    predefined_expression = lookup(
      {
        for tag in local.low_sensitivity_tags : tag.child => tag.masking_rule
      },
      each.value.display_name,
      "DEFAULT_MASKING_VALUE"
    )
  }
 }