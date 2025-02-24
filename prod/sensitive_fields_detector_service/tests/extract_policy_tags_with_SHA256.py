import json
import csv
import os

def extract_sha256_fields_from_file(file_path, output_csv):
    sha256_fields = []

    def traverse(node, tag_name=None, category=None, parent_field=None):
        if isinstance(node, dict):
            # Check if 'masking_rule' is SHA256 for the current node (field)
            if node.get("masking_rule") == "SHA256":
                field_name = node.get("name", parent_field)  # Default to parent field if name is missing
                # Add the results as (tag_name, category, field_name)
                if tag_name and category:
                    sha256_fields.append((tag_name, category, field_name))  
                else:
                    sha256_fields.append(("", "", field_name))  # Handle cases where tag_name/category might be empty
            
            # Traverse child fields (recursively) under 'children' or any nested fields
            for key, value in node.items():
                if isinstance(value, dict):
                    if key == "children":
                        # For children, iterate over them and set their names
                        for child_key, child_value in value.items():
                            child_value["name"] = child_key  # Store field name
                            traverse(child_value, tag_name, category, parent_field)  # Recursively traverse child fields
                    else:
                        traverse(value, tag_name, category, parent_field)  # Continue traversal on other dictionary values
                # Special cases for categories like PII, restricted, etc.
                elif key in ["PII", "restricted", "confidential", "low_sensitivity_tags_prod", "medium_sensitivity_tags_prod", "high_sensitivity_tags_prod"]:
                    traverse(value, tag_name, key, key)  # Pass the category as the parent_field

    # Load JSON data from file
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    # Traverse each category under different tags
    traverse(data.get("high_sensitivity_tags_prod", {}), "high_sensitivity_tags_prod", "high_sensitivity_tags_prod")
    traverse(data.get("medium_sensitivity_tags_prod", {}), "medium_sensitivity_tags_prod", "medium_sensitivity_tags_prod")
    traverse(data.get("low_sensitivity_tags_prod", {}), "low_sensitivity_tags_prod", "low_sensitivity_tags_prod")
    traverse(data.get("confidential", {}), "", "confidential", "confidential")
    traverse(data.get("restricted", {}), "", "restricted", "restricted")
    traverse(data.get("PII", {}), "", "PII", "PII")

    # Write results to a CSV file
    with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Tag Name", "Category", "Field Name"])  # Write the header row
        writer.writerows(sha256_fields)  # Write all found SHA256 fields


# Get the current directory where the script is located
current_dir = os.path.dirname(os.path.abspath(__file__))
schema_dir = current_dir.replace("tests","schemas")

# Assuming 'prod/tests' is the relative path from the script's location
file_path = os.path.join(schema_dir, "policy_tags/sensitive_fields_updated.json")
output_csv = os.path.join(current_dir, "outputs/sha256_fields.csv")
extract_sha256_fields_from_file(file_path, output_csv)
