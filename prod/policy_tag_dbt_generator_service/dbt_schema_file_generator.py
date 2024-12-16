import json
import os
import yaml
from google.cloud import bigquery
import pandas as pd

def load_config():
    """Load configuration from the YAML file."""
    with open('config.yml', 'r') as file:
        return yaml.safe_load(file)

# Load configuration
config = load_config()

# BigQuery client
client = bigquery.Client()

# Output directory
output_directory = os.path.abspath(config["output_directory"])
os.makedirs(output_directory, exist_ok=True)

schema_file_path = os.path.join(output_directory, "all_schemas.json")

def flatten_fields(fields, parent_name=""):
    """Flatten nested fields into a single-tier structure with dot notation and filter by policy tags."""
    flattened = []
    for field in fields:
        # Construct the full field name with dot notation
        full_name = f"{parent_name}.{field['name']}" if parent_name else field["name"]

        # Check if the field has policy tags
        policy_tags = field.get("policyTags", {}).get("names", [])
        if policy_tags:
            flattened.append({
                "name": full_name,
                "description": field.get("description", ""),
                "policy_tags": policy_tags,
            })

        # Process nested fields (if any) recursively
        if "fields" in field and field["fields"]:
            flattened.extend(flatten_fields(field["fields"], full_name))

    return flattened

def convert_schema_field_to_dict(field):
    """Convert a BigQuery schema field to a dictionary, skipping certain data types."""
    field_dict = {
        "name": field.name,
        "type": field.field_type,
        "mode": field.mode,
    }

    # Skip fields with unwanted data types
    if field.field_type in ['NUMERIC', 'BOOL', 'INT64', 'FLOAT64']:
        return None

    if field.field_type == "RECORD":
        field_dict["fields"] = [convert_schema_field_to_dict(subfield) for subfield in field.fields]
    
    return field_dict

def export_schemas():
    """Export schemas for all tables with the specified suffix."""
    dataset_ref = client.dataset(config["target_dataset"], project=config["target_project"])
    tables = client.list_tables(dataset_ref)
    schemas = {}

    for table in tables:
        if table.table_type == "TABLE" and table.table_id.endswith(config["table_suffix"]):
            table_ref = dataset_ref.table(table.table_id)
            schema = client.get_table(table_ref).schema
            schema_dict = [convert_schema_field_to_dict(field) for field in schema]
            schemas[table.table_id] = schema_dict

    # Save schemas to a single JSON file
    with open(schema_file_path, 'w') as f:
        json.dump(schemas, f, indent=2)
    print(f"Schemas exported successfully to {schema_file_path}")


def fetch_policy_tags():
    """Fetch policy tags metadata and create a policy_tag_mapping."""
    query = f"""
    SELECT t1.taxonomy_id, t1.display_name, t1.policy_tag_id, t2.taxonomy_display_name AS taxonomy_name
    FROM `{config["source_project"]}.{config["source_dataset"]}.{config["metadata_table"]}` t1
    JOIN `{config["source_project"]}.{config["source_dataset"]}.{config["taxonomy_table"]}` t2
    ON t1.taxonomy_id = t2.id
    """
    query_job = client.query(query)
    result = query_job.result()

    rows = [{"taxonomy_id": row["taxonomy_id"], 
             "display_name": row["display_name"], 
             "policy_tag_id": row["policy_tag_id"], 
             "taxonomy_name": row["taxonomy_name"]} for row in result]
    
    policy_metadata = pd.DataFrame(rows)

    # Create policy_tag_mapping for taxonomy_name and corresponding tag reference
    policy_tag_mapping = {}
    for _, row in policy_metadata.iterrows():
        tag_name = row["display_name"].lower()  # Normalize to lowercase
        tag_suffix = row["policy_tag_id"]
        policy_tag_name = row["taxonomy_name"]

        # Use the pre-defined policy_tag_reference as a prefix
        tag_prefix = f'{{{{var("policy_tag_{policy_tag_name}")}}}}/'
        tag_link = f"{tag_prefix}{tag_suffix}"

        policy_tag_mapping[tag_name] = tag_link

    return policy_tag_mapping

def match_policy_tags_to_fields(fields, policy_tag_mapping, parent_name=None):
    """Match policy tags to schema fields."""
    updated_fields = []

    for field in fields:
        # Skip fields with unwanted data types (already filtered in convert_schema_field_to_dict)
        if not field:
            continue

        field_name = field["name"]

        if field.get('fields'):  # For nested RECORD fields
            nested_fields = match_policy_tags_to_fields(
                field["fields"], policy_tag_mapping, parent_name=None if field["name"] == "invoices" else parent_name
            )
            field["fields"] = nested_fields
        else:  # For scalar fields
            # Check if the field name matches any tag name in the policy_tag_mapping
            field_name_lower = field_name.lower()  # Normalize to lowercase
            if field_name_lower in policy_tag_mapping:
                tag_link = policy_tag_mapping[field_name_lower]
                field["policyTags"] = {"names": [tag_link]}

        if parent_name and not field.get('name').startswith(f"{parent_name}."):
            field["name"] = f"{parent_name}.{field['name']}"

        updated_fields.append(field)

    return updated_fields

def update_schema_with_policy_tags(schemas, policy_tag_mapping):
    """Update schema JSON with matched policy tags."""
    updated_schemas = {}

    for table_name, schema in schemas.items():
        updated_fields = match_policy_tags_to_fields(schema, policy_tag_mapping)
        updated_schemas[table_name] = updated_fields

    return updated_schemas

def convert_to_dbt_format(updated_schemas):
    """Convert updated schema JSON to DBT YAML format, only including fields with policy tags."""
    dbt_models = []

    for table_name, fields in updated_schemas.items():
        # Flatten fields and filter out those without policy tags
        flattened_fields = flatten_fields(fields)
        if flattened_fields:  # Only include tables with at least one field with policy tags
            model = {
                "name": table_name,
                "description": "",
                "columns": flattened_fields,
            }
            dbt_models.append(model)

    return {"version": 2, "models": dbt_models}

def save_dbt_schema(dbt_schema):
    """Save the DBT schema YAML."""
    dbt_schema_path = os.path.join(output_directory, config["policy_tags_file"])

    with open(dbt_schema_path, 'w') as f:
        yaml.dump(dbt_schema, f, sort_keys=False, default_flow_style=False, allow_unicode=True)

    print(f"DBT schema YAML saved successfully to {dbt_schema_path}")

def main():
    # Step 1: Export schemas to a single file
    export_schemas()

    # Step 2: Load schemas
    with open(schema_file_path, 'r') as f:
        schemas = json.load(f)

    # Step 3: Fetch policy tags and create policy_tag_mapping
    policy_tag_mapping = fetch_policy_tags()

    # Step 4: Update schemas with policy tags
    updated_schemas = update_schema_with_policy_tags(schemas, policy_tag_mapping)

    # Step 5: Convert to DBT format
    dbt_schema = convert_to_dbt_format(updated_schemas)

    # Step 6: Save DBT schema YAML
    save_dbt_schema(dbt_schema)

if __name__ == "__main__":
    main()
