import json
import os
from google.cloud import bigquery
import pandas as pd

# Initialize BigQuery client
client = bigquery.Client()

# Define your project, dataset, and policy tags table
project_id = 'sambla-data-staging-compliance'
dataset_id = 'sambla_legacy_integration_legacy'
policy_tags_table = 'policy_tags_metadata.policy_tags'

# Directory where the schemas will be saved
output_directory = '/Users/aruldharani/Sambla/de-ingestion-staging-layer-1/prod/schemas/sambla_legacy/'

# Ensure the directory exists
os.makedirs(output_directory, exist_ok=True)

# Step 1: Export all schemas for tables in the dataset
def export_schemas():
    tables = client.list_tables(dataset_id)
    
    # Process each table schema
    for table in tables:
        table_ref = client.dataset(dataset_id).table(table.table_id)
        schema = client.get_table(table_ref).schema
        
        # Convert SchemaField objects to dictionaries
        schema_dict = [
            convert_schema_field_to_dict(field) for field in schema
        ]
        
        # Save each table's schema to its own JSON file
        table_schema_path = os.path.join(output_directory, f"{table.table_id}_schema.json")
        with open(table_schema_path, 'w') as f:
            json.dump(schema_dict, f, indent=2)
        
        print(f"Schema for table '{table.table_id}' exported successfully.")

# Helper function to convert SchemaField objects to dictionaries
def convert_schema_field_to_dict(field):
    field_dict = {
        "name": field.name,
        "type": field.field_type,
        "mode": field.mode
    }
    if field.field_type == "RECORD":
        field_dict["fields"] = [convert_schema_field_to_dict(f) for f in field.fields]
    return field_dict

# Step 2: Fetch the policy tags from the policy_tags table
def fetch_policy_tags():
    query = f"""
    SELECT taxonomy_id, display_name, policy_tag_id
    FROM `{project_id}.{policy_tags_table}`
    """
    query_job = client.query(query)
    result = query_job.result()

    # Fetch the rows and convert them into a list of dictionaries
    rows = [{"display_name": row["display_name"], "policy_tag_id": row["policy_tag_id"], "taxonomy_id": row["taxonomy_id"]} for row in result]
    
    # Create a DataFrame from the list of dictionaries
    policy_tags = pd.DataFrame(rows)
    
    return policy_tags

# Step 3: Recursively match fields in the schema against policy tags and add policyTags if a match is found
def match_policy_tags_to_fields(fields, policy_tags, parent_name=None):
    updated_fields = []
    
    for field in fields:
        field_name = field["name"]
        
        # If the field is a RECORD type, we need to process nested fields
        if field.get('fields'):
            # If it's a nested record, we process recursively, but don't add parent name for `REPEATED` fields
            nested_fields = match_policy_tags_to_fields(field["fields"], policy_tags, parent_name=None if field["name"] == "invoices" else parent_name)
            field["fields"] = nested_fields
        else:
            # Check if the field name matches any display name in the policy tags
            matching_tag = policy_tags[policy_tags['display_name'] == field_name]
            if not matching_tag.empty:
                # Add policyTags to the field
                policy_tag = matching_tag['policy_tag_id'].values[0]
                field["policyTags"] = {
                    "names": [
                        f"projects/{project_id}/locations/europe-north1/taxonomies/6126692965998272750/policyTags/{policy_tag}"
                    ]
                }
        
        # If there's a parent field, add it to the name (but only once for non-repeated fields)
        if parent_name and not field.get('name').startswith(f"{parent_name}."):
            field["name"] = f"{parent_name}.{field['name']}"
        
        updated_fields.append(field)
    
    return updated_fields

# Step 4: Update the schema with policy tags and process nested fields
def update_schema_with_policy_tags(schemas, policy_tags):
    updated_schemas = {}

    for table_name, schema in schemas.items():
        updated_fields = match_policy_tags_to_fields(schema, policy_tags)
        updated_schemas[table_name] = updated_fields
    
    return updated_schemas

# Step 5: Save the updated schema with policyTags into a JSON file for each table
def save_updated_schema_for_each_table(updated_schemas):
    for table_name, updated_fields in updated_schemas.items():
        table_schema_path = os.path.join(output_directory, f"{table_name}_schema.json")
        with open(table_schema_path, 'w') as f:
            json.dump(updated_fields, f, indent=2)

        print(f"Updated schema for table '{table_name}' saved successfully.")

# Main function to orchestrate the process
def main():
    # Export the schemas from BigQuery
    export_schemas()
    
    # Load the schemas from the exported files
    schemas = {}
    for table in os.listdir(output_directory):
        if table.endswith("_schema.json"):
            with open(os.path.join(output_directory, table), 'r') as f:
                schemas[table.replace('_schema.json', '')] = json.load(f)

    # Fetch policy tags
    policy_tags = fetch_policy_tags()

    # Update schemas with matching policy tags
    updated_schemas = update_schema_with_policy_tags(schemas, policy_tags)

    # Save the updated schemas for each table to a new JSON file
    save_updated_schema_for_each_table(updated_schemas)

if __name__ == "__main__":
    main()
