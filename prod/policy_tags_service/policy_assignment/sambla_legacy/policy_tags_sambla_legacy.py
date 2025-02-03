import json
import os
import yaml
from google.cloud import bigquery
import pandas as pd

def load_config():
    with open('../../config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    return config

client = bigquery.Client()

config = load_config()

project_id = config['prod']['policy_tags_service']['raw_layer_project']
dataset_id = config['prod']['policy_tags_service']['dataset_id']
policy_tags_table = config['prod']['policy_tags_service']['policy_tags_table']

relative_output_directory = config['prod']['policy_tags_service']['output_directory']
output_directory = os.path.abspath(relative_output_directory)

os.makedirs(output_directory, exist_ok=True)

def export_schemas():
    tables = client.list_tables(dataset_id)
    
    for table in tables:
        table_ref = client.dataset(dataset_id).table(table.table_id)
        schema = client.get_table(table_ref).schema
        
        schema_dict = [
            convert_schema_field_to_dict(field) for field in schema
        ]
        
        table_schema_path = os.path.join(output_directory, f"{table.table_id}_schema.json")
        with open(table_schema_path, 'w') as f:
            json.dump(schema_dict, f, indent=2)
        
        print(f"Schema for table '{table.table_id}' exported successfully.")

def convert_schema_field_to_dict(field):
    field_dict = {
        "name": field.name,
        "type": field.field_type,
        "mode": field.mode
    }
    if field.field_type == "RECORD":
        field_dict["fields"] = [convert_schema_field_to_dict(f) for f in field.fields]
    return field_dict

def fetch_policy_tags():
    query = f"""
    SELECT taxonomy_id, display_name, policy_tag_id
    FROM `{project_id}.{policy_tags_table}`
    """
    query_job = client.query(query)
    result = query_job.result()

    rows = [{"display_name": row["display_name"], "policy_tag_id": row["policy_tag_id"], "taxonomy_id": row["taxonomy_id"]} for row in result]
    
    policy_tags = pd.DataFrame(rows)
    
    return policy_tags

def normalize_name(name):
    """Normalize the name by converting to lowercase and removing underscores."""
    return name.lower().replace("_", "")

def match_policy_tags_to_fields(fields, policy_tags, parent_name=None):
    updated_fields = []
    print(fields)
    
    for field in fields:
        field_name = field["name"]
        normalized_field_name = normalize_name(field_name)
        
        if field.get('fields'):
            nested_fields = match_policy_tags_to_fields(field["fields"], policy_tags, parent_name=None if field["name"] == "invoices" else parent_name)
            field["fields"] = nested_fields
        else:
            # Normalize the policy tags display names for comparison
            policy_tags['normalized_display_name'] = policy_tags['display_name'].apply(normalize_name)
            matching_tag = policy_tags[policy_tags['normalized_display_name'] == normalized_field_name]
            
            if not matching_tag.empty:
                policy_tag = matching_tag['policy_tag_id'].values[0]
                field["policyTags"] = {
                    "names": [
                        f"projects/{project_id}/locations/europe-north1/taxonomies/6126692965998272750/policyTags/{policy_tag}"
                    ]
                }
        
        if parent_name and not field.get('name').startswith(f"{parent_name}."):
            field["name"] = f"{parent_name}.{field['name']}"
        
        updated_fields.append(field)
    
    return updated_fields

def update_schema_with_policy_tags(schemas, policy_tags):
    updated_schemas = {}

    for table_name, schema in schemas.items():
        updated_fields = match_policy_tags_to_fields(schema, policy_tags)
        updated_schemas[table_name] = updated_fields
    
    return updated_schemas

def save_updated_schema_for_each_table(updated_schemas):
    for table_name, updated_fields in updated_schemas.items():
        table_schema_path = os.path.join(output_directory, f"{table_name}_schema.json")
        with open(table_schema_path, 'w') as f:
            json.dump(updated_fields, f, indent=2)

        print(f"Updated schema for table '{table_name}' saved successfully.")

def main():
    export_schemas()
    
    schemas = {}
    for table in os.listdir(output_directory):
        if table.endswith("_schema.json"):
            with open(os.path.join(output_directory, table), 'r') as f:
                schemas[table.replace('_schema.json', '')] = json.load(f)

    policy_tags = fetch_policy_tags()

    updated_schemas = update_schema_with_policy_tags(schemas, policy_tags)

    save_updated_schema_for_each_table(updated_schemas)

if __name__ == "__main__":
    main()
