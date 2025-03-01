import json
import os
import yaml
from google.cloud import bigquery
import pandas as pd

class BigQuerySchemaExporter:
    def __init__(self, config_file='config.yml'):
        self.config = self.load_config(config_file)
        self.client = bigquery.Client()
        self.output_directory = os.path.abspath(self.config["output_directory"])
        os.makedirs(self.output_directory, exist_ok=True)
        self.schema_file_path = os.path.join(self.output_directory, "all_schemas.json")

    def load_config(self, config_file):
        """Load configuration from the YAML file."""
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)

    def flatten_fields(self, fields, parent_name=""):
        """Flatten nested fields into a single-tier structure with dot notation and filter by policy tags."""
        flattened = []
        for field in fields:
            full_name = f"{parent_name}.{field['name']}" if parent_name else field["name"]
            policy_tags = field.get("policyTags", {}).get("names", [])
            if policy_tags:
                flattened.append({
                    "name": full_name,
                    "description": field.get("description", ""),
                    "policy_tags": policy_tags,
                })
            if "fields" in field and field["fields"]:
                flattened.extend(self.flatten_fields(field["fields"], full_name))
        return flattened

    def convert_schema_field_to_dict(self, field):
        """Convert a BigQuery schema field to a dictionary, skipping certain data types."""
        field_dict = {
            "name": field.name,
            "type": field.field_type,
            "mode": field.mode,
        }
        if field.field_type in ['NUMERIC', 'BOOL', 'INT64', 'FLOAT64']:
            return None
        if field.field_type == "RECORD":
            field_dict["fields"] = [self.convert_schema_field_to_dict(subfield) for subfield in field.fields]
        return field_dict

    def export_schemas(self):
        """Export schemas for all tables with the specified suffix."""
        dataset_ref = self.client.dataset(self.config["target_dataset"], project=self.config["target_project"])
        tables = self.client.list_tables(dataset_ref)
        schemas = {}

        for table in tables:
            if table.table_type == "TABLE" and table.table_id.endswith(self.config["table_suffix"]):
                table_ref = dataset_ref.table(table.table_id)
                schema = self.client.get_table(table_ref).schema
                schema_dict = [self.convert_schema_field_to_dict(field) for field in schema]
                schemas[table.table_id] = schema_dict

        with open(self.schema_file_path, 'w') as f:
            json.dump(schemas, f, indent=2)
        print(f"Schemas exported successfully to {self.schema_file_path}")

    def fetch_policy_tags(self):
        """Fetch policy tags metadata and create a policy_tag_mapping."""
        query = f"""
        SELECT t1.taxonomy_id, t1.display_name, t1.policy_tag_id, t2.taxonomy_display_name AS taxonomy_name
        FROM `{self.config["source_project"]}.{self.config["source_dataset"]}.{self.config["metadata_table"]}` t1
        JOIN `{self.config["source_project"]}.{self.config["source_dataset"]}.{self.config["taxonomy_table"]}` t2
        ON t1.taxonomy_id = t2.id
        WHERE taxonomy_id IN ('7698000960465061299', '8248486934170083143', '655384675748637071')
        """
        query_job = self.client.query(query)
        result = query_job.result()

        rows = [{"taxonomy_id": row["taxonomy_id"],
                 "display_name": row["display_name"],
                 "policy_tag_id": row["policy_tag_id"],
                 "taxonomy_name": row["taxonomy_name"]} for row in result]
        
        policy_metadata = pd.DataFrame(rows)

        # Create policy_tag_mapping for taxonomy_name and corresponding tag reference
        policy_tag_mapping = {}
        for _, row in policy_metadata.iterrows():
            tag_name = self.normalize_name(row["display_name"])
            tag_suffix = row["policy_tag_id"]
            policy_tag_name = row["taxonomy_name"]

            tag_prefix = f'{{{{var("policy_tag_{policy_tag_name}")}}}}/'
            tag_link = f"{tag_prefix}{tag_suffix}"

            policy_tag_mapping[tag_name] = tag_link

        return policy_tag_mapping
    
    def normalize_name(self, name):
        """Normalize the name by converting to lowercase and removing underscores."""
        return name.lower().replace("_", "")

    def match_policy_tags_to_fields(self, fields, policy_tag_mapping, parent_name=None):
        """Match policy tags to schema fields."""
        updated_fields = []

        for field in fields:
            if not field:
                continue

            field_name = field["name"]
            field_type = field["type"]
            normalized_field_name = self.normalize_name(field_name)
            

            if field.get('fields'):
                nested_fields = self.match_policy_tags_to_fields(
                    field["fields"], policy_tag_mapping, parent_name=None if field["name"] == "invoices" else parent_name
                )
                field["fields"] = nested_fields
            else:
                # Explicitly check for ssn and balance when they are not of type STRING
                if (normalized_field_name == "ssn" or normalized_field_name == "nationalid" or normalized_field_name == "validnationalid") and field_type != "STRING" and field_type != "BOOL":
                    field["policyTags"] = {
                        "names": [
                            "{{var(\"policy_tag_gdpr_compliance_measures_high\")}}/8190767684129261300"
                        ]
                    }
                elif normalized_field_name == "phonenumber" and field_type != "STRING":
                    field["policyTags"] = {
                        "names": [
                            "{{var(\"policy_tag_gdpr_compliance_measures_high\")}}/1553289368757892144"
                        ]
                    }

                else:
                    if normalized_field_name in policy_tag_mapping:
                        tag_link = policy_tag_mapping[normalized_field_name]
                        field["policyTags"] = {"names": [tag_link]}

            if parent_name and not field.get('name').startswith(f"{parent_name}."):
                field["name"] = f"{parent_name}.{field['name']}"

            updated_fields.append(field)

        return updated_fields

    def update_schema_with_policy_tags(self, schemas, policy_tag_mapping):
        """Update schema JSON with matched policy tags."""
        updated_schemas = {}

        for table_name, schema in schemas.items():
            updated_fields = self.match_policy_tags_to_fields(schema, policy_tag_mapping)
            updated_schemas[table_name] = updated_fields

        return updated_schemas

    def convert_to_dbt_format(self, updated_schemas):
        """Convert updated schema JSON to DBT YAML format, only including fields with policy tags."""
        dbt_models = []

        for table_name, fields in updated_schemas.items():
            flattened_fields = self.flatten_fields(fields)
            if flattened_fields:  
                model = {
                    "name": table_name,
                    "description": "",
                    "columns": flattened_fields,
                }
                dbt_models.append(model)

        return {"version": 2, "models": dbt_models}

    def save_dbt_schema(self, dbt_schema):
        """Save the DBT schema YAML."""
        dbt_schema_path = os.path.join(self.output_directory, self.config["policy_tags_file"])

        with open(dbt_schema_path, 'w') as f:
            yaml.dump(dbt_schema, f, sort_keys=False, default_flow_style=False, allow_unicode=True)

        print(f"DBT schema YAML saved successfully to {dbt_schema_path}")

    def run(self):
        """Main execution method."""
        
        self.export_schemas()
        with open(self.schema_file_path, 'r') as f:
            schemas = json.load(f)

        policy_tag_mapping = self.fetch_policy_tags()

        updated_schemas = self.update_schema_with_policy_tags(schemas, policy_tag_mapping)

        dbt_schema = self.convert_to_dbt_format(updated_schemas)

        self.save_dbt_schema(dbt_schema)


if __name__ == "__main__":
    exporter = BigQuerySchemaExporter()
    exporter.run()
