import os
import json
import logging
from google.cloud import bigquery
import yaml
import glob
from pathlib import Path
import pandas as pd
import re

class PolicyAssignmentService:
    def __init__(self, config_path, env='dev'):
        self.config = self.load_config(config_path)
        self.env = env
        self.policy_assignment_config = self.config.get(self.env, self.config.get('default', {})).get('policy_tags_service', {})
        
        if not self.policy_assignment_config:
            logging.error(f"Configuration for environment '{env}' or 'default' not found.")
            raise ValueError(f"Configuration for environment '{env}' not found.")

        # Extract values from the configuration
        self.project_id = self.policy_assignment_config.get('raw_layer_project')
        self.base_folder = self.policy_assignment_config.get('base_folder')
        self.policy_tags_table = self.policy_assignment_config.get('policy_tags_table')

        # Initialize BigQuery clients using service account credentials
        self.client = self.initialize_bigquery_clients()

        # Initialize the current path where this service is located
        project_root = Path(__file__).resolve().parent.parent
        self.current_path = os.path.dirname(os.path.abspath(__file__))  
        self.base_path = project_root 

        self.schema_folder_path = project_root.parent / 'schemas'


        

    def load_config(self, config_path):
        """Load YAML configuration from the provided path."""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            logging.info("YAML configuration loaded successfully.")
            return config
        except FileNotFoundError as e:
            logging.error(f"Config file not found: {str(e)}")
            raise e
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML file: {str(e)}")
            raise e
        
    def initialize_bigquery_clients(self):
        """Initialize BigQuery Client."""
        try:
            client = bigquery.Client(project=self.project_id)
            return client
        except Exception as e:
            logging.error(f"Error initializing BigQuery clients: {str(e)}")
            raise e
        

    def fetch_policy_tags(self):
        query = f"""
        SELECT taxonomy_id, display_name, policy_tag_id
        FROM `{self.project_id}.{self.policy_tags_table}`
        WHERE taxonomy_id IN ('7698000960465061299', '8248486934170083143', '655384675748637071')
        """
        query_job = self.client.query(query)
        result = query_job.result()

        rows = [{"display_name": row["display_name"], "policy_tag_id": row["policy_tag_id"], "taxonomy_id": row["taxonomy_id"]} for row in result]

        policy_tags = pd.DataFrame(rows)

        return policy_tags

    def normalize_name(self, name):
        """Normalize the name by converting to lowercase and removing underscores."""
        return name.lower().replace("_", "")

    def match_policy_tags_to_fields(self, fields, policy_tags, parent_name=None):
        updated_fields = []

        # Normalize policy tags for easier matching
        policy_tags['normalized_display_name'] = policy_tags['display_name'].apply(self.normalize_name)

        for field in fields:
            field_name = field["name"]
            field_type = field["type"]
            normalized_field_name = self.normalize_name(field_name)

            # Handle nested fields recursively
            if field.get('fields'):
                nested_fields = self.match_policy_tags_to_fields(
                    field["fields"], policy_tags, parent_name=parent_name
                )
                field["fields"] = nested_fields
            else:
                # Explicitly check for ssn and balance when they are not of type STRING
                if (normalized_field_name == "ssn" or normalized_field_name == "nationalid" or normalized_field_name == "validnationalid") and field_type != "STRING" and field_type != "BOOL":
                    field["policyTags"] = {
                        "names": [
                            "projects/sambla-data-staging-compliance/locations/europe-north1/taxonomies/7698000960465061299/policyTags/8190767684129261300"
                        ]
                    }
                elif normalized_field_name == "phonenumber" and field_type != "STRING":
                    field["policyTags"] = {
                        "names": [
                            "projects/sambla-data-staging-compliance/locations/europe-north1/taxonomies/7698000960465061299/policyTags/1553289368757892144"
                        ]
                    }

                else:
                    # Find matching tag in policy_tags
                    matching_tag = policy_tags[policy_tags['normalized_display_name'] == normalized_field_name]


                    if not matching_tag.empty:
                        policy_tag = matching_tag['policy_tag_id'].values[0]
                        taxonomy_id = matching_tag['taxonomy_id'].values[0]  # Dynamically select taxonomy

                        field["policyTags"] = {
                            "names": [
                                f"projects/{self.project_id}/locations/europe-north1/taxonomies/{taxonomy_id}/policyTags/{policy_tag}"
                            ]
                        }

            # Ensure proper parent-child field naming
            if parent_name and not field.get('name').startswith(f"{parent_name}."):
                field["name"] = f"{parent_name}.{field['name']}"

            updated_fields.append(field)

        return updated_fields
    
    def update_schema_with_policy_tags(self, schemas, policy_tags):
        updated_schemas = {}

        for table_name, schema in schemas.items():
            updated_fields = self.match_policy_tags_to_fields(schema, policy_tags)
            updated_schemas[table_name] = updated_fields
        return updated_schemas

    def save_updated_schema_for_each_table(self, updated_schemas, schema_file_path):
        for table_name, updated_fields in updated_schemas.items():
            try:
                with open(schema_file_path, 'w') as f:
                    json.dump(updated_fields, f, indent=2)
                    logging.info(f"Schema for table {table_name} updated with policy tags")
            except Exception as e:
                logging.error(f"Failed to write to schema file {schema_file_path}: {e}")

            

    def construct_iam_policies(self, policy_tags):
        """Construct IAM policies based on policy tags and update schema files."""
        schemas = {}
        schema_file_path = self.schema_folder_path
        schema_files = glob.glob(os.path.join(schema_file_path, "**/*_schema.json"), recursive=True)
        


        for schema_file_path in schema_files:
            table_name = os.path.basename(schema_file_path).replace("_schema.json", "")
            match = re.search(r"/schemas/([^/]+)/", schema_file_path)
            schema_name = match.group(1) if match else None

            try:
                if os.path.getsize(schema_file_path) == 0:
                    logging.info(f"Schema file is empty: {schema_file_path}. Skipping.")
                    continue  

                with open(schema_file_path, 'r') as file:
                    schemas[table_name] = json.load(file)
            
                if schema_name in ('lvs', 'rahalaitos', 'salus', 'advisa_history'):

                    updated_schemas = self.update_schema_with_policy_tags(schemas, policy_tags)

                    self.save_updated_schema_for_each_table(updated_schemas, schema_file_path)
                else:
                    logging.info(f"Skipping the schemas in: {schema_name}")

            except FileNotFoundError:
                logging.error(f"Schema file not found for table {table_name} at {schema_file_path}")
            except json.JSONDecodeError:
                logging.error(f"Error decoding JSON from schema file: {schema_file_path}")
            except Exception as e:
                logging.error(f"An unexpected error occurred while processing {schema_file_path}: {e}")


    def main(self):
        """Main function to execute the workflow."""
        try:
    
            policy_tags = self.fetch_policy_tags()

            self.construct_iam_policies(policy_tags)

            logging.info("Workflow completed successfully.")
        except Exception as e:
            logging.error("An error occurred during the process.")
            raise e

