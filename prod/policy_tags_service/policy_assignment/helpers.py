import os
import json
import logging
from jinja2 import Template
from google.cloud import bigquery
import yaml
import glob
from pathlib import Path

class PolicyAssignmentService:
    def __init__(self, config_path, env='dev'):
        self.config = self.load_config(config_path)
        self.env = env
        self.policy_assignment_config = self.config.get(self.env, self.config.get('default', {})).get('policy_tags_service', {})
        
        if not self.policy_assignment_config:
            logging.error(f"Configuration for environment '{env}' or 'default' not found.")
            raise ValueError(f"Configuration for environment '{env}' not found.")

        # Extract values from the configuration
        self.raw_layer_project = self.policy_assignment_config.get('raw_layer_project')
        self.base_folder = self.policy_assignment_config.get('base_folder')

        # Initialize BigQuery clients using service account credentials
        self.clients = self.initialize_bigquery_clients()

        # Initialize the current path where this service is located
        project_root = Path(__file__).resolve().parent.parent
        self.current_path = os.path.dirname(os.path.abspath(__file__))  
        self.base_path = project_root 

        self.schema_folder_path = project_root.parent / 'schemas'
        print("base_path:",self.schema_folder_path)

        # Load SQL templates
        self.sensitive_fields_query_template = self.load_template(self.current_path,"get_matching_sensitive_fields.sql")

        # Initialize output path of union all query
        self.union_all_query_file = "generated_source_query.sql"
        # Initialize output of sql template files
        self.output_template_file = "sensitive_fields_query.sql"

        

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
        """Initialize BigQuery Clients using service account credentials."""
        clients = {}
        try:
            clients['raw_layer_project'] = bigquery.Client(project=self.raw_layer_project)
            return clients
        except Exception as e:
            logging.error(f"Error initializing BigQuery clients: {str(e)}")
            raise e
        
    def load_template(self, base_path,template_name):
        """Load SQL templates from the templates directory."""
        template_path = os.path.join(base_path, 'templates', template_name)
        try:
            with open(template_path) as f:
                return Template(f.read())
        except FileNotFoundError as e:
            logging.error(f"Template file not found: {str(e)}")
            raise e

    def save_template(self, query, template_name):
        """Save a SQL query as a template in the templates directory."""
        output_template_path = os.path.join(self.base_path, 'templates', template_name)
        try:
            with open(output_template_path, 'w') as f:
                f.write(query)
        except FileNotFoundError as e:
            logging.error(f"Template file not found: {str(e)}")
            raise e

    def execute_query(self, client, query):
        """Execute a query using a BigQuery client."""
        try:
            logging.info(f"Executing the query")
            query_job = client.query(query)
            result = query_job.result()
            logging.info("Query executed successfully.")
            return result
        except Exception as e:
            logging.error(f"Error executing query: {str(e)}")
            raise e

    def get_matching_sensitive_fields(self,sensitive_fields_query_template):
        """Get matching sensitive fields based on a BigQuery query."""   

        try:
            # Execute the query and store the results
            results = self.execute_query(self.clients['raw_layer_project'], sensitive_fields_query_template)
            
            # Define dictionary to hold table and column mappings of the tables to which policy tags are applied to.
            policy_mapping = {}
            for row in results:
                if row.table_name not in policy_mapping:
                    policy_mapping[row.table_name] = {}
                policy_mapping[row.table_name][row.column_name] = row.iam_policy_name
            
            return policy_mapping
        except Exception as e:
            # If no table names are found in the desired GCS location, print a message indicating that
            print(f"An error occurred: {e}")
            return {}, []

    def construct_iam_policies(self, policy_mapping):
        """Construct IAM policies based on the policy mapping and update schema files."""
        schema_file_path = self.schema_folder_path
        schema_files = glob.glob(os.path.join(schema_file_path, "**/*_schema.json"), recursive=True)
        logging.info(f"Constructing iam policies for schemas in path: {schema_file_path}")

        for schema_file_path in schema_files:
           #logging.info(f"Processing the current schema file: {schema_file_path}")
            table_name = os.path.basename(schema_file_path).replace("_schema.json", "")
            #print(f"Processing schema for table: {table_name} from {schema_file_path}")

            column_policies = policy_mapping.get(table_name, {})

            try:
                if os.path.getsize(schema_file_path) == 0:
                    logging.info(f"Schema file is empty: {schema_file_path}. Skipping.")
                    continue  

                with open(schema_file_path, 'r') as file:
                    schema = json.load(file)
                
                updated = False
                for field in schema:
                    # This is a temporary or statement, and applies till we set the table-policy mapping
                    if (field["name"] in column_policies and field["name"]!="data" ) or (table_name=="insurance_log_raha_r" and field["name"]=="data") and table_name!="bids_salus_r":
                        field["policyTags"] = {"names": [column_policies[field["name"]]]}
                        updated = True
            
                if updated:
                    print("updated:",table_name)
                    try:
                        with open(schema_file_path, 'w') as file:  
                            json.dump(schema, file, indent=4)
                        logging.info(f"Schema for table {table_name} updated with policy tags")
                    except Exception as e:
                        logging.info(f"Failed to write to schema file {schema_file_path}: {e}")

            except FileNotFoundError:
                print(f"Schema file not found for table {table_name} at {schema_file_path}")
            except json.JSONDecodeError:
                print(f"Error decoding JSON from schema file: {schema_file_path}")
            except Exception as e:
                print(f"An unexpected error occurred while processing {schema_file_path}: {e}")

    def main(self):
        """Main function to execute the workflow."""
        try:
            # Load the union all query template
            union_all_query = self.load_template("authorized_view_service",self.union_all_query_file).render()

            # Render and execute the encryption query template
            sensitive_fields_query_template = self.sensitive_fields_query_template.render(
                source_table_columns=union_all_query,
                raw_layer_project=self.raw_layer_project
            )

            policy_mapping = self.get_matching_sensitive_fields(sensitive_fields_query_template)

            self.construct_iam_policies(policy_mapping)

            logging.info("Workflow completed successfully.")
        except Exception as e:
            logging.error("An error occurred during the process.")
            raise e

