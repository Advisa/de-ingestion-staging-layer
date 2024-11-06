import os
import json
import logging
from jinja2 import Template
from google.cloud import bigquery
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import yaml
import glob

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
        self.base_path = os.path.join(self.policy_assignment_config.get('base_path'),"policy_assignment")
        self.schema_file_path = self.policy_assignment_config.get('schema_file_path')

        # Initialize BigQuery clients using service account credentials
        self.clients = self.initialize_bigquery_clients()

        # Load SQL templates
        self.sensitive_fields_query_template = self.load_template(self.base_path,"get_matching_sensitive_fields.sql")

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
            key_path = self.policy_assignment_config.get('service_account_key')
            credentials = service_account.Credentials.from_service_account_file(
                key_path, scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
            credentials.refresh(Request())
            clients['raw_layer_project'] = bigquery.Client(credentials=credentials)
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
            logging.info(f"Executing query: {query}")
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
        schema_files = glob.glob(os.path.join(self.schema_file_path, "**/*_schema.json"), recursive=True)

        for schema_file_path in schema_files:
            table_name = os.path.basename(schema_file_path).replace("_schema.json", "")
            #print(f"Processing schema for table: {table_name} from {schema_file_path}")

            column_policies = policy_mapping.get(table_name, {})

            try:
                if os.path.getsize(schema_file_path) == 0:
                    print(f"Schema file is empty: {schema_file_path}. Skipping.")
                    continue  

                with open(schema_file_path, 'r') as file:
                    schema = json.load(file)
                
                updated = False
                for field in schema:
                    if field["name"] in column_policies:
                        #print("table:",table_name," and policies:",column_policies[field["name"]])
                        field["policyTags"] = {"names": [column_policies[field["name"]]]}
                        updated = True
            
                if updated:
                    try:
                        with open(schema_file_path, 'w') as file:  
                            json.dump(schema, file, indent=4)
                        print(f"Schema for table {table_name} updated with policy tags")
                    except Exception as e:
                        print(f"Failed to write to schema file {schema_file_path}: {e}")

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
            union_all_query = self.load_template("prod/authorized_view_service",self.union_all_query_file).render()

            # Render and execute the encryption query template
            sensitive_fields_query_template = self.sensitive_fields_query_template.render(
                source_table_columns=union_all_query,
                raw_layer_project=self.raw_layer_project
            )


            policy_mapping = self.get_matching_sensitive_fields(sensitive_fields_query_template)


            # Construct IAM policies based on the policy mapping
            schema_dir = 'path/to/schemas'
            self.construct_iam_policies(policy_mapping)

            logging.info("Workflow completed successfully.")
        except Exception as e:
            logging.error("An error occurred during the process.")
            raise e
