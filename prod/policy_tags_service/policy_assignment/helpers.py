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
        output_template_path = os.path.join(self.base_path, 'policy_assignment/templates', template_name)
        os.makedirs(os.path.dirname(output_template_path), exist_ok=True)
        try:
            with open(output_template_path, 'w') as f:
                f.write(query)
                logging.info(f"File is written to this location; {output_template_path}")
        except FileNotFoundError as e:
            logging.error(f"Template file not found: {str(e)}")
            raise e

    def execute_query(self, client, query):
        """Execute a query using a BigQuery client."""
        try:
            logging.info(f"Executing the query")
            query_job = client.query(query)
            result = query_job.result()
            print(query)
            logging.info("Query executed successfully.")
            return result
        except Exception as e:
            logging.error(f"Error executing query: {str(e)}")
            raise e
    
    def generate_union_all_query(self):
        """Generate the UNION ALL query for all source tables/views."""
        query = """
            WITH tables AS (SELECT
                table_schema
                FROM
                `sambla-data-staging-compliance`.`region-europe-north1`.INFORMATION_SCHEMA.TABLES
                WHERE
                table_schema IN ("lvs_integration_legacy","rahalaitos_integration_legacy","salus_integration_legacy","advisa_history_integration_legacy")
            )
                SELECT
                DISTINCT table_schema,
                CONCAT( "SELECT * FROM `sambla-data-staging-compliance.", table_schema, "`.INFORMATION_SCHEMA.COLUMNS" ) AS column_query
                FROM
                tables
        """
        print(query)
        return self.execute_query(self.clients['raw_layer_project'], query)
    

    def get_matching_sensitive_fields(self,sensitive_fields_query_template):
        """Get matching sensitive fields based on a BigQuery query."""   

        try:
            # Execute the query and store the results
            results = self.execute_query(self.clients['raw_layer_project'], sensitive_fields_query_template)
            
            # Define dictionary to hold table and column mappings of the tables to which policy tags are applied to.
            policy_mapping = {}
            processed_columns = set()

            for row in results:
                table_name = row.table_name
                column_name = row.column_name  # Normalize the column name
                policy_name = row.iam_policy_name
            
                if (table_name, column_name) not in processed_columns:
                    print(f"For table: {table_name}, this {column_name}")
                    if table_name not in policy_mapping:
                        policy_mapping[table_name] = {}
                    policy_mapping[table_name][column_name] = policy_name
                    processed_columns.add((table_name, column_name))
        
             # Exceptional cases where policy tags are only required for specific tables:
            exceptional_cases = {
                 "crm_user_raha_r": {"name": "projects/sambla-data-staging-compliance/locations/europe-north1/taxonomies/6126692965998272750/policyTags/1064433561942680153"},
                 "accounts_salus_r": {"name": "projects/sambla-data-staging-compliance/locations/europe-north1/taxonomies/6126692965998272750/policyTags/1064433561942680153"},
                 "insurance_log_raha_r": {"name": "projects/sambla-data-staging-compliance/locations/europe-north1/taxonomies/6126692965998272750/policyTags/1064433561942680153"}
             }
            
            for table, columns in exceptional_cases.items():
                if table not in policy_mapping:
                    policy_mapping[table] = {}
                for column, policy in columns.items():
                    policy_mapping[table][column] = policy
            return policy_mapping
        except Exception as e:
            # If no table names are found in the desired GCS location, print a message indicating that
            logging.error(f"An error occurred: {e}")
            return {}, []

    def construct_iam_policies(self, policy_mapping):
        """Construct IAM policies based on the policy mapping and update schema files."""
        schema_file_path = self.schema_folder_path
        schema_files = glob.glob(os.path.join(schema_file_path, "**/*_schema.json"), recursive=True)

        for schema_file_path in schema_files:
            #logging.info(f"Processing the current schema file: {schema_file_path}")
            table_name = os.path.basename(schema_file_path).replace("_schema.json", "")
            column_policies = policy_mapping.get(table_name, {})
           
            try:
                if os.path.getsize(schema_file_path) == 0:
                    logging.info(f"Schema file is empty: {schema_file_path}. Skipping.")
                    continue  

                with open(schema_file_path, 'r') as file:
                    schema = json.load(file)
                
                updated = False
                for field in schema:
                    schema_field = field["name"]
                    # Check if the field already has policyTags
                    if field.get("policyTags"):  
                        #logging.info(f"Skipping field '{field['name']}' in {table_name} because policyTags already exist.")
                        continue  

                     # Check if the field name contains any sensitive substring from column_policies keys
                    #is_sensitive = any(sensitive_field in schema_field for sensitive_field in column_policies)
                    #print(f"is column:{schema_field} sensitive? {is_sensitive}")
                    if (field["name"] in column_policies) and table_name!="bids_salus_r":
                        field["policyTags"] = {"names": [column_policies[field["name"]]]}
                        updated = True
                        logging.info(f"Schema file is updated for table:{table_name} with tag:{schema_field}")
            
                if updated:
                    try:
                        with open(schema_file_path, 'w') as file:  
                            json.dump(schema, file, indent=4)
                        logging.info(f"Schema for table {table_name} updated with policy tags")
                    except Exception as e:
                        logging.info(f"Failed to write to schema file {schema_file_path}: {e}")

            except FileNotFoundError:
                logging.error(f"Schema file not found for table {table_name} at {schema_file_path}")
            except json.JSONDecodeError:
                logging.error(f"Error decoding JSON from schema file: {schema_file_path}")
            except Exception as e:
                logging.error(f"An unexpected error occurred while processing {schema_file_path}: {e}")

    def main(self):
        """Main function to execute the workflow."""
        try:
            # Generate union all queries
            query_table_names_result = self.generate_union_all_query()
            union_all_queries = [row.column_query for row in query_table_names_result]
            union_all_query = '\nUNION ALL \n'.join(union_all_queries)
            print(union_all_query)
            # Save the complete union all statement to a file
            self.save_template(union_all_query,self.union_all_query_file)

            # Render and execute the encryption query template
            sensitive_fields_query_template =  self.load_template(self.current_path,"get_matching_sensitive_fields.sql").render(
                source_table_columns=union_all_query,
                raw_layer_project=self.raw_layer_project
            )


            policy_mapping = self.get_matching_sensitive_fields(sensitive_fields_query_template)


            self.construct_iam_policies(policy_mapping)

            logging.info("Workflow completed successfully.")
        except Exception as e:
            logging.error("An error occurred during the process.")
            raise e

