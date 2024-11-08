import logging
import yaml
import os
import sys
from jinja2 import Template
from google.cloud import bigquery


class AnonymizationService:
    def __init__(self, config_path, env='dev'):
        self.config = self.load_config(config_path)
        self.env = env
        self.anonymization_config = self.config.get(self.env, self.config.get('default', {})).get('anonymization_service', {})

        if not self.anonymization_config:
            logging.error(f"Configuration for environment '{env}' or 'default' not found.")
            sys.exit(1)

        # Extract values from the YAML config for the selected environment
        self.exposure_project = self.anonymization_config.get('exposure_project')
        self.raw_layer_project = self.anonymization_config.get('raw_layer_project')
        self.compliance_project = self.anonymization_config.get('compliance_project')

        # Initialize BigQuery clients using Application Default Credentials (ADC)
        self.clients = self.initialize_bigquery_clients()

        # Load SQL Templates
        self.key_generation_query_template = self.load_template("key_generation_template.sql")
        self.update_flag_template = self.load_template("update_flag_template.sql")

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
        """Initialize BigQuery Clients using Application Default Credentials."""
        clients = {}
        try:
            clients['raw_layer_project'] = bigquery.Client(project=self.raw_layer_project)
            return clients
        except Exception as e:
            logging.error(f"Error initializing BigQuery clients: {str(e)}")
            raise e

    def load_template(self, template_name):
        """Load SQL templates from the templates directory."""
        try:
            with open(f"templates/{template_name}") as f:
                return Template(f.read())
        except FileNotFoundError as e:
            logging.error(f"Template file not found: {str(e)}")
            raise e

    def get_column_key_for_table(self, table_name):
        """Retrieve the first available key column from the schema of the given table."""
        # Columns to check in order of priority [TODO: for now i search for these 3 columns, have to develop something that should pick the ssn cloumns through pattern matching]
        possible_keys = ['ssn', 'ssn_id', 'national_id']
        
        try:
            # Query schema of the table to check for available columns
            table_ref = f"{self.raw_layer_project}.authorized_view_lvs_integration_legacy.{table_name}"
            schema = self.clients['raw_layer_project'].get_table(table_ref).schema
            columns = [field.name for field in schema]
            
            logging.info(f"Table: {table_name}, Columns: {columns}")

            for key in possible_keys:
                if key in columns:
                    logging.debug(f"Found key: {key} for table: {table_name}")
                    return key
                
            logging.debug(f"No matching key found for table: {table_name}") 
            return None 
        except Exception as e:
            logging.error(f"Error fetching schema for {table_name}: {str(e)}")
            raise e

    def update_anonymized_flags(self, join_keys):
        """Update anonymization flags in the raw layer project."""
        exists_clauses = []
        
        # Step 1: Query BigQuery to get the list of tables in the dataset
        dataset_ref = f"{self.raw_layer_project}.authorized_view_lvs_integration_legacy"
        
        try:
            tables = self.clients['raw_layer_project'].list_tables(dataset_ref)
            relevant_tables = [table.table_id for table in tables]
            
            logging.debug(f"Relevant tables found: {relevant_tables}")
        except Exception as e:
            logging.error(f"Error fetching tables from the dataset: {str(e)}")
            return
        
        # Step 2: Loop through the relevant tables and find the key column
        for table in relevant_tables:

            key = join_keys.get(table) or self.get_column_key_for_table(table)
            
            logging.debug(f"Table: {table}, Key: {key}") 
            
            if key:
                exists_clause = f"SELECT raw.{key} FROM {self.raw_layer_project}.authorized_view_lvs_integration_legacy.{table} raw"
                exists_clauses.append(exists_clause)
            else:
                logging.debug(f"No key found for table: {table}") 

        # If no clauses are found, use a default SELECT NULL
        exists_clauses_str = ' UNION ALL '.join(exists_clauses) if exists_clauses else "SELECT NULL"

        logging.debug(f"Generated exists_clauses_str: {exists_clauses_str}")

        # Step 3: Generate and execute the final query
        update_flag_query = self.update_flag_template.render(
            compliance_project=self.compliance_project,
            raw_layer_project=self.raw_layer_project,
            exists_clauses=exists_clauses_str
        )

        logging.info("Executing anonymization flag update query.")
        self.execute_query(self.clients['raw_layer_project'], update_flag_query)

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

    def main(self):
        """Main function to execute the workflow."""
        try:
            logging.info("Executing key generation.")
            key_generation_query = self.key_generation_query_template.render(
                exposure_project=self.exposure_project,
                compliance_project=self.compliance_project
            )
            self.execute_query(self.clients['raw_layer_project'], key_generation_query)

            logging.info("Updating flags to anonymized in the GDPR vault.")
            join_keys = {}             
            self.update_anonymized_flags(join_keys)
            logging.info("Workflow completed successfully.")

        except Exception as e:
            logging.error("An error occurred during the process.")
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO) 

    # Path to the YAML config file
    config_path = "config.yaml"

    # Get environment from an environment variable (default to 'dev')
    env = os.getenv('ENV', 'dev')

    # Initialize the anonymization service with the config path and environment
    anonymization_service = AnonymizationService(config_path, env)
    anonymization_service.main()
