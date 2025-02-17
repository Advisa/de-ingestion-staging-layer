import logging
import os
import yaml
from jinja2 import Template
from google.cloud import bigquery
from pathlib import Path

class AuthorizedViewService:
    def __init__(self, config_path, env='dev'):
        self.config = self.load_config(config_path)
        self.env = env
        self.authorized_view_config = self.config.get(self.env, self.config.get('default', {})).get('authorized_view_service', {})
        
        if not self.authorized_view_config:
            logging.error(f"Configuration for environment '{env}' or 'default' not found.")
            raise ValueError(f"Configuration for environment '{env}' not found.")

        # Extract values from the YAML config
        self.raw_layer_project = self.authorized_view_config.get('raw_layer_project')
        self.exposure_project = self.authorized_view_config.get('exposure_project')
        self.compliance_project = self.authorized_view_config.get('compliance_project')
        self.base_folder = self.authorized_view_config.get('base_folder')
        self.generate_encrypted  = self.authorized_view_config.get('generate_encrypted')
        self.gdpr_vault_table  = self.authorized_view_config.get('gdpr_vault_table')

        # Initialize the current path where this service is located
        project_root = Path(__file__).resolve().parent.parent  
        self.base_path = project_root / self.base_folder 

        # Initialize BigQuery clients using ADC
        self.clients = self.initialize_bigquery_clients()

        # Load SQL templates
        self.encryption_query_template = self.load_template("encryption_query_template.sql")

        # Initialize output of sql template files
        self.output_template_file = "generated_source_query.sql"

        self.processed_tables = set() 
        
    def load_config(self, config_path):
        """Load YAML configuration from the provided path."""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            logging.info("YAML configuration loaded successfully.")
            return config
        except FileNotFoundError as e:
            logging.error(f"Config file not found: {str(e)}")
          
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML file: {str(e)}")

    def initialize_bigquery_clients(self):
        """Initialize BigQuery Clients using service account credentials."""
        clients = {}
        try:
            
            clients['raw_layer_project'] = bigquery.Client(project=self.raw_layer_project)
            return clients
        except Exception as e:
            logging.error(f"Error initializing BigQuery clients: {str(e)}")
            raise e

    def load_template(self, template_name):
        """Load SQL templates from the templates directory."""
        template_path = os.path.join(self.base_path, 'templates', template_name)
        try:
            with open(template_path) as f:
                return Template(f.read())
        except FileNotFoundError as e:
            logging.error(f"Template file not found: {str(e)}")
            raise e

    def save_template(self,query,template_name):
        """Upload a new SQL template to the templates directory."""
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

    def generate_union_all_query(self):
        """Generate the UNION ALL query for all source tables/views."""
        query = """
            WITH tables AS (SELECT
                table_schema
                FROM
                `sambla-data-staging-compliance`.`region-europe-north1`.INFORMATION_SCHEMA.TABLES
                WHERE
                table_schema like '%_integration_legacy'
            )
                SELECT
                DISTINCT table_schema,
                CONCAT( "SELECT * FROM `sambla-data-staging-compliance.", table_schema, "`.INFORMATION_SCHEMA.COLUMNS" ) AS column_query
                FROM
                tables
        """
        return self.execute_query(self.clients['raw_layer_project'], query)

    def generate_encryption_queries(self, encrypted_query_template):
        """Generate encryption queries from the encrypted query template."""
        encryption_queries = []
        result = self.execute_query(self.clients['raw_layer_project'], encrypted_query_template)
    
        
        for row in result:
            schema = row.table_schema
            table = row.table_name
            encryption_query = row.final_encrypted_columns
            encryption_queries.append(f"{schema}|{table}|{encryption_query}")
            self.processed_tables.add(f"{schema}|{table}") 

        # Save the encryption queries to a file (for now it saves to lvs, please change this later)
        mapping_file_path = os.path.join(self.base_path, 'templates', 'auth_view_mapping_lvs.txt')
        with open(mapping_file_path, 'w') as f:
            for eq in encryption_queries:
                f.write(eq + "\n")

    def generate_non_encrypted_queries(self):
        """Generate non-encrypted queries for each table."""
        non_encrypted_queries = []
        datasets = self.clients['raw_layer_project'].list_datasets()

        if datasets:
            for dataset in datasets:
                schema = dataset.dataset_id

                if 'legacy' in schema and 'authorized' not in schema:
                    logging.info(f"Processing dataset: {schema}")
                    table_query = f"""
                        SELECT table_name
                        FROM `{self.raw_layer_project}.{schema}.INFORMATION_SCHEMA.TABLES`
                    """
                    table_result = self.execute_query(self.clients['raw_layer_project'], table_query)
                    
                    for table_row in table_result:
                        table = table_row.table_name
                        #if f"{schema}|{table}" not in self.processed_tables:
                        if table == "providers_lvs_r":
                                non_encrypted_query = f"SELECT *, _FILE_NAME as f FROM `{self.raw_layer_project}.{schema}.{table}`"
                        else:
                            non_encrypted_query = f"SELECT * FROM `{self.raw_layer_project}.{schema}.{table}`"
                        non_encrypted_queries.append(f"{schema}|{table}|{non_encrypted_query}")
            
        else:
            logging.error(f"No datasets found in project {self.raw_layer_project}")
        
        mapping_file_path = os.path.join(self.base_path, 'templates', 'auth_view_mapping_non_encrypted.txt')
        with open(mapping_file_path, 'w') as f:
            for eq in non_encrypted_queries:
                f.write(eq + "\n")


    def main(self):
        """Main function to execute the workflow."""

        try:
            # Render and execute the encryption query template
            encrypted_query_template = self.encryption_query_template.render(
                compliance_project=self.compliance_project,
                raw_layer_project=self.raw_layer_project,
                gdpr_vault_table=self.gdpr_vault_table
            )
            self.generate_encryption_queries(encrypted_query_template)


            logging.info("Workflow completed successfully.")
        except Exception as e:
            logging.error("An error occurred during the process.")
            raise e
