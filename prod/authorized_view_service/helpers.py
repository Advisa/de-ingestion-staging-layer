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
        self.se_table_names  = self.authorized_view_config.get('se_table_names', [])
        self.fi_table_names  = self.authorized_view_config.get('fi_table_names', [])
        self.no_table_names  = self.authorized_view_config.get('no_table_names', [])
        # In order to use the encryption template dynamically, we explicitly define the gdpr_vault table names as follows
        self.gdpr_vault_table_dev  = self.config.get('default', {}).get('authorized_view_service', {}).get('gdpr_vault_table')
        self.gdpr_vault_table_prod = self.config.get('prod', {}).get('authorized_view_service', {}).get('gdpr_vault_table')
        # In order to deploy the auth views for production and dev datasets, we define the output file paths
        self.output_file_name_dev  = self.config.get('default', {}).get('authorized_view_service', {}).get('output_file_name')
        self.output_file_name_prod = self.config.get('prod', {}).get('authorized_view_service', {}).get('output_file_name')


        # Initialize the current path where this service is located
        project_root = Path(__file__).resolve().parent.parent  
        self.base_path = project_root / self.base_folder 

        # Initialize BigQuery clients using ADC
        self.clients = self.initialize_bigquery_clients()

        # Load SQL templates
        self.encryption_query_template = self.load_template("encryption_query_template.sql")
        self.encryption_query_template_cdc = self.load_template("encryption_query_template_cdc.sql")

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

    def generate_encryption_queries(self, encrypted_query_template, output_file_name):
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
        mapping_file_path = os.path.join(self.base_path, 'templates', 'auth_view_mapping_salus.txt')
        with open(mapping_file_path, 'w') as f:
            for eq in encryption_queries:
                f.write(eq + "\n")
    
    def generate_encryption_queries_cdc(self, encrypted_query_template_cdc):
        """Generate encryption queries from the encrypted query template."""
        encryption_queries = []
        result = self.execute_query(self.clients['raw_layer_project'], encrypted_query_template_cdc)
    
        
        for row in result:
            schema = row.table_schema
            table = row.table_name
            encryption_query = row.final_encrypted_columns
            encryption_queries.append(f"{schema}|{table}|{encryption_query}")
            self.processed_tables.add(f"{schema}|{table}") 

        # Save the encryption queries to a file (for now it saves to lvs, please change this later)
        mapping_file_path = os.path.join(self.base_path, 'templates', 'auth_view_mapping_cdc.txt')
        mapping_file_path = os.path.join(self.base_path, 'templates', output_file_name)
        with open(mapping_file_path, 'w') as f:
            for eq in encryption_queries:
                f.write(eq + "\n")


    def main(self):
        """Main function to execute the workflow."""
        try:
            # Check if generate_cdc is set to "true"
            if self.authorized_view_config.get('generate_cdc') == "true":
                logging.info("Generating CDC encryption queries...")
                # Render and execute the encryption query template for dev
                #encrypted_query_template_dev = self.encryption_query_template_cdc.render(
                #    compliance_project=self.compliance_project,
                #    raw_layer_project=self.raw_layer_project,
                #    gdpr_vault_table=self.gdpr_vault_table_prod,
                #    exposure_project=self.exposure_project,
                #    se_table_names=self.se_table_names,
                #    fi_table_names=self.fi_table_names,
                #    no_table_names=self.no_table_names
                #)
                # Generate encryption queries for dev
                # Uncomment it to test the generate the template for dev views
                # self.generate_encryption_queries_cdc(encrypted_query_template_dev)
                encrypted_query_template_prod = self.encryption_query_template_cdc.render(
                    compliance_project=self.compliance_project,
                    raw_layer_project=self.raw_layer_project,
                    gdpr_vault_table=self.gdpr_vault_table_prod,
                    exposure_project=self.exposure_project,
                    se_table_names=self.se_table_names,
                    fi_table_names=self.fi_table_names,
                    no_table_names=self.no_table_names
                )
                # Generate encryption queries for prod
                self.generate_encryption_queries_cdc(encrypted_query_template_prod)
            else:
                logging.info("Generating standard encryption queries...")
                # Render and execute the encryption query template for dev
                #encrypted_query_template_dev = self.encryption_query_template.render(
                #    compliance_project=self.compliance_project,
                #    raw_layer_project=self.raw_layer_project,
                #    gdpr_vault_table=self.gdpr_vault_table_dev,
                #    exposure_project = self.exposure_project
                #)

                # Generate encryption queries for dev
                # Uncomment it to test the generate the template for dev views
                #self.generate_encryption_queries(encrypted_query_template_dev, self.output_file_name_dev)

                # Render and execute the encryption query template for prod
                encrypted_query_template_prod = self.encryption_query_template.render(
                    compliance_project=self.compliance_project,
                    raw_layer_project=self.raw_layer_project,
                    gdpr_vault_table=self.gdpr_vault_table_prod,
                    exposure_project = self.exposure_project)

                # Generate encryption queries for prod
                self.generate_encryption_queries(encrypted_query_template_prod, self.output_file_name_prod)

            logging.info("Workflow completed successfully.")
        except Exception as e:
            logging.error("An error occurred during the process.")
            raise e

