import logging
import os
import yaml
from jinja2 import Template
from google.cloud import bigquery
from google.auth.transport.requests import Request
from google.oauth2 import service_account


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
        self.base_path = self.authorized_view_config.get('base_path')

        # Initialize BigQuery clients using service account credentials
        self.clients = self.initialize_bigquery_clients()

        # Load SQL templates
        self.encryption_query_template = self.load_template("encryption_query_template.sql")

        # Initialize output of sql template files
        self.output_template_file = "generated_source_query.sql"
        
    def load_config(self, config_path):
        """Load YAML configuration from the provided path."""
        print("config path:",config_path)
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
            key_path = self.authorized_view_config.get('service_account_key')
            credentials = service_account.Credentials.from_service_account_file(
                key_path, scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
            credentials.refresh(Request())
            clients['raw_layer_project'] = bigquery.Client(credentials=credentials)
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
                table_schema IN ("lvs_integration_legacy","rahalaitos_integration_legacy")
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
            encryption_query = row.encrypted_columns
            encryption_queries.append(f"{schema}|{table}|{encryption_query}")

        # Save the encryption queries to a file
        mapping_file_path = os.path.join(self.base_path, 'templates', 'auth_view_mapping.txt')
        with open(mapping_file_path, 'w') as f:
            for eq in encryption_queries:
                f.write(eq + "\n")

    def main(self):
        """Main function to execute the workflow."""

        try:
            # Generate the UNION ALL query
            query_table_names_result = self.generate_union_all_query()
            union_all_queries = [row.column_query for row in query_table_names_result]
            union_all_query = '\nUNION ALL \n'.join(union_all_queries)

            # Save the complete union all statement to a file
            self.save_template(union_all_query,self.output_template_file)

            # Render and execute the encryption query template
            encrypted_query_template = self.encryption_query_template.render(
                query_table_columns=union_all_query,
                compliance_project=self.compliance_project,
                raw_layer_project=self.raw_layer_project
            )
            self.generate_encryption_queries(encrypted_query_template)

            logging.info("Workflow completed successfully.")
        except Exception as e:
            logging.error("An error occurred during the process.")
            raise e
