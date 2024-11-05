from google.cloud import bigquery
import logging
from jinja2 import Template
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import os


#Uncomment for testing
SCOPES = ['https://www.googleapis.com/auth/cloud-platform']

SERVICE_ACCOUNT_KEYS = {
    "exposure_project": "/Users/aruldharani/Downloads/data-domain-data-warehouse-dd73eebb6814.json",
    "raw_layer_project": "/Users/aruldharani/Downloads/sambla-data-staging-compliance-5d68a484424a.json"
}


clients = {}
for project_name, key_path in SERVICE_ACCOUNT_KEYS.items():
    if os.path.exists(key_path):
        credentials = service_account.Credentials.from_service_account_file(key_path, scopes=['https://www.googleapis.com/auth/cloud-platform'])
        credentials.refresh(Request()) 
        clients[project_name] = bigquery.Client(credentials=credentials)
    else:
        raise FileNotFoundError(f"Service account key file not found: {key_path}")


exposure_client = clients['exposure_project']
raw_layer_client = clients['raw_layer_project']


with open("templates/key_generation_template.sql") as f:
    key_generation_query_template = Template(f.read())

with open("templates/encryption_template.sql") as f:
    encryption_query_template = Template(f.read())

with open("templates/update_flag_template.sql") as f:
    update_flag_template = Template(f.read())

# Project names
exposure_project = 'data-domain-data-warehouse'
raw_layer_project = 'sambla-data-staging-compliance'
complaince_project = 'sambla-group-compliance-db'

def execute_query(client, query):
    """Execute a query using a BigQuery client."""
    print(query)
    try:
        query_job = client.query(query)
        result = query_job.result()
        logging.info("Query executed successfully.")
        return result
    except Exception as e:
        logging.error(f"Error executing query: {str(e)}")
        raise e

def update_anonymized_flags(raw_layer_project, complaince_project, relevant_tables,join_keys,raw_layer_client):
    exists_clauses = []
    print(complaince_project)
    for table in relevant_tables:
        key = join_keys.get(table)
        
        if key:
            exists_clause = f"SELECT raw.{key} FROM `{raw_layer_project}.authorized_view_lvs_integration_legacy.view_{table}` raw"
            exists_clauses.append(exists_clause)

    if exists_clauses:
        exists_clauses_str = ' UNION ALL '.join(exists_clauses)
    else:
        exists_clauses_str = "SELECT NULL"

    update_flag_query = update_flag_template.render(
        complaince_project=complaince_project,
        raw_layer_project=raw_layer_project,
        exists_clauses=exists_clauses_str
    )

    print(update_flag_query) 
    logging.info("Executing anonymization flag update query.")
    execute_query(raw_layer_client, update_flag_query)  

def create_authorized_view(raw_layer_client, raw_layer_project, table_name, encryption_query):
    view_name = f"{raw_layer_project}.authorized_view_lvs_integration_legacy.view_{table_name}"
    create_view_query = f"CREATE OR REPLACE VIEW `{view_name}` AS {encryption_query}"
    logging.info(f"Creating view for table: {table_name}")
    execute_query(raw_layer_client, create_view_query)


def main():
    """Main function to execute the workflow."""
    try:

        logging.info("Executing key generation and data encryption query.")
        

        key_generation_query = key_generation_query_template.render(
            exposure_project=exposure_project,
            complaince_project=complaince_project
        )
        execute_query(raw_layer_client, key_generation_query)
        logging.info("Key generation and encryption query executed successfully.")


        logging.info("Generating dynamic encryption queries.")
        encryption_query = encryption_query_template.render(
            exposure_project=exposure_project,
            raw_layer_project=raw_layer_project,
            complaince_project=complaince_project
        )

        print(encryption_query) 
        dynamic_queries = execute_query(raw_layer_client, encryption_query)

        relevant_tables = []
        join_keys = {}

        for row in dynamic_queries:
            table_name = row['table_name']
            relevant_tables.append(table_name)
            join_keys[table_name] = row['join_key'] 
            
            # Create the authorized view
            create_authorized_view(raw_layer_client, raw_layer_project, table_name, row.encryption_query)
            # view_name = f"{raw_layer_project}.authorized_view_lvs_integration_legacy.view_{table_name}"
            # run_query = f"""
            # SELECT * FROM `{view_name}`
            # """
            # result = execute_query(exposure_client, run_query)
            # for row in result:
            #     print(dict(row))
        logging.info("Updating flags to anonymized in the gdpr_vault.")
        update_anonymized_flags(raw_layer_project, complaince_project, relevant_tables, join_keys,raw_layer_client)
        logging.info("Workflow completed successfully.")

        

    except Exception as e:
        logging.error("An error occurred during the process.")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
