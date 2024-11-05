from google.cloud import bigquery
import logging
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import os
from jinja2 import Template

# Uncomment for testing
SCOPES = ['https://www.googleapis.com/auth/cloud-platform']

SERVICE_ACCOUNT_KEYS = {
    #"exposure_project": "/Users/aruldharani/Downloads/data-domain-data-warehouse-dd73eebb6814.json",
    "exposure_project": "/Users/duygugenc/Documents/de-ingestion-staging-layer-filtering/sambla-data-staging-compliance-gcs-handler.json",
    "raw_layer_project": "/Users/duygugenc/Documents/de-ingestion-staging-layer-filtering/sambla-data-staging-compliance-gcs-handler.json"
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

with open("/Users/duygugenc/Documents/de-ingestion-staging-layer/anonymisation_service/templates/key_generation_template.sql") as f:
    key_generation_query_template = Template(f.read())
with open("/Users/duygugenc/Documents/de-ingestion-staging-layer/anonymisation_service/templates/update_flag_template.sql") as f:
    update_flag_template = Template(f.read())
with open("/Users/duygugenc/Documents/de-ingestion-staging-layer/prod/modules/auth_views/authorized_view_scripts/encryption_query_template.sql") as f:
    encrypted_query_template = Template(f.read())

# Project names
exposure_project = 'data-domain-data-warehouse'
raw_layer_project = 'sambla-data-staging-compliance'
complaince_project = 'sambla-group-compliance-db'

def  read_union_all_query_template():
    with open("/Users/duygugenc/Documents/de-ingestion-staging-layer/prod/modules/auth_views/authorized_view_scripts/generated_source_query.sql") as f:
        source_union_all_query_template = Template(f.read())
        return source_union_all_query_template.render()
    
def execute_query(client, query):
    """Execute a query using a BigQuery client."""
    try:
        query_job = client.query(query)
        result = query_job.result()
        logging.info("Query executed successfully.")

        return result
    except Exception as e:
        logging.error(f"Error executing query: {str(e)}")
        raise e
    
def update_anonymized_flags(raw_layer_project, complaince_project, relevant_tables,join_keys,schemas,raw_layer_client):
    exists_clauses = []
    print(complaince_project)
    for table, schema in zip(relevant_tables, schemas):
        key = join_keys.get(table)
        
        if key:
            exists_clause = f"SELECT raw.{key} FROM `{raw_layer_project}.{schema}.{table}` raw"
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

        union_all_query = read_union_all_query_template()
        print(union_all_query)

        logging.info("Generating dynamic encryption queries.")
        # Pass the union all query cte (which is found in generated_source_query.sql) to the `encryption_query_template.sql`
        encryption_query = encrypted_query_template.render(
            query_table_columns = union_all_query
            # exposure_project=exposure_project,
            # raw_layer_project=raw_layer_project,
            # complaince_project=complaince_project
        )
        print(encryption_query) 

        dynamic_queries = execute_query(raw_layer_client, encryption_query)
        relevant_tables = []
        schemas = []
        join_keys = {}

        for row in dynamic_queries:
            print("\n TABLE INFO:", row.table_name,",",row.table_schema,",",row.j_key)
            relevant_tables.append(row.table_name)
            schemas.append(row.table_schema)
            join_keys[row.table_name] = row.j_key

        logging.info("Updating flags to anonymized in the gdpr_vault.")
        update_anonymized_flags(raw_layer_project, complaince_project, relevant_tables, join_keys,schemas,raw_layer_client)
        logging.info("Workflow completed successfully.")

        

    except Exception as e:
        logging.error("An error occurred during the process.")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
