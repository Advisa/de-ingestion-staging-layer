from google.cloud import bigquery
import logging
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from jinja2 import Template
import os

# Define the required scope for the BigQuery API
SCOPES = ['https://www.googleapis.com/auth/cloud-platform']

# Service account keys for authentication
SERVICE_ACCOUNT_KEYS = {
    "raw_layer_project": "/Users/duygugenc/Documents/de-ingestion-staging-layer-filtering/sambla-data-staging-compliance-gcs-handler.json"
}

PATH = os.path.dirname(os.path.abspath(__file__))

# Initialize BigQuery clients
clients = {}
for project_name, key_path in SERVICE_ACCOUNT_KEYS.items():
    if os.path.exists(key_path):
        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=SCOPES)
        credentials.refresh(Request()) 
        clients[project_name] = bigquery.Client(credentials=credentials)
    else:
        raise FileNotFoundError(f"Service account key file not found: {key_path}")

# Assign the BigQuery client for raw layer
raw_layer_client = clients['raw_layer_project']

# File paths
output_file_path = os.path.join(PATH,"generated_source_query.sql")
auth_view_query_mapping = os.path.join(PATH,"auth_view_mapping.txt")
print(auth_view_query_mapping)
encrpytion_template_file_path = os.path.join(PATH,"encryption_query_template.sql")


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

def generate_union_all_query():
    # Create the UNION ALL query statement based on all raw view/table sources included in staging compliance project
    # Note: Where statement should be altered to include all raw datasets ( maybe through a rule?)
    query = """
        WITH tables AS (SELECT
            table_schema
            FROM
            `sambla-data-staging-compliance`.`region-europe-north1`.INFORMATION_SCHEMA.TABLES
            WHERE
            table_schema  IN ("lvs_integration_legacy","rahalaitos_integration_legacy")
        )
            SELECT
            DISTINCT table_schema,
            CONCAT( "SELECT * FROM `sambla-data-staging-compliance.", table_schema, "`.INFORMATION_SCHEMA.COLUMNS" ) AS column_query
            FROM
            tables
    """
    return execute_query(raw_layer_client, query)

def save_union_all_query(union_all_query):
    with open(output_file_path, 'w') as f:
        f.write(union_all_query)

def read_encrypted_template():
    with open(encrpytion_template_file_path) as f:
        encrypted_query = Template(f.read())
        return encrypted_query

def generate_encryption_queries(encrypted_query_template):
    encryption_queries = []
    # Run the encryption query template
    encryption_query_result = execute_query(raw_layer_client, encrypted_query_template)
    # Iterate over each row produced by encryption query template query
    for row in encryption_query_result:
        print("ROW:",row)
        # For each generated encryption query, retrieve the schema and table information.
        # This helps us to understand the which schema and table the encryption query applies to.
        schema = row.table_schema
        table = row.table_name
        encryption_query = row.encrypted_columns
        # Save it to the list object
        encryption_queries.append(f"{schema}|{table}|{encryption_query}")

    # Save encryption queries to a file
    with open(auth_view_query_mapping, 'w') as f:
        for eq in encryption_queries:
            f.write(eq + "\n")


def main():
    """Main function to execute the workflow."""
    try:
        # Execute the query to retrieve all columns from each source table or view in the raw data layer.
        query_table_names_result = generate_union_all_query()
        union_all_queries = []
        
        # Combine each source query with UNION ALL to create a single cte with all raw data layer columns.
        # This will be used by `encryption_query_template.sql` to map sensitive columns by using policy tags.
        for row in query_table_names_result:
            union_all_queries.append(row.column_query)

        # Create the UNION ALL query statement by joining the each query with UNION ALL
        union_all_query = '\nUNION ALL \n'.join(union_all_queries)

        # Save the complete union all statement to a file
        save_union_all_query(union_all_query)

        # Pass the union all query cte to the `encryption_query_template.sql`
        encrypted_query_template = read_encrypted_template().render(
            query_table_columns = union_all_query
        )
        print(encrypted_query_template)

        # Generate and save mapping txt file that consists of encryption query and its source schema & table information.
        generate_encryption_queries(encrypted_query_template)

        logging.info("Workflow completed successfully.")

    except Exception as e:
        logging.error("An error occurred during the process.")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

