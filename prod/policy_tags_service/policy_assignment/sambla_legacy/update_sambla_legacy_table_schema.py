import subprocess
import os
from google.cloud import bigquery

# Define your variables
project_id = 'sambla-data-staging-compliance'
dataset_name = 'sambla_legacy_integration_legacy'
schema_directory = '/Users/aruldharani/Sambla/de-ingestion-staging-layer-1/prod/schemas/sambla_legacy/'

# Initialize BigQuery client
client = bigquery.Client(project=project_id)

# Fetch list of tables in the dataset
def get_table_names(dataset_name):
    try:
        dataset_ref = client.dataset(dataset_name)
        tables = client.list_tables(dataset_ref)  # List all tables in the dataset
        return [table.table_id for table in tables]
    except Exception as e:
        print(f"Error fetching tables: {e}")
        return []

# Function to update schema for a table
def update_table_schema(project_id, dataset_name, table_name, schema_file_path):
    # Ensure the schema file exists
    if not os.path.exists(schema_file_path):
        print(f"Schema file does not exist: {schema_file_path}")
        return

    # Construct the bq update command
    bq_command = [
        'bq', 'update',
        f'{project_id}:{dataset_name}.{table_name}',
        schema_file_path
    ]

    # Run the command and capture both stdout and stderr
    try:
        result = subprocess.run(bq_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(f"Successfully updated schema for {table_name}.")
        print("Output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error executing bq command: {e}")
        print("Error output:", e.stderr)
        print("Command:", ' '.join(bq_command))

# Loop through all tables and update their schemas
def update_schemas_for_all_tables():
    # Get all table names from the dataset
    table_names = get_table_names(dataset_name)

    if not table_names:
        print("No tables found in the dataset.")
        return

    # Loop through all tables and update their schema
    for table_name in table_names:
        schema_file_path = os.path.join(schema_directory, f"{table_name}_schema.json")
        update_table_schema(project_id, dataset_name, table_name, schema_file_path)

# Call the function to update schemas
update_schemas_for_all_tables()
