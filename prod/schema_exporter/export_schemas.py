import yaml
import os
import logging
from google.cloud import bigquery
from pathlib import Path
import subprocess
from google.cloud.exceptions import NotFound

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def is_table_exists(project, dataset, table):
    """ Check if table actually exists in BigQuery"""
    table_id = f"{project}.{dataset}.{table}"
    client = bigquery.Client(project=project)
    try:
        client.get_table(table_id)  
        return True
    except NotFound:
        return False

def export_schema(project, dataset, output_path, table_list):
    for row in table_list:
        table_name = row["table_name"]
        command = f"bq show --format=prettyjson --schema {project}:{dataset}.{table_name} > {output_path}/{table_name}_schema.json"
        try:
            if is_table_exists(project, dataset, table_name):
                subprocess.run(command, shell=True, check=True)
                logging.info(f"Schema exported for {dataset}.{table_name}")
            else:
                logging.info("Table doesn't exist, schema export is cancelled")
        except Exception as e:
            logging.error(f"Error exporting schema for {dataset}.{table_name}: {e}")
            raise e
            
def update_bigquery_table_schema(project, dataset, table_list, schema_path):
    """
    Updates a BigQuery table's schema using the `bq update` command.
    """
    for row in table_list:
        table_name = row["table_name"]
        table_id = f"{project}:{dataset}.{table_name}"
        command = [
            "bq", "update", 
            table_id, 
        f"{schema_path}/{table_name}_schema.json"
        ]
        try:
            if is_table_exists(project, dataset, table_name):
                # Execute the command
                subprocess.run(command, check=True)
                logging.info(f"Table {table_name} schema updated successfully.")
            else:
                logging.info("Table doesn't exist, schema update is cancelled")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error updating table {table_name}:{e}")
            raise

def get_list_of_tables(project, dataset, legacy_stack, suffix, exception_table):
    table_list = []
    client = bigquery.Client(project=project)
    tables = client.list_tables(dataset)
    for table in tables :
        if table.table_id.endswith(suffix) and table.table_id!=exception_table:
            table_list.append({"legacy_stack": legacy_stack, "table_name": table.table_id})
    return table_list

def run(project, dataset_id, schema, base_schema_path, suffix, exception_table):
    """Process each entry in the external_data_info list."""

    # Destination folder path
    schema_path = os.path.join(base_schema_path, schema)
    # Extract tables with names and their corresponding GCS locations
    table_info = get_list_of_tables(project, dataset_id, schema, suffix, exception_table)
    # Generate and execute BigQuery commands to create JSON schema files for tables
    export_schema( project, dataset_id, schema_path, table_info)


def main():
    """Main entry point."""
    # Specify configuration for LVS
    project_id = "data-domain-data-warehouse"
    dataset_id = "helios_staging"
    schema = "lvs/p_layer"
    exception_table =  "funnel_data_lvs_p"
    suffix = "_lvs_p"
    
    # Paths
    base_path = Path(__file__).resolve().parent.parent
    base_schema_path = base_path / "schemas"


    # run
    run(project_id, dataset_id, schema, base_schema_path, suffix, exception_table)

if __name__ == "__main__":
    main()