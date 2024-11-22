from google.cloud import bigquery
import subprocess
import os
from pathlib import Path
import sys


def generate_clone_statements(table_list, source_project, source_dataset, target_project, target_dataset):
    """
    Generate SQL statements to clone tables from a source dataset to a target dataset.

    Returns:
    list of str: List of SQL  statements.
    """
    clone_statements = []
    for table_name in table_list:
        target_table = f"{target_project}.{target_dataset}.{table_name}"
        source_table = f"{source_project}.{source_dataset}.{table_name}"
        statement = f"""
        CREATE OR REPLACE TABLE `{target_table}`
        CLONE `{source_table}`;
        """
        clone_statements.append(statement.strip())
    return clone_statements

def generate_schema_for_incremental_r_models(project, dataset,path, table_list):
    for table_name in table_list:
        # Construct the bq command to retrieve the schema in pretty JSON format
        bq_command = f'bq show --format=prettyjson --schema {project}:{dataset}.{table_name} > {path}/{table_name}_schema.json'
        print(f"Running command: {bq_command}")
        try:
            subprocess.run(bq_command, shell=True, check=True)
            print(f"Schema created for {dataset}.{table_name}")
        except Exception as e:
            print(f"Failed to execute command: {bq_command}")
            print("Error:", e)

def execute_sql_statements(sql_statements):
    """
    Executes a list of SQL statements on BigQuery.
    """
    # Initialize BigQuery client
    client = bigquery.Client()
    
    for sql in sql_statements:
        print(f"Executing:\n{sql}\n")
        # Execute the SQL statement
        query_job = client.query(sql)
        # Wait for the job to complete
        query_job.result()
        print("Execution completed successfully!\n")

def update_bigquery_table_schema(project, dataset, table_list, schema_path):
    """
    Updates a BigQuery table's schema using the `bq update` command.
    """
    for table_name in table_list:
        table_id = f"{project}:{dataset}.{table_name}"
        command = [
            "bq", "update", 
             table_id, 
           f"{schema_path}/{table_name}_schema.json"
        ]

        try:
            print(f"Executing command: {' '.join(command)}")
            # Execute the command
            print(command)
            subprocess.run(command, check=True)
            print(f"Table {table_name} schema updated successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error updating table {table_name}:{e}")
            raise

def run_main_py_script(main_script_path):
    """
    This function runs the main.py script located at the given path.
    """
    try:
        print(f"Running main.py script at {main_script_path}...")
        result = subprocess.run(
            ['python3', main_script_path],  # Command to execute main.py
            check=True,  # Raise an error if the command fails
            stdout=subprocess.PIPE,  # Capture standard output
            stderr=subprocess.PIPE   # Capture standard error
        )
        
        # Print the result of the script execution
        print(f"Script executed successfully. Output:\n{result.stdout.decode('utf-8')}")
    
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running main.py:\n{e.stderr.decode('utf-8')}")
        raise e  

# Example usage
table_list = [
    "applicants_salus_incremental_r",
    "accounts_salus_incremental_r",
    "applicant-jobs_salus_incremental_r"
]

gcs_data_info = [

        {
            "dataset_id":'salus_group_integration',
            "gcs_bucket_name": 'salus-integration',
            "sink_gcs_bucket_name": 'sambla-group-salus-integration-legacy',
            "schema":'salus'

        }
    ]
source_project = "data-domain-data-warehouse"
source_dataset = "salus_group_integration"
target_project = "sambla-data-staging-compliance"
target_dataset = "salus_integration_legacy"
base_schema_path =  Path(__file__).resolve().parent.parent  / "schemas"

# Generate SQL statements
sql_statements = generate_clone_statements(
    table_list, source_project, source_dataset, target_project, target_dataset
)


#execute_sql_statements(sql_statements)


for row in gcs_data_info:
    dataset_id = row.get('dataset_id')
    schema = row.get('schema')
    schema_path  = os.path.join(base_schema_path,schema,"incremental")
    print("Path:",schema_path)

    generate_schema_for_incremental_r_models(source_project,dataset_id,schema_path,table_list)

    run_main_py_script("/Users/duygugenc/Documents/current_de-ingestion-staging-layer/de-ingestion-staging-layer/prod/policy_tags_service/policy_assignment/main.py")
 
    update_bigquery_table_schema(target_project,target_dataset,table_list,schema_path)