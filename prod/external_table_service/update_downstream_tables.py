from google.cloud import bigquery
import subprocess
import os
from pathlib import Path
import sys
import yaml


def generate_schema_for_incremental_r_models(project, dataset,path, table_list):
    for row in table_list:
        table_name = row["table_name"]
        # Construct the bq command to retrieve the schema in pretty JSON format
        bq_command = f'bq show --format=prettyjson --schema {project}:{dataset}.{table_name} > {path}/{table_name}_schema.json'
        print(f"Running command: {bq_command}")
        try:
            subprocess.run(bq_command, shell=True, check=True)
            print(f"Schema created for {dataset}.{table_name}")
        except Exception as e:
            print(f"Failed to execute command: {bq_command}")
            print("Error:", e)

def execute_sql_statements(sql_statement,project_id,location):
    """
    Executes a list of SQL statements on BigQuery.
    """
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    
    print(f"Executing:\n{sql_statement}\n")
    # Execute the SQL statement
    query_job = client.query(sql_statement, location=location)
    # Wait for the job to complete
    rows = query_job.result()
    print("Execution completed successfully!\n")
    return [dict(row) for row in rows]

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

def run_policy_tags_assignment_script(main_script_path):
    """
    This function runs the main.py script located at the given path.
    """
    try:
        print(f"Running main.py script at {main_script_path}...")
        result = subprocess.run(
            ['python3', main_script_path],
            check=True,  
            stdout=subprocess.PIPE,  
            stderr=subprocess.PIPE   
        )
        
        # Print the result of the script execution
        print(f"Script executed successfully. Output:\n{result.stdout.decode('utf-8')}")
    
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running main.py:\n{e.stderr.decode('utf-8')}")
        raise e  


def load_config(config_path):
        """Load YAML configuration from the provided path."""
        print("config path:",config_path)
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            print("YAML configuration loaded successfully.")
            return config
        except FileNotFoundError as e:
            print(f"Config file not found: {str(e)}")
          
        except yaml.YAMLError as e:
            print(f"Error parsing YAML file: {str(e)}")

def generate_source_query_incremental_r(source_project,source_dataset,location):
        """Generate the UNION ALL query for all source tables/views."""
        query = f"""
            WITH tables AS (SELECT
                table_schema
                FROM
                `{source_project}`.`region-europe-north1`.INFORMATION_SCHEMA.TABLES
                WHERE
                table_schema IN ("{source_dataset}")
            )
                SELECT
                DISTINCT table_schema,
                CONCAT( "SELECT * FROM `{source_project}.", table_schema, "`.INFORMATION_SCHEMA.COLUMNS" ) AS column_query
                FROM
                tables
        """
        return execute_sql_statements(query,source_project,location)

def append_union_all_to_sql(file_path, union_all_statement):
    """
    Appends a UNION ALL clause after the placeholder {{source_table_columns}} in the SQL query.
    """
    placeholder = "{{source_table_columns}}"
    
    # Read the SQL file
    sql_file = Path(file_path)
    if not sql_file.exists():
        raise FileNotFoundError(f"The file '{file_path}' does not exist.")
    
    # Read the content of the file
    with sql_file.open("r") as file:
        sql_content = file.read()
    
    # Check if the placeholder exists in the content
    if placeholder not in sql_content:
        raise ValueError(f"The placeholder '{placeholder}' was not found in the SQL file.")
    
    # Create the UNION ALL section
    union_all_section = f"{placeholder}\n    UNION ALL\n    {union_all_statement}"
    
    # Replace the placeholder with the new UNION ALL section
    modified_sql_content = sql_content.replace(placeholder, union_all_section)
    
    # Write the modified content back to the file
    with sql_file.open("w") as file:
        file.write(modified_sql_content)
    
    print(f"File '{file_path}' has been successfully updated.")


def main(env):
    # Load the config
    config_path = Path.cwd() / "external_table_service/config.yaml"
    config = load_config(config_path)
    external_table_config = config.get(env, config.get('default', {})).get('external_table_service', {})
        
    if not external_table_config:
        print(f"Configuration for environment '{env}' or 'default' not found.")
        raise ValueError(f"Configuration for environment '{env}' not found.")
    
    # Extract values from the YAML config
    source_project = external_table_config.get('source_project')
    # Extract the external table mapping
    external_data_info = external_table_config.get('external_data_info')
    location = external_table_config.get('location')

    # Define the base path for schemas
    base_path = Path(__file__).resolve().parent.parent 
    base_schema_path =  base_path / "schemas"
    # Base path for policy tags service main.py
    base_policy_tags_service_path = base_path  / "policy_tags_service/policy_assignment/main.py"
    sql_template_path = base_path / "policy_tags_service/policy_assignment/templates/get_matching_sensitive_fields.sql"

    

    for row in external_data_info:
        print(row)
        source_dataset = row.get('source_dataset_id')
        schema = row.get('schema')
        schema_path  = os.path.join(base_schema_path,schema,"incremental")
        # SQL Statement to extract the Incremental R tables
        sql_statement = f"SELECT table_name FROM `{source_project}.{source_dataset}`.INFORMATION_SCHEMA.TABLES WHERE table_name like '%incremental_r'"
        # Store it
        table_list = execute_sql_statements(sql_statement,source_project,location)

        # generate a schema for the incremental r models, if it doesnt exist uncomment it
        #generate_schema_for_incremental_r_models(source_project,source_dataset,schema_path,table_list)

        # Generate a query to add incremental models as source for sensitive field mapping (policy tags assignment)
        query_source = generate_source_query_incremental_r(source_project,source_dataset,location)
       
        union_all_queries = [row["column_query"] for row in query_source]
        union_all_query = '\nUNION ALL \n'.join(union_all_queries)

        # modify the policy_assignment/templates/get_matching_sensitive_fields.sql template to include incremental r columns as source too
        append_union_all_to_sql(sql_template_path, union_all_query)

        run_policy_tags_assignment_script(base_policy_tags_service_path)
 
        # Uncomment it to update the production upstream model schemas
        #update_bigquery_table_schema(source_project,source_dataset,table_list,schema_path)



if __name__ == "__main__":
    try:
        main("dev")
    except Exception as e:
        print(f"An error occured:{e}")







    