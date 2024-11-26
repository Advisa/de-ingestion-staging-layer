from google.cloud import bigquery
import subprocess
import re
import os
from pathlib import Path
import json
import yaml

def get_legacy_stack_ext_table(project_id, dataset_id, location, gcs_bucket_name,sink_gcs_bucket_name):
    # Initialize the BigQuery client
    client = bigquery.Client(location=location, project=project_id)
    
    # SQL query to retrieve the names and DDL of external tables using the specified GCS bucket
    query = f"""
        SELECT table_name, ddl
        FROM `{project_id}.{dataset_id}`.INFORMATION_SCHEMA.TABLES 
        WHERE ddl LIKE '%gs://{gcs_bucket_name}/%' and table_name like "%_r";
    """
    print(query)
    try:
        # Run the query and store the results
        query_tables = client.query(query)
        results = query_tables.result()
        
        # Define lists to hold table names and GCS locations
        table_info = []

        # Regular expression to extract GCS paths from the DDL column values
        gcs_pattern = r'gs://[^"]+\*'


        for row in results:
            ddl = row.ddl
            # Extract all GCS locations from the DDL
            matches = re.findall(gcs_pattern, ddl)
            matches_adjusted = [
                    re.sub(f"gs://{gcs_bucket_name}", f"gs://{sink_gcs_bucket_name}", match)
                    for match in matches
                ]
            if matches_adjusted:
                gcs_location = matches_adjusted if len(matches_adjusted)==1 else matches_adjusted[0]
                table_name = row.table_name

                table_info.append({
                    "table_name":table_name,
                    "gcs_location":gcs_location
                })

        return table_info
    except Exception as e:
        print(f"An error occurred: {e}")
        return [], []
    

def generate_and_run_bq_commands(project, dataset, table_info,path):
    if table_info:
        for row in table_info:
            table_name = row.get('table_name')
            # Construct the bq command to retrieve the schema in pretty JSON format
            bq_command = f'bq show --format=prettyjson --schema {project}:{dataset}.{table_name} > {path}/{table_name}_schema.json'
            #print(f"Running command: {bq_command}")
            try:
                subprocess.run(bq_command, shell=True, check=True)
            except Exception as e:
                print(f"Failed to execute command: {bq_command}")
                print("Error:", e)
    else:
        print("No tables found.")

def generate_file_txt(file_name, table_info):
    if not table_info:
        print("No valid table information found to write.")
        return

    try:
        with open(file_name, 'w') as file:
            for entry in table_info:
                table_name = entry.get('table_name')
                #print(f"{table_name}:[],")
                gcs_locations = entry.get('gcs_location')
                # Write table name and GCS locations (comma-separated)
                gcs_locations_str = ",".join(gcs_locations) if gcs_locations else ""
                file.write(f"{table_name},{gcs_locations_str}\n")
        print(f"Generated file: {file_name}")
    except Exception as e:
        print(f" X An error occurred while writing the file: {e}")

def remove_partition_columns_from_schema(file_name,schema_folder_path):
    try:
        with open(file_name, 'r') as file:
            print("Reading file...")
            table_columns = {}

            for line in file:
                #print("Processing line:", line)
                # Use comma as the delimiter
                table_name, columns = line.strip().split(",", 1)
                # Safely parse the columns without eval
                columns_list = [
                    col.strip().strip("'\"")
                    for col in columns.strip()[1:-1].split(",")
                    if col.strip()
                ]
                table_columns[table_name.strip()] = columns_list
                #print("Parsed table columns:", table_columns)
        
        for table_name, columns_to_remove in table_columns.items():     
            schema_file = os.path.join(schema_folder_path, f"{table_name}_schema.json")
            print("schema:",schema_file)
            print(f"Table: {table_name} with columns to remove:",columns_to_remove)

            if os.path.exists(schema_file):
                with open(schema_file, 'r') as schema_file_obj:
                    schema_data = json.load(schema_file_obj)

                updated = False
                fields = []
                old = []
                for field in schema_data:
                    old.append(field)
                    if field["name"] not in columns_to_remove:
                        fields.append(field)
                        updated = True

                print(f"new json fields: {len(fields)} and old {len(old)}")       

                if updated:
                    print("Fields to write:", fields)
                    print(type(fields))
                    with open(schema_file, 'w') as schema_file:
                        json.dump(fields, schema_file, indent=4)
                    print(f"Updated schema for table: {table_name}")
            else:
                print(f"Schema file not found for table: {table_name}")


    except Exception as e:
        print(f"An error occurred while reading the file: {e}")   


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
    gcs_data_info = external_table_config.get('gcs_data_info')
    location = external_table_config.get('location')

    # Define the base path for schemas
    base_path = Path(__file__).resolve().parent.parent 
    base_schema_path =  base_path / "schemas"


    # Process each GCS bucket name
    for row in gcs_data_info:
        dataset_id = row.get('dataset_id')
        gcs_bucket_name = row.get('gcs_bucket_name')
        sink_gcs_bucket_name = row.get('sink_gcs_bucket_name')
        schema = row.get("schema")
        # print(f"Processing for: {dataset_id},{gcs_bucket_name}")
        schema_output_path = os.path.join(base_schema_path,schema)
        
        #Retrieve external table names and their corresponding GCS locations
        table_info = get_legacy_stack_ext_table(
           source_project, dataset_id, location, gcs_bucket_name,sink_gcs_bucket_name
        )

        
        # Generate and execute BigQuery commands to create JSON schema files for external tables
        generate_and_run_bq_commands(source_project, dataset_id, table_info,schema_output_path)
        
        # Generate a text file mapping table names to their GCS bucket locations
        output_file_name = f"{schema}_external_table_info.txt"
        output_template_path = os.path.join(base_path,"modules",schema, "bigquery",output_file_name)
        generate_file_txt(output_template_path, table_info)
        print(f"Generated file: {output_template_path}")

        """ Only uncomment and run this func when first we create the hive partitioned tables. Because it doesnt assign partition columns if it exist on schema """
        """ Once hive partition tables are created, we have to re-add the partition columns to the schema file. So comment this func below and run this whole py file once again."""
        #remove_partition_columns_from_schema("external_table_path_resolver/salus_partition_columns.txt",schema_output_path)
       

if __name__ == "__main__":
    main()

           