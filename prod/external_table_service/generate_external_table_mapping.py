import yaml
import os
import logging
from pathlib import Path
from utils.bigquery_utils import BigQueryUtils
from utils.schema_utils import SchemaUtils
import re
import logging


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path):
    """Load YAML configuration from the provided path."""
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        logging.info("YAML configuration loaded successfully.")
        return config
    except FileNotFoundError as e:
        logging.error(f"Config file not found: {str(e)}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML file: {str(e)}")
        raise

def initialize_services(project, location):
    """Initialize and return all services."""
    bigquery_service = BigQueryUtils(project, location)
    schema_service = SchemaUtils(project, location)
    return bigquery_service, schema_service


def extract_external_table_info(bigquery_service,project_id, dataset_id, gcs_bucket_name,sink_gcs_bucket_name):

    # SQL query to retrieve the names and DDL of external tables using the specified GCS bucket
    query = f"""
        SELECT table_name, ddl
        FROM `{project_id}.{dataset_id}`.INFORMATION_SCHEMA.TABLES 
        WHERE ddl LIKE '%gs://{gcs_bucket_name}/%' and table_name like "%_r";
    """
    print(query)
    
    try:
        # Run the query and store the results
        results = bigquery_service.execute_query(query)
        
        # Define lists to hold table names and GCS locations
        table_info = []

        # Regular expression to extract GCS paths from the DDL column values
        gcs_pattern =  r'gs://[^"\[\],]+'
        #gcs_pattern = r'gs://[^"\[\],]+'
        # Regular expression to extract max_bad_records value
        max_bad_records_pattern = r'max_bad_records\s*=\s*(\d+)'
        format_pattern = r'format\s*=\s*"([^"]+)"'
        field_delimiter_pattern = r'field_delimiter\s*=\s*"([^"]+)"'

        for row in results:
            ddl = row.get('ddl')
            # Extract max_bad_records value if it exists
            match = re.search(max_bad_records_pattern, ddl)
            max_bad_records = int(match.group(1)) if match else 0  # Default to 0 if not found

            # Extract all GCS locations from the DDL
            matches = re.findall(gcs_pattern, ddl)
            matches_adjusted = [
                re.sub(f"gs://{gcs_bucket_name}", f"gs://{sink_gcs_bucket_name}", match) + "*" if "*" not in match else re.sub(f"gs://{gcs_bucket_name}", f"gs://{sink_gcs_bucket_name}", match)
                for match in matches
            ]
            format_match = re.search(format_pattern, ddl)
            field_delimiter_match = re.search(field_delimiter_pattern, ddl)
            file_format = format_match.group(1) if format_match else None
            field_delimiter = field_delimiter_match.group(1) if field_delimiter_match else None

            if matches_adjusted:
                gcs_location = matches_adjusted[0]
                table_name = row.get('table_name')
                print(table_name)

                table_info.append({
                    "table_name":table_name,
                    "gcs_location":gcs_location,
                    "max_bad_records": max_bad_records,
                    "file_format": file_format,
                    "field_delimiter": field_delimiter
                })
            else:
                print("table is not written:",ddl)

        return table_info
    except Exception as e:
        print(f"An error occurred: {e}")
        return [], []
    
def generate_file_txt(file_name, table_info):
    print(len(table_info))
    if not table_info:
        print("No valid table information found to write.")
        return

    try:
        with open(file_name, 'w') as file:
            for entry in table_info:
                table_name = entry.get('table_name')
                gcs_locations = entry.get('gcs_location')
                max_bad_records = entry.get('max_bad_records')
                file_format = entry.get('file_format')
                field_delimiter = entry.get('field_delimiter')
                # Write table name and GCS locations (comma-separated)
                file.write(f"{table_name},{gcs_locations},{max_bad_records}, {file_format}, {field_delimiter} \n")
        logging.info(f"Generated file: {file_name}")
    except Exception as e:
        logging.error(f"An error occurred while writing the file: {e}")
    
def process_gcs_data_info(project_id,gcs_data_info,base_schema_path,bigquery_service,schema_service,output_template_path,output_file_name):
    """Process each entry in the external_data_info list."""
    for row in gcs_data_info:
        dataset_id = row.get('dataset_id')
        gcs_bucket_name = row.get('gcs_bucket_name')
        sink_gcs_bucket_name = row.get('sink_gcs_bucket_name')
        schema = row.get("schema")
        print(f"Schema: {schema}")

        # Destination folder path
        schema_path = os.path.join(base_schema_path, schema)

        # Generate the correct file name without duplication
        output_file_name = f"{schema}_external_table_info.txt"

        # Correct output path for each schema
        output_path = os.path.join(output_template_path, schema, "bigquery", output_file_name)

        print(f"Output file name: {output_file_name}")
        print(f"Output file path: {output_path}")

        # Extract tables with names and their corresponding GCS locations
        table_info = extract_external_table_info(bigquery_service,project_id, dataset_id, gcs_bucket_name,sink_gcs_bucket_name)

        # Generate and execute BigQuery commands to create JSON schema files for tables
        #schema_service.export_schema( dataset_id, schema_path, table_info)

        # Generate a text file mapping table names to their GCS bucket locations
        generate_file_txt(output_path, table_info)


        """ Only uncomment and run this func when first we create the hive partitioned tables. Because it doesnt assign partition columns if it exist on schema """
        """ Once hive partition tables are created, we have to re-add the partition columns to the schema file. So comment this func below and run this whole py file once again."""
        #schema_service.remove_partition_columns(schema_path,"external_table_service/input/salus_partition_columns.txt")
       


def main(env):
    """Main entry point."""
    # Load configuration
    config_file_path = Path(__file__).resolve().parent / 'config.yaml'
    config = load_config(config_file_path)

    # Extract environment-specific configuration
    external_table_config = config.get(env, config.get('default', {})).get('external_table_service', {})
    if not external_table_config:
        print(f"Configuration for environment '{env}' or 'default' not found.")
        raise ValueError(f"Configuration for environment '{env}' not found.")

    # Paths
    base_path = Path(__file__).resolve().parent.parent
    base_schema_path = base_path / "schemas"

    # Initialize services
    bigquery_service, schema_service = initialize_services(
        external_table_config.get('project'),
        external_table_config.get('location')
    )

    output_template_path = os.path.join(base_path,"modules")
    # Process external data info
    external_data_info = external_table_config.get('gcs_data_info', [])
    process_gcs_data_info(
        external_table_config.get('project'),
        external_data_info,
        base_schema_path,
        bigquery_service,
        schema_service,
        output_template_path,
        external_table_config.get('output_file_name')
        
    )

if __name__ == "__main__":
    main("prod")