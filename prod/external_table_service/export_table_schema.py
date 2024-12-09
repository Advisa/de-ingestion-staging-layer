import yaml
import os
import logging
from pathlib import Path
from utils.bigquery_utils import BigQueryUtils
from utils.schema_utils import SchemaUtils

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

def process_gcs_data_info(gcs_data_info,base_schema_path,bigquery_service,schema_service):
    """Process each entry in the external_data_info list."""
    for row in gcs_data_info:
        dataset_id = row.get('dataset_id')
        schema = row.get("schema")

        # Destination folder path
        schema_path = os.path.join(base_schema_path, schema)

        # Extract tables with names and their corresponding GCS locations
        table_info = bigquery_service.get_list_of_tables(dataset_id,schema)

        # Generate and execute BigQuery commands to create JSON schema files for tables
        schema_service.export_schema( dataset_id, schema_path, table_info)


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

    # Process external data info
    external_data_info = external_table_config.get('gcs_data_info', [])
    process_gcs_data_info(
        external_data_info,
        base_schema_path,
        bigquery_service,
        schema_service
    )

if __name__ == "__main__":
    main("prod")