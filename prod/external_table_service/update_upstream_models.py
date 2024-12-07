import yaml
import os
import logging
from pathlib import Path
from utils.bigquery_utils import BigQueryUtils
from utils.schema_utils import SchemaUtils
from utils.policy_tags_utils import PolicyTagsUtils

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

def initialize_services(project, location, policy_tags_script_path, sql_template_path):
    """Initialize and return all services."""
    bigquery_service = BigQueryUtils(project, location)
    schema_service = SchemaUtils(project, location)
    policy_tags_service = PolicyTagsUtils(policy_tags_script_path, sql_template_path)
    return bigquery_service, schema_service, policy_tags_service

def process_external_data_info(
    external_data_info,
    base_schema_path,
    bigquery_service,
    schema_service,
    policy_tags_service
):
    """Process each entry in the external_data_info list."""
    for row in external_data_info:
        dataset = row.get('dataset_id')
        schema = row.get('schema')
        table_suffix = row.get('table_suffix')
        legacy_stack_name = row.get('legacy_stack')

        # Destination folder path
        schema_path = os.path.join(base_schema_path, legacy_stack_name, schema)

        # Extract tables with policy tags
        tables_with_policy_tags = policy_tags_service.extract_tables_with_policy_tags(
            base_schema_path, table_suffix, legacy_stack_name
        )

        # Generate queries for upstream models
        query_source = bigquery_service.generate_source_query_upstream_models(dataset)
        union_all_queries = [row["column_query"] for row in query_source]
        union_all_query = '\nUNION ALL \n'.join(union_all_queries)

        # Update policy assignment SQL template
        policy_tags_service.update_sql_template("append", union_all_query)

        # Run policy tags assignment script
        policy_tags_service.run_policy_tags_assignment_script()

        # Update BigQuery table schema
        schema_service.update_bigquery_table_schema(dataset, tables_with_policy_tags, schema_path)

        # Remove the chnages to sql template file 
        policy_tags_service.update_sql_template("remove", union_all_query)
    
   

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
    sql_template_path = base_path / external_table_config.get('sql_template_path')
    policy_tags_script_path = base_path / external_table_config.get('policy_tags_script_path')

    # Initialize services
    bigquery_service, schema_service, policy_tags_service = initialize_services(
        external_table_config.get('project'),
        external_table_config.get('location'),
        policy_tags_script_path,
        sql_template_path
    )

    # Process external data info
    external_data_info = external_table_config.get('external_data_info', [])
    process_external_data_info(
        external_data_info,
        base_schema_path,
        bigquery_service,
        schema_service,
        policy_tags_service
    )

if __name__ == "__main__":
    main("prod")
