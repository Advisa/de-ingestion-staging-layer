import os
import json
import logging
from google.cloud import bigquery
import yaml
import glob
from pathlib import Path
import pandas as pd
import re
import csv



# Extract values from the configuration
project_id = "sambla-data-staging-compliance"
policy_tags_table = "policy_tags_metadata.policy_tags"
# Initialize the current path where this service is located
project_root = Path(__file__).resolve().parent.parent
current_path = os.path.dirname(os.path.abspath(__file__))  
base_path = project_root
schema_folder_path = project_root.parent / 'schemas'


        

def load_config(config_path):
    """Load YAML configuration from the provided path."""
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        logging.info("YAML configuration loaded successfully.")
        return config
    except FileNotFoundError as e:
        logging.error(f"Config file not found: {str(e)}")
        raise e
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML file: {str(e)}")
        raise e
    
def initialize_bigquery_clients():
    """Initialize BigQuery Client."""
    try:
        client = bigquery.Client(project=project_id)
        return client
    except Exception as e:
        logging.error(f"Error initializing BigQuery clients: {str(e)}")
        raise e
    

def normalize_name(name):
    """Normalize the name by converting to lowercase and removing underscores."""
    return name.lower().replace("_", "")


def extract_field_names(csv_file_path):
    """Extract field names from CSV file after 'PII,'"""
    field_names = []
    try:
        with open(csv_file_path, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header
            for row in reader:
                if len(row) > 1 :
                    field_names.append(normalize_name(row[2]))
        return field_names
    except Exception as e:
        logging.error(f"Error reading CSV file: {str(e)}")
        return []

def fetch_policy_tags_and_apply_to_datasets(csv_file_path,output_csv_path):
    datasets = [
        "sambla-data-staging-compliance.advisa_history_integration_legacy",
        "sambla-data-staging-compliance.salus_integration_legacy",
        "sambla-data-staging-compliance.lvs_integration_legacy",
        "sambla-data-staging-compliance.rahalaitos_integration_legacy",
        "sambla-data-staging-compliance.sambla_legacy_integration_legacy",
        "sambla-data-staging-compliance.maxwell_integration_legacy",
        "data-domain-data-warehouse.sambla_new_mongodb",
        "data-domain-data-warehouse.sambla_group_data_stream",
        "data-domain-data-warehouse.helios_staging",
        "data-domain-data-warehouse.helios_model",
        "data-domain-data-warehouse.helios_history",
        "data-domain-data-warehouse.helios_dm_master_bi",
        "data-domain-data-warehouse.helios_dm_master",
        "data-domain-data-warehouse.helios_dm_finance",
        "data-domain-data-warehouse.helios_exploration",
        "data-domain-data-warehouse.data_science",
        "data-domain-data-warehouse.google_sheet_export",
        "data-domain-data-warehouse.sambla_group_reversed_etl",
        "data-domain-data-warehouse.helios_dm_master_sensitive"
    ]
    
    client = initialize_bigquery_clients()
    sensitive_fields = extract_field_names(csv_file_path)
    print(len(sensitive_fields))

    
    with open(output_csv_path, 'w', newline='') as csvfile:
         fieldnames = ["Dataset", "Table", "Field", "Data Type"]
         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
         writer.writeheader()

         for dataset_id in datasets:
            try:
                query = f"""
                SELECT table_name, table_schema, field_path AS column_name, data_type
                FROM `{dataset_id}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
                WHERE data_type NOT LIKE "STRUCT%" AND data_type NOT LIKE "ARRAY%" AND data_type!="STRING" 
                """
                query_job = client.query(query)
                results = query_job.result()
                for row in results:
                    column_parts = row.column_name.split(".")
                    last_part = column_parts[-1] 
                    normalized_column_name = normalize_name(last_part)
  
                    if normalized_column_name in sensitive_fields:
                        writer.writerow({
                            "Dataset": dataset_id,
                            "Table": row.table_name,
                            "Field": row.column_name,
                            "Data Type": row.data_type
                        })
            except Exception as e:
                continue

# Get the current directory where the script is located
current_dir = os.path.dirname(os.path.abspath(__file__))

# Assuming 'prod/tests' is the relative path from the script's location
input_csv = os.path.join(current_dir, "outputs/sha256_fields.csv")
output_csv = os.path.join(current_dir, "outputs/results.csv")

fetch_policy_tags_and_apply_to_datasets(input_csv,output_csv)