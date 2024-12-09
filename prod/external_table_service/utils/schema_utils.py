import json
import subprocess
import os
from utils.bigquery_utils import BigQueryUtils
import logging


class SchemaUtils:
    def __init__(self, project, location):
        # Initialize the parameters
        self.project = project
        self.location = location

        self.bigquery_service = BigQueryUtils(project,location)

    def export_schema(self, dataset, output_path, table_list):
        for row in table_list:
            table_name = row["table_name"]
            command = f"bq show --format=prettyjson --schema {self.project}:{dataset}.{table_name} > {output_path}/{table_name}_schema.json"
            try:
                if self.bigquery_service.is_table_exists(dataset, table_name):
                    subprocess.run(command, shell=True, check=True)
                    logging.info(f"Schema exported for {dataset}.{table_name} in location: {output_path}")
                else:
                    logging.info("Table doesn't exist, schema export is cancelled")
            except Exception as e:
                logging.error(f"Error exporting schema for {dataset}.{table_name}: {e}")
                raise e
            
    def update_bigquery_table_schema(self, dataset, table_list, schema_path):
        """
        Updates a BigQuery table's schema using the `bq update` command.
        """
        for row in table_list:
            table_name = row["table_name"]
            table_id = f"{self.project}:{dataset}.{table_name}"
            command = [
                "bq", "update", 
                table_id, 
            f"{schema_path}/{table_name}_schema.json"
            ]

            try:
                if self.bigquery_service.is_table_exists(dataset, table_name):
                    # Execute the command
                    subprocess.run(command, check=True)
                    logging.info(f"Table {table_name} schema updated successfully.")
                else:
                    logging.info("Table doesn't exist, schema update is cancelled")
            except subprocess.CalledProcessError as e:
                logging.error(f"Error updating table {table_name}:{e}")
                raise

    
    def remove_partition_columns(self,schema_folder_path, file_name):
        """Remove hive partition columns from schema JSON files."""
        print(schema_folder_path,file_name)
        try:
            with open(file_name, 'r') as file:
                table_columns = {}
                for line in file:
                    table_name, columns = line.strip().split(",", 1)
                    columns_list = [
                        col.strip().strip("'\"") 
                        for col in columns.strip()[1:-1].split(",") 
                        if col.strip()
                    ]
                    table_columns[table_name.strip()] = columns_list

            for table_name, columns_to_remove in table_columns.items():
                schema_file = os.path.join(schema_folder_path, f"{table_name}_schema.json")
                if os.path.exists(schema_file):
                    with open(schema_file, 'r') as schema_file_obj:
                        schema_data = json.load(schema_file_obj)
                    updated = False
                    fields = [
                        field for field in schema_data 
                        if field["name"] not in columns_to_remove
                    ]
                    if len(fields) != len(schema_data):
                        updated = True
                    if updated:
                        with open(schema_file, 'w') as schema_file:
                            json.dump(fields, schema_file, indent=4)
                        logging.info(f"Updated schema for table: {table_name}")
                else:
                    logging.info(f"Schema file not found for table: {table_name}")
        except Exception as e:
            logging.error(f"An error occurred while reading the file: {e}")