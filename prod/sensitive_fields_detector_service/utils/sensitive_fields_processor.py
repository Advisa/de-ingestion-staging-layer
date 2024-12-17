import csv
import yaml
import json
import pickle
import os
import networkx as nx
from google.cloud import bigquery


class SensitiveFieldsProcessor:
    def __init__(self, config_file, input_csv, output_json, project_id):
        self.config_file = config_file
        self.input_csv = input_csv
        self.output_json = output_json
        self.project_id = project_id
        self.client = bigquery.Client(project=self.project_id)
        self.cache_file = 'schemas_cache.pkl'  # Pickle cache file

    def read_yaml_config(self):
        with open(self.config_file, "r") as file:
            return yaml.safe_load(file)

    def build_graph(self, config):
        # Normalize columns before adding them to the graph
        G = nx.Graph()
        processed_columns = set()

        for source_column, mappings in config.items():
            for target_column, mapping in mappings.items():
                source = self.preprocess_column(mapping.get("source_column"))
                target_column = self.preprocess_column(target_column)

                if source and target_column:
                    G.add_edge(target_column, source, model=mapping.get("source_model"))
                    processed_columns.add(source)
                    processed_columns.add(target_column)

        return G, processed_columns

    @staticmethod
    def preprocess_column(column):
        # Normalize column names by stripping prefixes and lowercasing
        if "." in column:
            return column.split(".")[-1].lower()
        return column.lower()

    @staticmethod
    def extract_keywords(connected_columns):
        # Extract keywords from the connected columns to be used for grouping
        keywords = set()
        for column in connected_columns:
            keywords.add(SensitiveFieldsProcessor.preprocess_column(column).lower())
        return keywords

    @staticmethod
    def keyword_based_grouping(connected_columns, config_columns):
        # Group columns based on keywords derived from connected columns
        grouped_columns = set()
        grouped_columns.update(connected_columns)
        keywords = SensitiveFieldsProcessor.extract_keywords(connected_columns)
        for column in config_columns:
            normalized_column = SensitiveFieldsProcessor.preprocess_column(column)
            if any(keyword in normalized_column.lower() for keyword in keywords):
                grouped_columns.add(column)
        return sorted(grouped_columns)

    @staticmethod
    def resolve_connections(G, legacy_column):
        """
        Resolve all connected columns for a given legacy column using the graph.
        """
        legacy_column = SensitiveFieldsProcessor.preprocess_column(legacy_column)
        connected_columns = set()
        if legacy_column not in G:
            raise ValueError(f"The legacy column '{legacy_column}' does not exist in the graph.")

        for node in nx.dfs_preorder_nodes(G, legacy_column):
            connected_columns.add(node)

        return connected_columns

    @staticmethod
    def clean_csv(input_csv):
        # Clean and read sensitive field data from the CSV file
        sensitive_fields = []
        with open(input_csv, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                legacy_column = row.get("legacy column name", "").strip()
                if legacy_column and legacy_column.lower() != 'null':
                    sensitive_fields.append(legacy_column)
        return sensitive_fields

    def get_all_bigquery_schemas(self):
        """
        Fetches the schema for all tables in all datasets of the given project,
        excluding datasets that start with 'dbt_', 'analytics_', or have names that are purely numeric.
        """
        # Check if the cache file exists and load it
        if os.path.exists(self.cache_file):
            print("Loading schemas from cache file.")
            try:
                return self.load_cache_from_file()
            except Exception as e:
                print(f"Error loading cache: {e}")
                print("Proceeding to fetch schemas from BigQuery.")
        
        schemas = {}

        # Prefixes to exclude
        exclude_datasets = {"dbt_", "analytics_","google_ads","data_science","derek","_airbyte","inbox_se","playgrounds","playgrounds_gdpr","raw_data_vault_integrations","test-","test_"}

        try:
            # List datasets in the project
            datasets = self.client.list_datasets()
            if not datasets:
                print(f"No datasets found in project {self.project_id}")
                return schemas

            for dataset in datasets:
                dataset_id = dataset.dataset_id

                # Skip datasets that start with specific prefixes or are entirely numeric
                if any(dataset_id.startswith(prefix) for prefix in exclude_datasets) or dataset_id.isdigit():
                    print(f"Skipping dataset: {dataset_id}")
                    continue

                print(f"Processing dataset: {dataset_id}")

                # List tables in the dataset
                tables = self.client.list_tables(dataset_id)
                for table in tables:
                    table_id = table.table_id
                    table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
                    print(f"  Processing table: {table_ref}")

                    # Get the schema for each table
                    table_obj = self.client.get_table(table_ref)
                    table_schema = {field.name: field.field_type for field in table_obj.schema}

                    schemas[table_ref] = table_schema

            # Cache the schemas to a file for future use
            self.save_cache_to_file(schemas)

        except Exception as e:
            print(f"Error fetching schemas: {e}")
        
        print(schemas)
        return schemas

    def save_cache_to_file(self, data):
        """
        Save the data (schemas) to a pickle file for caching.
        """
        try:
            with open(self.cache_file, "wb") as f:
                pickle.dump(data, f)
            print(f"Schemas cached to {self.cache_file}")
        except Exception as e:
            print(f"Error saving cache to file: {e}")

    def load_cache_from_file(self):
        """
        Load cached data (schemas) from a pickle file.
        """
        try:
            with open(self.cache_file, "rb") as f:
                cached_data = pickle.load(f)
                print(f"Loaded cached data successfully.")
                return cached_data
        except (EOFError, pickle.PickleError) as e:
            print(f"Error loading cache from file: {e}")
            return {}


    @staticmethod
    def convert_grouped_columns_to_json(grouped_columns, schemas, output_json):
        """
        Convert the grouped columns to a structured JSON format with sensitivity levels.
        """
        try:
            json_output = {
                "taxonomy_name": "gdpr_compliance_measures",
                "high_sensitivity_tags": {"PII": {}, "restricted": {}, "confidential": {}},
                "medium_sensitivity_tags": {"PII": {}, "restricted": {}, "confidential": {}},
                "low_sensitivity_tags": {"PII": {}, "restricted": {}, "confidential": {}},
            }

            processed_columns = set()

            # Filter out excluded columns upfront
            def filter_columns(columns):
                return [
                    column
                    for column in columns
                    if not SensitiveFieldsProcessor.is_excluded_column(
                        column, SensitiveFieldsProcessor.get_column_type(column, schemas)
                    )
                ]

            for legacy_column, columns in grouped_columns.items():
                # Apply filtering upfront to remove excluded columns
                filtered_columns = filter_columns(columns)

                # Process remaining columns
                for column in filtered_columns:
                    if column in processed_columns:
                        continue

                    sensitivity, category = SensitiveFieldsProcessor.categorize_column(column)
                    masking_rule = "HASH" if sensitivity == "high" else "MASK"

                    # Prepare children list with additional filtering
                    children = [
                        child
                        for child in filtered_columns
                        if child != column and child not in processed_columns
                    ]

                    # Create the tag data for the current column
                    tag_data = {
                        "children": children,
                        "sensitivity": sensitivity,
                        "masking_rule": masking_rule,
                        "category": category,
                    }

                    # Add to the appropriate sensitivity group
                    if sensitivity == "high":
                        json_output["high_sensitivity_tags"][category][column] = tag_data
                    elif sensitivity == "medium":
                        json_output["medium_sensitivity_tags"][category][column] = tag_data
                    else:
                        json_output["low_sensitivity_tags"][category][column] = tag_data

                    # Mark the current column and its children as processed
                    processed_columns.add(column)
                    processed_columns.update(tag_data["children"])

            # Write the output to the file after processing all columns
            with open(output_json, "w") as jf:
                json.dump(json_output, jf, indent=4)

            print(f"Successfully written grouped columns to {output_json}")

        except Exception as e:
            print(f"Error: {e}")

    @staticmethod
    def get_column_type(column, schemas):
        """
        Retrieve the type of the column from schemas.
        """
        for table_ref, schema in schemas.items():
            if column in schema:
                return schema[column].lower()  # Normalize to lowercase
        return None


    @staticmethod
    def is_excluded_column(column, column_type=None):
        """
        Checks if the column should be excluded based on its name or type.
        """
        # Normalize column name to lowercase
        normalized_column = column.lower()
        
        print(f"Checking exclusion for column: {normalized_column}")

        if normalized_column == "is_pep":
            return False 
        if normalized_column.startswith(("num_", "hashed_")):
            print(f"Excluding column {normalized_column} due to exclusion rules (starts with 'num_' or 'hashed_').")
            return True
        if "hashed_" in normalized_column:
            print(f"Excluding column {normalized_column} due to exclusion rules (contains  'hashed_').")
            return True
        if column_type and column_type in ["bool", "boolean"]:
            print(f"Excluding column {normalized_column} due to exclusion rules (BOOLEAN type).")
            return True
        
        return False


    @staticmethod
    def categorize_column(column):
        # Categorize the column based on known sensitivity rules
        if column in ["ssn", "email", "phone"]:
            return "high", "PII"
        elif column in ["first_name", "last_name"]:
            return "high", "restricted"
        elif column in ["tili"]:
            return "high", "confidential"
        elif column in ["education", "address", "marital_status", "net_income", "ytunnus"]:
            return "low", "PII"
        else:
            return "medium", "restricted"