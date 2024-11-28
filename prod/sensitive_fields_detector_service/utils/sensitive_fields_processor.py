# utils/sensitive_fields_processor.py
import csv
import yaml
import json
import networkx as nx
import os

class SensitiveFieldsProcessor:
    def __init__(self, config_file, input_csv, output_json):
        # Initialize with file paths for configuration, input CSV, and output JSON
        self.config_file = config_file
        self.input_csv = input_csv
        self.output_json = output_json

    def read_yaml_config(self):
        # Read and return the YAML configuration
        with open(self.config_file, "r") as file:
            return yaml.safe_load(file)

    def build_graph(self, config):
        # Build a graph based on column connections from the YAML configuration
        G = nx.Graph()
        processed_columns = set()

        for source_column, mappings in config.items():
            for target_column, mapping in mappings.items():
                source = mapping.get("source_column")
                if source:
                    source = self.preprocess_column(source)
                    target_column = self.preprocess_column(target_column)
                    G.add_edge(target_column, source, model=mapping.get("source_model"))
                    processed_columns.add(source)
                    processed_columns.add(target_column)

        processed_columns = {self.preprocess_column(column) for column in processed_columns}
        return G, processed_columns

    @staticmethod
    def preprocess_column(column):
        # Remove prefix before the dot (if any) to normalize the column name
        if "." in column:
            return column.split(".")[-1]
        return column

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
        # Resolve all connected columns in the graph using DFS
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

    @staticmethod
    def convert_grouped_columns_to_json(grouped_columns, output_json):
        # Convert the grouped columns to a structured JSON format with sensitivity levels
        try:
            json_output = {
                "taxonomy_name": "gdpr_complaince_measures",
                "high_sensitivity_tags": {
                    "PII": {},
                    "restricted": {},
                    "confidential": {}
                },
                "medium_sensitivity_tags": {
                    "PII": {},
                    "restricted": {},
                    "confidential": {}
                },
                "low_sensitivity_tags": {
                    "PII": {},
                    "restricted": {},
                    "confidential": {}
                }
            }

            for legacy_column, columns in grouped_columns.items():
                for column in columns:
                    sensitivity, category = SensitiveFieldsProcessor.categorize_column(column)
                    masking_rule = "HASH" if sensitivity == "high" else "MASK"
                    tag_data = {
                        "children": columns,
                        "sensitivity": sensitivity,
                        "masking_rule": masking_rule,
                        "category": category
                    }

                    # Place grouped columns under the appropriate sensitivity tags
                    if sensitivity == "high":
                        json_output["high_sensitivity_tags"][category][column] = tag_data
                    elif sensitivity == "medium":
                        json_output["medium_sensitivity_tags"][category][column] = tag_data
                    else:
                        json_output["low_sensitivity_tags"][category][column] = tag_data

            # Write the generated JSON to the output file
            with open(output_json, 'w') as jf:
                json.dump(json_output, jf, indent=4)

            print(f"Successfully written grouped columns to {output_json}")
        except Exception as e:
            print(f"Error: {e}")

    @staticmethod
    def categorize_column(column):
        # [TODO] Implement Dynamic approach Categorize the column based on known sensitivity rules
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

class Main:
    def __init__(self):
        # Set file paths for input, config, and output
        current_dir = os.path.dirname(os.path.realpath(__file__))
        parent_dir = os.path.dirname(current_dir)
        
        self.config_file = os.path.join(parent_dir, "output_files/column_lineage.yml")
        self.input_csv = os.path.join(parent_dir, "data/sensitive_field_mapping.csv")  # [TODO] retrieve from bucket or googlesheet in the future
        self.output_json = os.path.join(parent_dir, "output_files/resolved_columns.json") 

        # Initialize the SensitiveFieldsProcessor with file paths
        self.processor = SensitiveFieldsProcessor(self.config_file, self.input_csv, self.output_json)

    def run(self):
        # Clean and read sensitive fields from the CSV file
        sensitive_fields = self.processor.clean_csv(self.input_csv)

        # Read YAML configuration
        config = self.processor.read_yaml_config()

        # Build graph and get processed columns
        G, processed_columns = self.processor.build_graph(config)

        all_grouped_columns = {}
        for legacy_column in sensitive_fields:
            try:
                # Resolve connections for each legacy column
                connected_columns = self.processor.resolve_connections(G, legacy_column)

                # Group columns based on derived keywords
                grouped_columns = self.processor.keyword_based_grouping(connected_columns, processed_columns)
                all_grouped_columns[legacy_column] = grouped_columns
            except ValueError as e:
                print(f"Skipping legacy column '{legacy_column}': {e}")

        # Convert the grouped columns into JSON format and save to output file
        self.processor.convert_grouped_columns_to_json(all_grouped_columns, self.output_json)


if __name__ == "__main__":
    # Create an instance of Main and run the process
    main_obj = Main()
    main_obj.run()

