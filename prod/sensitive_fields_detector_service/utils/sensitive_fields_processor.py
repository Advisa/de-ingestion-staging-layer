import csv
import yaml
import json
import pickle
import os
import networkx as nx


class SensitiveFieldsProcessor:
    def __init__(self, config_file, input_csv, output_json, lineage_file):
        self.config_file = config_file
        self.input_csv = input_csv
        self.output_json = output_json
        self.lineage_file = lineage_file

    def read_yaml_config(self):
        with open(self.config_file, "r") as file:
            return yaml.safe_load(file)

    def build_graph(self, config):
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
        if "." in column:
            return column.split(".")[-1].lower()
        return column.lower()

    @staticmethod
    def extract_keywords(connected_columns):
        keywords = set()
        for column in connected_columns:
            keywords.add(SensitiveFieldsProcessor.preprocess_column(column).lower())
        return keywords

    @staticmethod
    def keyword_based_grouping(connected_columns, config_columns):
        grouped_columns = set(connected_columns)
        keywords = SensitiveFieldsProcessor.extract_keywords(connected_columns)
        for column in config_columns:
            normalized_column = SensitiveFieldsProcessor.preprocess_column(column)
            if any(keyword in normalized_column.lower() for keyword in keywords):
                grouped_columns.add(column)
        return sorted(grouped_columns)

    @staticmethod
    def resolve_connections(G, legacy_column):
        legacy_column = SensitiveFieldsProcessor.preprocess_column(legacy_column)
        connected_columns = set()
        if legacy_column not in G:
            raise ValueError(f"The legacy column '{legacy_column}' does not exist in the graph.")

        for node in nx.dfs_preorder_nodes(G, legacy_column):
            connected_columns.add(node)

        return connected_columns

    @staticmethod
    def clean_csv(input_csv):
        sensitive_fields = []
        with open(input_csv, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                legacy_column = row.get("legacy column name", "").strip()
                if legacy_column and legacy_column.lower() != 'null':
                    sensitive_fields.append(legacy_column)
        return sensitive_fields

    def load_lineage_data(self):
        """
        Load column lineage data from the lineage file.
        """
        try:
            with open(self.lineage_file, "r") as file:
                return json.load(file)
        except Exception as e:
            print(f"Error loading lineage data: {e}")
            return {}

    @staticmethod
    def convert_grouped_columns_to_json(grouped_columns, lineage_data, output_json):
        try:
            json_output = {
                "taxonomy_name": "gdpr_compliance_measures",
                "high_sensitivity_tags": {"PII": {}, "restricted": {}, "confidential": {}},
                "medium_sensitivity_tags": {"PII": {}, "restricted": {}, "confidential": {}},
                "low_sensitivity_tags": {"PII": {}, "restricted": {}, "confidential": {}},
            }

            processed_columns = set()

            def filter_columns(columns):
                return [
                    column
                    for column in columns
                    if not SensitiveFieldsProcessor.is_excluded_column(
                        column, SensitiveFieldsProcessor.get_column_type(column, lineage_data)
                    )
                ]

            for legacy_column, columns in grouped_columns.items():
                filtered_columns = filter_columns(columns)

                for column in filtered_columns:
                    if column in processed_columns:
                        continue

                    sensitivity, category = SensitiveFieldsProcessor.categorize_column(column)
                    masking_rule = "HASH" if sensitivity == "high" else "MASK"

                    children = [
                        child
                        for child in filtered_columns
                        if child != column and child not in processed_columns
                    ]

                    tag_data = {
                        "children": children,
                        "sensitivity": sensitivity,
                        "masking_rule": masking_rule,
                        "category": category,
                    }

                    if sensitivity == "high":
                        json_output["high_sensitivity_tags"][category][column] = tag_data
                    elif sensitivity == "medium":
                        json_output["medium_sensitivity_tags"][category][column] = tag_data
                    else:
                        json_output["low_sensitivity_tags"][category][column] = tag_data

                    processed_columns.add(column)
                    processed_columns.update(tag_data["children"])

            with open(output_json, "w") as jf:
                json.dump(json_output, jf, indent=4)

            print(f"Successfully written grouped columns to {output_json}")

        except Exception as e:
            print(f"Error: {e}")

    @staticmethod
    def get_column_type(column, lineage_data):
        """
        Retrieve the type of the column from the lineage data.
        """
        return lineage_data.get(column, {}).get("type", None)

    @staticmethod
    def is_excluded_column(column, column_type=None):
        normalized_column = column.lower()

        print(f"Checking exclusion for column: {normalized_column}")

        if normalized_column == "is_pep":
            return False
        if normalized_column.startswith(("num_", "hashed_")) or "hashed_" in normalized_column:
            return True
        if column_type and column_type in ["bool", "boolean"]:
            return True

        return False

    @staticmethod
    def categorize_column(column):
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
