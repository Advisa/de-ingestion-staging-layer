import csv
import yaml
import json
import networkx as nx


class SensitiveFieldsProcessor:
    def __init__(self, config_file, input_csv, output_json, lineage_file, exclusion_file, column_mapping_file):
        self.config_file = config_file
        self.input_csv = input_csv
        self.output_json = output_json
        self.lineage_file = lineage_file
        self.exclusion_file = exclusion_file
        self.column_mapping_file = column_mapping_file

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
        print(f"Extracted keywords: {keywords}")
        for column in config_columns:
            normalized_column = SensitiveFieldsProcessor.preprocess_column(column)
            print(f"Normalized column: {normalized_column}")
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

    def load_exclusion_list(self, exclusion_file):
        """
        Load exclusion list from the exclusion.txt file.
        """
        excluded_columns = set()
        if exclusion_file:
            with open(exclusion_file, "r") as f:
                excluded_columns = {line.strip().lower() for line in f}
        return excluded_columns

    def load_column_mapping(self, column_mapping_file):
        """
        Load column mapping from the column_mapping.json file.
        """
        column_mapping = {}
        if column_mapping_file:
            with open(column_mapping_file, "r") as f:
                column_mapping = json.load(f)
        return column_mapping
    
    @staticmethod
    def swap_keys_with_children(json_output, column_mapping):
        key_list = [
            "ssn", "first_name", "last_name", "email", "phone", "bank_account_number",
            "dob", "amount", "business_id", "citizenship", "employer", "gross_income",
            "post_code", "profession", "address", "education", "marital_status", "business_organization_number"
        ]

        for sensitivity_category, category_data in json_output.items():
            if sensitivity_category == "taxonomy_name":
                continue

            if isinstance(category_data, dict):
                for category, category_content in category_data.items():
                    if isinstance(category_content, dict):
                        changes = []
                        for column, tag_data in list(category_content.items()):
                            if "children" in tag_data:
                                for child in tag_data["children"]:
                                    if child in key_list:
                                        mapped_key = child

                                        # Only proceed if column exists
                                        if column in category_content:
                                            changes.append((column, mapped_key, category_content[column]))

                                        # Ensure the mapped key has children initialized
                                        if mapped_key in category_content:
                                            if "children" not in category_content[mapped_key]:
                                                category_content[mapped_key]["children"] = {}
                                            category_content[mapped_key]["children"][column] = tag_data

                        # Apply the changes after iteration
                        for column, mapped_key, column_data in changes:
                            # Remove the original column and add it under the new key
                            del category_content[column]
                            category_content[mapped_key] = column_data

                            # Ensure the column is added as a child of the mapped key
                            if "children" not in category_content[mapped_key]:
                                category_content[mapped_key]["children"] = {}
                            category_content[mapped_key]["children"][column] = {
                                "sensitivity": column_data.get("sensitivity"),
                                "masking_rule": column_data.get("masking_rule"),
                                "type": column_data.get("type", None),
                            }

                            print(f"Swapped {column} with child {mapped_key} and added {column} as child of {mapped_key}.")
        return json_output



    
    @staticmethod
    def convert_grouped_columns_to_json(grouped_columns, lineage_data, output_json, excluded_columns, column_mapping):
        try:
            json_output = {
                "taxonomy_name": "gdpr_compliance_measures",
                "high_sensitivity_tags": {"PII": {}, "restricted": {}, "confidential": {}},
                "medium_sensitivity_tags": {"PII": {}, "restricted": {}, "confidential": {}},
                "low_sensitivity_tags": {"PII": {}, "restricted": {}, "confidential": {}},
            }

            processed_columns = set()

            def filter_columns(columns):
                """
                Filter out the columns based on exclusion rules.
                """
                filtered_columns = []
                for column in columns:
                    if column in excluded_columns:
                        print(f"Excluding column: {column} due to exclusion list.")
                    else:
                        column_type = SensitiveFieldsProcessor.get_column_type(column, lineage_data)
                        if SensitiveFieldsProcessor.is_excluded_column(column, column_type,lineage_data):
                            print(f"Excluding column: {column} due to exclusion rules.")
                        else:
                            print(f"Including column: {column}")
                            filtered_columns.append(column)
                return filtered_columns
            
            def get_masking_rule(child, lineage_data):
                # Determine masking rule for the child column
                sensitivity, _ = SensitiveFieldsProcessor.categorize_column(child)
                column_type = SensitiveFieldsProcessor.get_column_type(child, lineage_data)

                if column_type in ("INT64", "FLOAT64"):
                        return "ALWAYS_NULL"
                else:
                    if sensitivity == "high":
                        return "SHA256"
                return "DEFAULT_MASKING_VALUE"

            for legacy_column, columns in grouped_columns.items():
                # Filter the current columns based on the exclusion list
                #filtered_columns = filter_columns(columns)

                # Check for additional columns from column_mapping that should be added as children
                if legacy_column in column_mapping:
                    # Add children from the column mapping (e.g., userName, person, name)
                    additional_columns = column_mapping.get(legacy_column, [])
                    for column in additional_columns:
                        if column not in columns and column not in processed_columns:
                            columns.append(column)
                filtered_columns = filter_columns(columns)

                # Check for additional columns from column_mapping that should be added as children
                if legacy_column in column_mapping:
                    # Add children from the column mapping (e.g., userName, person, name)
                    additional_columns = column_mapping.get(legacy_column, [])
                    for column in additional_columns:
                        if column not in columns and column not in processed_columns:
                            columns.append(column)
                filtered_columns = filter_columns(columns)

                for column in filtered_columns:
                    if column in processed_columns:
                        continue

                    # Extract data type of the column
                    column_type = SensitiveFieldsProcessor.get_column_type(column, lineage_data)
                    sensitivity, category = SensitiveFieldsProcessor.categorize_column(column)

                    # handling numeric columns explicitiy 
                    masking_rule = "SHA256" if sensitivity == "high" and column_type not in ("INT64","FLOAT64") else "ALWAYS_NULL" if column_type in ("INT64", "FLOAT64")  else "DEFAULT_MASKING_VALUE"

                    # Find children (all the filtered columns that are not the current column)
                    # children = {
                    #     child
                    #     for child in filtered_columns
                    #     if child != column and child not in processed_columns
                    # }

                    children = {
                        child: {
                                "sensitivity": SensitiveFieldsProcessor.categorize_column(child)[0],
                                "masking_rule": get_masking_rule(child, lineage_data),
                                "type": SensitiveFieldsProcessor.get_column_type(child, lineage_data)
                             }
                        for child in filtered_columns
                        if child != column and child not in processed_columns

                    }


                    tag_data = {
                        "children": children,
                        "sensitivity": sensitivity,
                        "masking_rule": masking_rule,
                        "type": SensitiveFieldsProcessor.get_column_type(column, lineage_data),
                        "category": category,
                    }

                    # Add the tag data to the correct sensitivity category
                    if sensitivity == "high":
                        json_output["high_sensitivity_tags"][category][column] = tag_data
                    elif sensitivity == "medium":
                        json_output["medium_sensitivity_tags"][category][column] = tag_data
                    else:
                        json_output["low_sensitivity_tags"][category][column] = tag_data
                    
                    processed_columns.add(column)
                    processed_columns.update(tag_data["children"])

            json_output = SensitiveFieldsProcessor.swap_keys_with_children(json_output, column_mapping)

            with open(output_json, "w") as jf:
                json.dump(json_output, jf, indent=4)

            print(f"Successfully written grouped columns to {output_json}")

        except Exception as e:
            print(f"Error: {e}")


    @staticmethod
    def get_column_type(column, lineage_data):
        """
        Retrieve the type of the column from the lineage data.
        The column name is normalized by splitting by '.' and using the last part to look it up in model_data.
        """
        normalized_column = column.split('.')[-1].lower()
        for model, model_data in lineage_data.items():
            for col in model_data:
                normalized_col = col.split('.')[-1].lower()
                if normalized_column == normalized_col:
                    column_info = model_data[col]
                    column_type = column_info.get("source_datatype") or column_info.get("target_datatype")
                    return column_type

        print(f"Column '{normalized_column}' not found in lineage data.")
        return None


    @staticmethod
    def is_excluded_column(column, column_type,lineage_data):
        """
        Check if a column should be excluded based on its type or name pattern.
        """
        normalized_column = column.lower()
        print(f"Checking exclusion for column: {normalized_column}, Type: {column_type}")

        if normalized_column.startswith(("num_", "hashed_")) or "hashed_" in normalized_column:
            return True

        if column_type is None:
            return False  
        
        #these are boolean but we still need them
        if normalized_column == "is_pep" or normalized_column == "birth_date" or normalized_column == "politicallyexposedperson":
            return False
        
        if isinstance(column_type, str) and column_type.startswith("STRUCT"):
            struct_fields = SensitiveFieldsProcessor.extract_struct_fields(column_type)  
            for field in struct_fields:
                field_type = SensitiveFieldsProcessor.get_column_type(field,lineage_data)  
                if field_type in ["bool", "boolean", "timestamp"]:
                    print(f"Excluding {field} due to type: {field_type}")
                    return True

        if isinstance(column_type, str) and column_type.startswith("ARRAY<STRUCT"):
            array_struct_fields = SensitiveFieldsProcessor.extract_array_struct_fields(column_type)  
            for field in array_struct_fields:
                field_type = SensitiveFieldsProcessor.get_column_type(field,lineage_data)  
                if field_type in ["bool", "boolean", "timestamp"]:
                    print(f"Excluding {field} due to type: {field_type}")
                    return True

        if column_type and column_type.lower() in ["bool", "boolean", "timestamp"]:
            print(f"Excluding column: {normalized_column} due to type: {column_type}")
            return True

        return False

    @staticmethod
    def get_column_type(column, lineage_data):
        """
        Retrieve the type of the column from the lineage data.
        The column name is normalized by splitting by '.' and using the last part to look it up in model_data.
        """
        normalized_column = column.split('.')[-1].lower()
        for model, model_data in lineage_data.items():
            for col in model_data:
                normalized_col = col.split('.')[-1].lower()
                if normalized_column == normalized_col:
                    column_info = model_data[col]
                    column_type = column_info.get("source_datatype") or column_info.get("target_datatype")
                    return column_type

        print(f"Column '{normalized_column}' not found in lineage data.")
        return None


    @staticmethod
    def is_excluded_column(column, column_type=None):
        """
        Check if a column should be excluded based on its type or name pattern.
        """
        normalized_column = column.lower()
        print(f"Checking exclusion for column: {normalized_column}, Type: {column_type}")

        if normalized_column.startswith(("num_", "hashed_")) or "hashed_" in normalized_column:
            return True

        if column_type is None:
            return False  
        
        if normalized_column == "is_pep" or normalized_column == "birth_date":
            return False
        
        if isinstance(column_type, str) and column_type.startswith("STRUCT"):
            struct_fields = SensitiveFieldsProcessor.extract_struct_fields(column_type)  
            for field in struct_fields:
                field_type = SensitiveFieldsProcessor.get_column_type(field,lineage_data)  
                if field_type in ["bool", "boolean", "timestamp"]:
                    print(f"Excluding {field} due to type: {field_type}")
                    return True

        if isinstance(column_type, str) and column_type.startswith("ARRAY<STRUCT"):
            array_struct_fields = SensitiveFieldsProcessor.extract_array_struct_fields(column_type)  
            for field in array_struct_fields:
                field_type = SensitiveFieldsProcessor.get_column_type(field,lineage_data)  
                if field_type in ["bool", "boolean", "timestamp"]:
                    print(f"Excluding {field} due to type: {field_type}")
                    return True

        if column_type and column_type.lower() in ["bool", "boolean", "timestamp"]:
            print(f"Excluding column: {normalized_column} due to type: {column_type}")
            return True

        return False

    @staticmethod
    def categorize_column(column):
        column_lower = column.lower()

        if any(keyword in column_lower for keyword in ["ssn", "email", "phone", "national_"]):
            return "high", "PII"
        elif any(keyword in column_lower for keyword in ["name","etunimi"]):
            return "high", "restricted"
        elif "bank_account_number" in column_lower:
            return "high", "confidential"
        elif any(keyword in column_lower for keyword in ["education", "address", "marital_status", "net_income", "ytunnus"]):
            return "medium", "restricted"
        else:
            return "medium", "restricted"


    @staticmethod
    def extract_struct_fields(struct_type):
        """
        Extract field names from a STRUCT type string.
        """
        struct_type = struct_type[7:-1]  # Remove 'STRUCT<' and '>'
        fields = struct_type.split(",")
        return [field.strip().split(" ")[0] for field in fields]

    @staticmethod
    def extract_array_struct_fields(array_struct_type):
        """
        Extract field names from an ARRAY<STRUCT<...>> type string.
        """
        struct_type = array_struct_type[11:-1]  
        return SensitiveFieldsProcessor.extract_struct_fields(f"STRUCT<{struct_type}>")
