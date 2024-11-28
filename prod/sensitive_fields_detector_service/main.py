from utils.column_lineage_extractor import ColumnLineageExtractor
from utils.sensitive_fields_processor import SensitiveFieldsProcessor
import yaml
import os

def main():
    config_file = 'utils/config.yml'
    
    # Initialize SensitiveFieldsProcessor to read config
    processor = SensitiveFieldsProcessor(config_file, '', '')  # Empty input and output paths
    config = processor.read_yaml_config()  # Use the method to read the config
    
    # Column Lineage Extraction
    extractor = ColumnLineageExtractor(config['manifest_path'], config['catalog_path'])
    lineage_map = extractor.build_lineage_map()
    extractor.write_lineage_to_yaml(config['lineage_output_file'])
    
    # Sensitive Fields Processing
    processor = SensitiveFieldsProcessor(config_file, config['sensitive_field_mapping'], config['sensitive_fields_output_json'])
    sensitive_columns = processor.clean_csv(config['sensitive_field_mapping'])
    
    # Build the graph based on the lineage map
    G, processed_columns = processor.build_graph(lineage_map)

    # Initialize a dictionary to store grouped columns
    all_grouped_columns = {}

    # Process each sensitive column separately
    for legacy_column in sensitive_columns:
        if legacy_column not in G:
            print(f"Warning: Legacy column '{legacy_column}' not found in the graph.")
            continue

        try:
            # Resolve connections for each legacy column individually
            connected_columns = processor.resolve_connections(G, legacy_column)
            grouped_columns = processor.keyword_based_grouping(connected_columns, processed_columns)
            all_grouped_columns[legacy_column] = grouped_columns  # Use all_grouped_columns here
        except ValueError as e:
            print(f"Error resolving connections for {legacy_column}: {e}")
    
    # Convert grouped columns to JSON and output the result
    processor.convert_grouped_columns_to_json(all_grouped_columns, config['sensitive_fields_output_json'])  # Pass all_grouped_columns here


if __name__ == "__main__":
    main()
