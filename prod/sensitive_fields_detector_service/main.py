from utils.column_lineage_extractor import ColumnLineageExtractor
from utils.sensitive_fields_processor import SensitiveFieldsProcessor
import yaml
import os
import json

def main():
    # Path to config file
    config_file = 'utils/config.yml'
    
    # Initialize SensitiveFieldsProcessor with required file paths
    processor = SensitiveFieldsProcessor(config_file, '', '','','','')
    config = processor.read_yaml_config()

    # Column Lineage Extraction
    extractor = ColumnLineageExtractor(config['manifest_path'], config['catalog_path'])
    lineage_map = extractor.build_lineage_map()  # Lineage map includes column data types
    extractor.write_lineage_to_yaml(config['lineage_output_file'])

    # Sensitive Fields Processing
    processor = SensitiveFieldsProcessor(
        config_file, 
        config['sensitive_field_mapping'], 
        config['sensitive_fields_output_json'], 
        config['lineage_output_file'],
        config['exclusion_file'], 
        config['column_mapping_file']
    )
    
    # Clean CSV to get sensitive columns
    sensitive_columns = processor.clean_csv(config['sensitive_field_mapping'])

    # Build the graph based on the lineage map
    G, processed_columns = processor.build_graph(lineage_map)

    all_grouped_columns = {}

    # Process each sensitive column
    for legacy_column in sensitive_columns:
        if legacy_column not in G:
            print(f"Warning: Legacy column '{legacy_column}' not found in the graph.")
            continue

        try:
            connected_columns = processor.resolve_connections(G, legacy_column)
            grouped_columns = processor.keyword_based_grouping(connected_columns, processed_columns)
            print(grouped_columns)
            all_grouped_columns[legacy_column] = grouped_columns
        except ValueError as e:
            print(f"Error resolving connections for {legacy_column}: {e}")

    # Load exclusion columns and column mapping
    excluded_columns = processor.load_exclusion_list(processor.exclusion_file)
    column_mapping = processor.load_column_mapping(processor.column_mapping_file)

    # Convert grouped columns to JSON using lineage map for column data types
    processor.convert_grouped_columns_to_json(all_grouped_columns, lineage_map, config['sensitive_fields_output_json'], excluded_columns, column_mapping)

    print("Sensitive fields processing complete. Output written to JSON.")

if __name__ == "__main__":
    main()