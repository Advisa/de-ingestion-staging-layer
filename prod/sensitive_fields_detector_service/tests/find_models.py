import json
import csv
import re
from collections import defaultdict
from google.cloud import bigquery

def load_manifest(manifest_path):
    """Load the manifest.json file."""
    with open(manifest_path, 'r') as f:
        return json.load(f)

def load_table_list(csv_path):
    """Load the table list CSV with table_catalog, table_schema, and table_name."""
    table_list = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            table_catalog = row['table_catalog']
            table_schema = row['table_schema']
            table_name = row['table_name']
            table_list.append((table_catalog, table_schema, table_name))
    return table_list

def find_models_using_table_as_source(manifest, table_list):
    """Find models that use the given tables as sources (in raw code or sources field)."""
    models_using_table = defaultdict(list)
    
    # Regex pattern to find {{source('catalog', 'schema')}} in raw code
    source_pattern = re.compile(r"\{\{source\('([^']+)',\s*'([^']+)'\)\}\}")
    print(source_pattern)
    
    # Go through each model in the manifest
    for node in manifest.get('nodes', {}).values():
        model_name = node['name']
        
        # Check for sources in the model's 'sources' field
        sources = node.get('sources', [])
        for source in sources:
            if len(source) == 2:
                source_catalog, source_schema = source
                for (catalog, schema, table) in table_list:
                    if source_catalog == schema and source_schema == table:
                        print('matching')
                        models_using_table[(catalog, schema, table)].append(model_name)
        
        # Check for raw SQL code (raw_code field)
        raw_code = node.get('raw_code', '')
        if raw_code:
            # Search for the source pattern in the raw SQL code
            matches = source_pattern.findall(raw_code)
            for source_catalog, source_schema in matches:
                # Match the catalog and schema with the tables from the input CSV
                for (catalog, schema, table) in table_list:
                    if source_catalog == catalog and source_schema == schema:
                        models_using_table[(catalog, schema, table)].append(model_name)
    
    return models_using_table

def check_ssn_field(client, table_catalog, table_schema, table_name):
    """Check if a table has relevant columns in BigQuery's INFORMATION_SCHEMA."""
    query = f"""
        SELECT table_schema, table_name, column_name, field_path
        FROM `{table_catalog}.{table_schema}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
        WHERE table_name = '{table_name}' AND LOWER(REPLACE(
            CASE 
                WHEN field_path LIKE '%.%' THEN SUBSTR(field_path, STRPOS(field_path, '.') + 1)
                ELSE field_path
            END, "_", ""
        )) in ('ssn','nationalid','customerssn','foreignerssn','ssnid','sotu','yvsotu','nationalidsensitive')
    """
    query_job = client.query(query)  # Execute the query
    results = query_job.result()  # Wait for the results
    
    # Collect matching column names
    matching_columns = [row['field_path'] for row in results]
    
    return matching_columns  # Return list of matching column names (empty if none found)


def generate_report(models_using_table, client, output_path):
    """Generate a report with models that use each table and check for 'ssn' field."""
    with open(output_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['table_catalog', 'table_schema', 'table_name', 'models_using_table', 'matching_columns'])

        # Write each table and its associated models
        for (catalog, schema, table), models in models_using_table.items():
            matching_columns = check_ssn_field(client, catalog, schema, table)
            matching_columns_str = ', '.join(matching_columns) if matching_columns else 'None'
            writer.writerow([catalog, schema, table, ', '.join(models), matching_columns_str])

def main(manifest_path, csv_path, output_path):
    # Load manifest and table list
    manifest = load_manifest(manifest_path)
    table_list = load_table_list(csv_path)
    
    # Initialize BigQuery client
    client = bigquery.Client()
    
    # Find models using tables from the CSV
    models_using_table = find_models_using_table_as_source(manifest, table_list)
    
    # Generate the report
    generate_report(models_using_table, client, output_path)
    print(f"Report generated: {output_path}")

# File paths (modify these as needed)
manifest_path = 'prod/sensitive_fields_detector_service/tests/tests_inputs/dbt_manifest_test.json'
csv_path = 'prod/sensitive_fields_detector_service/tests/tests_inputs/input_dbt.csv'
output_path = 'prod/sensitive_fields_detector_service/tests/tests_outputs/output_dbt.csv'

# Run the main function
main(manifest_path, csv_path, output_path)
