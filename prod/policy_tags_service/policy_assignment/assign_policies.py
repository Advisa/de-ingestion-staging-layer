from google.cloud import bigquery
import json
import os
from jinja2 import Template
import glob

# File paths
PATH = os.path.dirname(os.path.abspath(__file__))
union_all_query_path =  "/Users/duygugenc/Documents/de-ingestion-staging-layer/prod/modules/auth_views/authorized_view_scripts/generated_source_query.sql"
sensitive_fields_query_path = os.path.join(PATH,"template_sql_files/get_matching_sensitive_fields.sql")

def read_sensitive_fields_query_template():
    with open(sensitive_fields_query_path) as f:
        sensitive_fields_query = Template(f.read())
        return sensitive_fields_query
    
def read_union_all_query_template():
    with open(union_all_query_path) as f:
        return f.read()

def get_matching_sensitive_fields(location,project_id):
    # Initialize the bigquery client
    client = bigquery.Client(location=location,project=project_id)
    # Render the template with the source_query as a parameter
    union_all_query = read_union_all_query_template()
    query = read_sensitive_fields_query_template().render(source_table_columns=union_all_query)
    
    try: 
        # Run the query and store the results  
        query_tables = client.query(query)
        results = query_tables.result()
        print("res:",results)
        # Define dictionary to hold table and column mappings of the tables to which policy tags are applied to.
        policy_mapping = {}
        for row in results:
            if row.table_name not in policy_mapping:
                policy_mapping[row.table_name] = {}
            policy_mapping[row.table_name][row.column_name] = row.iam_policy_name
        
        return policy_mapping
    except Exception as e:
        # if no table names are found in the desired gcs location, print a message indicating that
        print(f"An Error occured: {e}")
        return [], []

def construct_iam_policies(policy_mapping, schema_dir):
    # Extract all schema JSON files in the schemas directory and its subdirectories (lvs, raha, etc.)
    schema_files = glob.glob(os.path.join(schema_dir, "**/*_schema.json"), recursive=True)

    for schema_file_path in schema_files:
        # Extract the table name from the file name
        table_name = os.path.basename(schema_file_path).replace("_schema.json", "")
        print(f"Processing schema for table: {table_name} from {schema_file_path}")

        column_policies = policy_mapping.get(table_name, {})

        try:
            # Check if the file is empty before trying to load it
            if os.path.getsize(schema_file_path) == 0:
                print(f"Schema file is empty: {schema_file_path}. Skipping.")
                continue  

            # Load schema file for the current table
            with open(schema_file_path, 'r') as file:
                schema = json.load(file)  
            
            # Track if any updates were made to the schema
            updated = False
            
            # Update each fields policy tag based on policy mapping
            for field in schema:
                if field["name"] in column_policies:
                    field["policyTags"] = {"names": [column_policies[field["name"]]]}
                    updated = True
            
            # Save the updated schema to the same file
            if updated:
                try:
                    # Overwrite the original schema file
                    with open(schema_file_path, 'w') as file:  
                        json.dump(schema, file, indent=4)
                    print(f"Schema for table {table_name} updated with policy tags")
                except Exception as e:
                    print(f"Failed to write to schema file {schema_file_path}: {e}")

        except FileNotFoundError:
            print(f"Schema file not found for table {table_name} at {schema_file_path}")
        except json.JSONDecodeError:
            print(f"Error decoding JSON from schema file: {schema_file_path}")
        except Exception as e:
            print(f"An unexpected error occurred while processing {schema_file_path}: {e}")

      

def main():
    # Retrieve the policy mapping
    policy_mapping = get_matching_sensitive_fields(LOCATION, PROJECT_ID)
    print("Policy mappings retrieved:", policy_mapping)
    construct_iam_policies(policy_mapping,schema_dir)



if __name__ == "__main__":
    LOCATION = "europe-north1"
    PROJECT_ID = "sambla-data-staging-compliance"   
    schema_dir = "/Users/duygugenc/Documents/de-ingestion-staging-layer/prod/schemas/"
    main()


