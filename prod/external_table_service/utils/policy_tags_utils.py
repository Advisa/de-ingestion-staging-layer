import json
import subprocess
import os
import glob
from pathlib import Path
import re
import logging


class PolicyTagsUtils:
    def __init__(self, script_path, sql_template_path):
        # Initialize the parameters
        self.script_path = script_path
        self.sql_template_path = sql_template_path
     

    def has_policy_tags(self,data):
        """ Checks whether the json schema has any policy tags assigned"""
        if isinstance(data, dict):
            for key, value in data.items():
                if key == "policyTags":
                        return True

                # Recursively search nested dictionaries
                if self.has_policy_tags(value):
                    return True
        elif isinstance(data, list):
            # Recursively search in list elements
            for item in data:
                if self.has_policy_tags(item):
                    return True
        return False

    def extract_tables_with_policy_tags(self,schema_folder_path,table_suffix,source_schema):
        """
            Extract the list of tables with existing policy tags and returns the list of tables
             where policy tags should be applied.
        """
        
        schema_file_path = schema_folder_path # Path to folder containing schema JSON files
        tables_with_policy_tags = []
        # Recursively go through all JSON schema files under the schemas folder
        schema_files = glob.glob(os.path.join(schema_file_path, "**/*_schema.json"), recursive=True)

        for schema_file_path in schema_files:
            # Extract table name from the schema file name and append the table suffix 
            table_name = os.path.basename(schema_file_path).replace("_r_schema.json", "") + table_suffix
            schema_name = os.path.basename(os.path.dirname(schema_file_path))
            try:
                if os.path.getsize(schema_file_path) == 0 :
                    logging.info(f"Schema file is empty: {schema_file_path}. Skipping.")
                    continue  

                # Process files only if they belong to the specified schema
                if schema_name == source_schema:
                    with open(schema_file_path, 'r') as file:
                        schema = json.load(file)
                        # Check if the schema of source table has assigned to any policy tags
                        if self.has_policy_tags(schema):
                            # For all the dbt models, search if its source tables has any policy tags assigned.
                            # If yes, add these dbt model information to the list
                            tables_with_policy_tags.append({"legacy_stack":schema_name, "table_name":table_name})
            except FileNotFoundError:
                    logging.error(f"Schema file not found for table {table_name} at {schema_file_path}")
            except json.JSONDecodeError:
                logging.error(f"Error decoding JSON from schema file: {schema_file_path}")
            except Exception as e:
                logging.error(f"An unexpected error occurred while processing {schema_file_path}: {e}")

        return tables_with_policy_tags


    def run_policy_tags_assignment_script(self):
        """
        This function runs the main.py script located at the given path.
        """
        try:
            logging.info(f"Running main.py script at {self.script_path}...")
            result = subprocess.run(
                ['python3', self.script_path],
                check=True
            )
            
            # Print the result of the script execution
            logging.info(f"Script executed successfully.")
        
        except subprocess.CalledProcessError as e:
            logging.error(f"Error occurred while running main.py:\n{e}")
            raise e  
 
    def update_sql_template(self, action, append_sql_block):
        """
        Updates the SQL template by either appending or removing a UNION ALL block based on the action ('append' or 'remove').
        """
        placeholder = "{{source_table_columns}}"
        
        # Read the file
        sql_file = Path(self.sql_template_path)
        if not sql_file.exists():
            raise FileNotFoundError(f"The file '{self.sql_template_path}' does not exist.")

        # Read the current content
        with sql_file.open("r") as file:
            sql_content = file.read()

        if placeholder not in sql_content:
            raise ValueError(f"The placeholder '{placeholder}' was not found in the SQL file.")

        # The block to append
        block_to_append = f"    UNION ALL\n    {append_sql_block.strip()}"

        # Find the location of {{source_table_columns}} in the file content
        placeholder_pos = sql_content.find(placeholder)

        # Extract the content after the placeholder
        content_after_placeholder = sql_content[placeholder_pos + len(placeholder):]

        # Regex to find all existing UNION ALL statements below {{source_table_columns}}
        existing_union_all_statements = re.findall(r"\s*UNION ALL\s*SELECT\s*.*?FROM\s*`.*?`.*?INFORMATION_SCHEMA.COLUMNS", content_after_placeholder, flags=re.DOTALL)

        # Check if the current UNION ALL block already exists in the SQL content
        if action != "remove" and any(append_sql_block.strip() in statement for statement in existing_union_all_statements):
            logging.info(f"The UNION ALL block '{append_sql_block}' already exists. No changes made.")
            return

        # If appending, add the new block after the last UNION ALL statement or immediately after the placeholder if no UNION ALL exists.
        if action == "append":
            # Check if there is an existing UNION ALL, and append after it
            if existing_union_all_statements:
                last_union_all_pos = content_after_placeholder.rfind(existing_union_all_statements[-1])
                modified_sql_content = sql_content[:placeholder_pos + len(placeholder) + last_union_all_pos] + block_to_append + sql_content[placeholder_pos + len(placeholder) + last_union_all_pos:]
            else:
                modified_sql_content = sql_content[:placeholder_pos + len(placeholder)] + block_to_append + sql_content[placeholder_pos + len(placeholder):]
            message = "successfully updated with the new SQL block."

        # If action is 'remove', remove the block (if exists)
        elif action == "remove":
            # Remove the specific UNION ALL block (if it exists)
            modified_sql_content = re.sub(re.escape(block_to_append.strip()) + r"\s*", "", sql_content)
            if modified_sql_content == sql_content:
                logging.info(f"The UNION ALL block '{append_sql_block}' was not found. No changes made.")
                return
            message = "SQL block has been successfully removed."

        else:
            raise ValueError("Invalid action. Please specify 'append' or 'remove'.")

        # Write the modified content back to the file
        with sql_file.open("w") as file:
            file.write(modified_sql_content)

        logging.info(f"File '{self.sql_template_path}' {message}")
