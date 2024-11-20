import logging
import os
import yaml
import csv
import subprocess
from pathlib import Path

class CsvExporterService:
    def __init__(self, config_path, env='dev'):
        self.config = self.load_config(config_path)
        self.env = env
        self.csv_exporter_config = self.config.get(self.env, self.config.get('default', {})).get('policy_tags_service', {})
        
        if not self.csv_exporter_config:
            logging.error(f"Configuration for environment '{env}' or 'default' not found.")
            raise ValueError(f"Configuration for environment '{env}' not found.")

        # Extract values from the YAML config
        self.raw_layer_project = self.csv_exporter_config.get('raw_layer_project')
        self.location = self.csv_exporter_config.get('project_location')
        self.base_folder = self.csv_exporter_config.get('base_folder')

        # Initialize output of csv files
        self.output_taxonomy_file = "taxonomy.csv"
        self.output_policy_tags_file = "policy_tags.csv"

        # Initialize the current path where this service is located
        project_root = Path(__file__).resolve().parent.parent  
        self.base_path = project_root


        
    def load_config(self, config_path):
        """Load YAML configuration from the provided path."""
        print("config path:",config_path)
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            logging.info("YAML configuration loaded successfully.")
            return config
        except FileNotFoundError as e:
            logging.error(f"Config file not found: {str(e)}")
          
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML file: {str(e)}")
        
    def run_gcloud_command(self, command):
        """Run a gcloud command and return the output."""
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            logging.error(f"Error while executing gcloud command: {e}")
            raise e
        except Exception as e:
            logging.error(f"Unexpected error occurred while executing gcloud command: {str(e)}")
            raise e
        
    def authenticate_gcloud(self):
        """Authenticate gcloud with the service account."""
        key_path = self.csv_exporter_config.get('terraform_sa_key')
      
        command = [
                'gcloud', 'auth', 'activate-service-account',
                'de-compliance-terraform-admin@sambla-data-staging-compliance.iam.gserviceaccount.com',
                '--key-file', key_path
            ]
        print(command)
        self.run_gcloud_command(command)

    def list_taxonomies(self):
        """List all taxonomies in the specified location and return their details."""
        command = [
            'gcloud', 'beta', 'data-catalog', 'taxonomies', 'list',
            '--location', self.location,
            '--format', 'yaml','--project', self.raw_layer_project
        ]
        print(command)
        output = self.run_gcloud_command(command)
        taxonomies = []

        # Parse the YAML output to extract taxonomy information
        for entry in output.split('---'):
            print(entry)
            if entry.strip():  # Ensure entry is not empty
                taxonomy_info = {}
                for line in entry.splitlines():
                    line = line.strip()
                    if line.startswith('displayName:'):
                        taxonomy_info['displayName'] = line.split('displayName: ')[1].strip()
                    elif line.startswith('description:'):
                        taxonomy_info['description'] = line.split('description: ')[1].strip()
                    elif line.startswith('name:'):
                        taxonomy_info['name'] = line.split('name: ')[1].strip()
                if taxonomy_info:  # If taxonomy_info is not empty, append it
                    taxonomies.append(taxonomy_info)
        return taxonomies

    def retrieve_policy_tags(self, taxonomy_id):
        """Retrieve all policy tags for the specified taxonomy ID."""
        command = [
            'gcloud', 'beta', 'data-catalog', 'taxonomies', 'policy-tags', 'list',
            '--taxonomy', taxonomy_id,
            '--location', self.location,
            '--format', 'yaml','--project', self.raw_layer_project
        ]
        output = self.run_gcloud_command(command)
        tags_info = []
        print(command)

        # Parse the YAML output
        for entry in output.split('---'):
            if entry.strip():  # Ensure entry is not empty
                tag_info = {}
                for line in entry.splitlines():
                    print(line)
                    line = line.strip()
                    if line.startswith('displayName:'):
                        tag_info['displayName'] = line.split('displayName: ')[1].strip()
                    elif line.startswith('name:'):
                        tag_info['name'] = line.split('name: ')[1].strip()
                    elif line.startswith('parentPolicyTag:'):
                        tag_info['parentPolicyTag'] = line.split('parentPolicyTag: ')[1].strip()

                if tag_info:  # If tag_info is not empty, append it
                    tags_info.append(tag_info)

        return tags_info

    def extract_id(self, full_id):
        """Extract the last part of the ID (taxonomy or policy tag ID) from the full ID."""
        try:
            return full_id.split('/')[-1]  
        except Exception as e:
            logging.error(f"Error extracting ID from full ID {full_id}: {str(e)}")
            raise e

    def write_to_csv(self, file_path_name, headers, data):
        """Write data to CSV file with specified headers."""
        file_path = os.path.join(self.base_path, 'csv_exporter/outputs', file_path_name)
        try:
            with open(file_path, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(headers)
                for row in data:
                    writer.writerow(row)
            logging.info(f"Data written to CSV file {file_path}")
        except Exception as e:
            logging.error(f"Error writing to CSV file {file_path}: {str(e)}")
            raise e


    def main(self):
        """Main function to execute the workflow."""
        try:
            # Authenticate using service account
            self.authenticate_gcloud()

            # Get taxonomies
           
            taxonomies = self.list_taxonomies()


            # Prepare taxonomies content data for taxonomy csv file
            taxonomies_data = [
                (taxonomy['displayName'], taxonomy['description'], self.extract_id(taxonomy['name']))
                for taxonomy in taxonomies
            ]
            # Call the function to write the content to the txt file
            self.write_to_csv(self.output_taxonomy_file, ['taxonomy_display_name', 'description', 'id'], taxonomies_data)

            # Prepare policy tags content data for csv file
            policy_tags_data = []
            for taxonomy in taxonomies:
                taxonomy_id = self.extract_id(taxonomy['name']) 
                tags_info = self.retrieve_policy_tags(taxonomy['name'])
                for tag_info in tags_info:
                    policy_tags_data.append((
                        taxonomy_id,
                        self.extract_id(tag_info['name']),
                        tag_info['displayName'],
                        self.extract_id(tag_info.get('parentPolicyTag', ''))
                    ))

            # Write policy tags data to CSV
            self.write_to_csv(self.output_policy_tags_file, ['taxonomy_id', 'policy_tag_id', 'display_name', 'parent_policy_tag_id'], policy_tags_data)

            logging.info("Taxonomies and policy tags data export completed successfully.")

        except Exception as e:
            logging.error(f"An error occurred during the export process: {str(e)}")
            raise e
