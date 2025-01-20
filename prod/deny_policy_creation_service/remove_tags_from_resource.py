import json
import requests
from google.auth.transport.requests import Request
from google.auth import default
import os

class BigQueryTagManager:
    def __init__(self, project_id, dataset_id):
        self.project_id = project_id
        self.dataset_id = dataset_id

        # Authentication
        self.credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        auth_request = Request()
        self.credentials.refresh(auth_request)

        # API Headers
        self.headers = {
            "Authorization": f"Bearer {self.credentials.token}",
            "Content-Type": "application/json"
        }

    def remove_tags_from_dataset(self):
        """Remove resource tags from the dataset."""
        dataset_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{self.project_id}/datasets/{self.dataset_id}"
        dataset_body = {"resourceTags": {}}  # Clear resource tags

        print(f"Sending PUT request to {dataset_url} to remove tags...")
        response = requests.put(dataset_url, headers=self.headers, data=json.dumps(dataset_body))
        if response.status_code == 200:
            print(f"Resource tags removed successfully from dataset {self.dataset_id}: {response.json()}")
        else:
            print(f"Failed to remove resource tags from dataset {self.dataset_id}: {response.status_code} {response.text}")
    
    def list_tables(self):
        """List all tables in the dataset."""
        tables_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{self.project_id}/datasets/{self.dataset_id}/tables"
        response = requests.get(tables_url, headers=self.headers)

        if response.status_code == 200:
            tables = response.json().get("tables", [])
            return [table["tableReference"]["tableId"] for table in tables]
        else:
            print(f"Failed to list tables: {response.status_code} {response.text}")
            return []

    def remove_tags_from_table(self, table_id):
        """Remove resource tags from a table."""
        table_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{self.project_id}/datasets/{self.dataset_id}/tables/{table_id}"
        response = requests.get(table_url, headers=self.headers)

        if response.status_code == 200:
            table_metadata = response.json()
            table_metadata["resourceTags"] = {}  # Clear resourceTags

            # Send PATCH request to remove tags
            patch_response = requests.patch(table_url, headers=self.headers, data=json.dumps(table_metadata))
            if patch_response.status_code == 200:
                print(f"Resource tags removed successfully from table {table_id}: {patch_response.json()}")
            else:
                print(f"Failed to remove resource tags from table {table_id}: {patch_response.status_code} {patch_response.text}")
        else:
            print(f"Failed to fetch metadata for table {table_id}: {response.status_code} {response.text}")

    def remove_tags_from_dataset_and_tables(self):
        """Remove resource tags from the dataset and all tables in it."""
        self.remove_tags_from_dataset()
        tables = self.list_tables()
        if tables:
            for table_id in tables:
                self.remove_tags_from_table(table_id)
        else:
            print("No tables found in the dataset.")






if __name__ == "__main__":
    # Read values from environment variables or hardcode for testing
    project_id = 'data-domain-data-warehouse'
    dataset_id = 'dbt_ardharani'

    tag_manager = BigQueryTagManager(project_id, dataset_id)

    # Remove resource tags from the dataset and its tables
    tag_manager.remove_tags_from_dataset_and_tables()
