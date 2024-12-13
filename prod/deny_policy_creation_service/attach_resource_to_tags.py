import json
import requests
from google.auth.transport.requests import Request
from google.auth import default
import os

class BigQueryTagManager:
    def __init__(self, project_id, dataset_id, tag_key_id, tag_value_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.tag_key_id = tag_key_id
        self.tag_value_id = tag_value_id

        # tags in the required format Tag key is expected to be in the namespaced format, for example "123456789012/environment" where 123456789012 is the ID of the parent organization or project resource for this tag key. Tag value is expected to be the short name, for example "Production".
        self.resource_tags = {f'{self.project_id}/{self.tag_key_id}': self.tag_value_id}
        self.credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        auth_request = Request()
        self.credentials.refresh(auth_request)

        # API Headers
        self.headers = {
            "Authorization": f"Bearer {self.credentials.token}",
            "Content-Type": "application/json"
        }

    def apply_tags_to_dataset(self):
        """Apply tags to the dataset."""
        dataset_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{self.project_id}/datasets/{self.dataset_id}"
        dataset_body = {"resourceTags": self.resource_tags}

        response = requests.put(dataset_url, headers=self.headers, data=json.dumps(dataset_body))
        if response.status_code == 200:
            print(f"Tags updated successfully on dataset {self.dataset_id}: {response.json()}")
        else:
            print(f"Failed to update tags on dataset {self.dataset_id}: {response.status_code} {response.text}")

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

    def apply_tags_to_tables(self, tables):
        """Apply tags to each table in the dataset."""
        for table_id in tables:
            table_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{self.project_id}/datasets/{self.dataset_id}/tables/{table_id}"
            table_body = {"resourceTags": self.resource_tags}

            response = requests.put(table_url, headers=self.headers, data=json.dumps(table_body))
            if response.status_code == 200:
                print(f"Tags updated successfully on table {table_id}: {response.json()}")
            else:
                print(f"Failed to update tags on table {table_id}: {response.status_code} {response.text}")

    def apply_tags_to_dataset_and_tables(self):
        """Apply tags to the dataset and all tables in it."""
        self.apply_tags_to_dataset()
        tables = self.list_tables()
        if tables:
            self.apply_tags_to_tables(tables)


if __name__ == "__main__":
    # Read values from terraform parameters (or hardcode them for testing)
    project_id = os.getenv("PROJECT_ID", "your_project_id")
    dataset_id = os.getenv("DATASET_ID", "your_dataset_id")
    tag_key_id = os.getenv("TAG_KEY_ID", "your_tag_key_id")
    tag_value_id = os.getenv("TAG_VALUE_ID", "your_tag_value_id")

    tag_manager = BigQueryTagManager(project_id, dataset_id, tag_key_id, tag_value_id)
    tag_manager.apply_tags_to_dataset_and_tables()