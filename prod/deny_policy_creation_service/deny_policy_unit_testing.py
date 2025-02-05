import os
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth import default

SERVICE_ACCOUNT_KEY_PATH = ""

PROJECT_ID = "data-domain-data-warehouse"
DATASET_ID = "sambla_group_data_stream_deny"
TABLE_ID = "advisory_service_customer_comments_sgds_r"

def initialize_bigquery_client():
    #credentials, project = default()
    credentials = service_account.Credentials.from_service_account_file(
      SERVICE_ACCOUNT_KEY_PATH
    )
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    return client

def query_bigquery_table():
    client = initialize_bigquery_client()

    query = f"""
    SELECT * 
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` 
    LIMIT 10
    """
    
    query_job = client.query(query)

    results = query_job.result()

    for row in results:
        print(row)

if __name__ == "__main__":
    query_bigquery_table()