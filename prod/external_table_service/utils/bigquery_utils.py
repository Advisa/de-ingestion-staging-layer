from google.cloud import bigquery
import subprocess
from google.cloud.exceptions import NotFound
import logging

class BigQueryUtils:

    def __init__(self, project, location):
        # Initialize the parameters
        self.project = project
        self.location = location
        self.client = bigquery.Client(project=self.project)


    def is_table_exists(self, dataset, table):
        """ Check if table actually exists in BigQuery"""

        table_id = f"{self.project}.{dataset}.{table}"

        try:
            self.client.get_table(table_id)  
            return True
        except NotFound:
            return False
    
    def get_list_of_tables(self, dataset, legacy_stack):
        table_list = []
        tables = self.client.list_tables(dataset)
        for table in tables:
            if table.table_id.endswith('_r'):
                table_list.append({"legacy_stack": legacy_stack, "table_name": table.table_id})
        return table_list
    
    def clone_tables(self, project, dataset, target_project, target_dataset):
        query = f"""
             SELECT table_name
            FROM `{project}.{dataset}`.INFORMATION_SCHEMA.TABLES 
            WHERE  table_type!='EXTERNAL' and table_name like '%_r'
        """

        results = self.execute_query(query)

        for row in results:
            table_name = row.get('table_name')
            clone_query = f"""
            CREATE TABLE
                {target_project}.{target_dataset}.{table_name}
                CLONE {project}.{dataset}.{table_name};
        """
        
            results = self.execute_query(clone_query)


  
    def execute_query(self, sql):
        """Execute a BigQuery SQL query."""
        query_job = self.client.query(sql, location=self.location)
        rows = query_job.result()
        return [dict(row) for row in rows]


    def generate_source_query_upstream_models(self, dataset):
        """Generate the UNION ALL query for all source tables/views."""
        query = f"""
            WITH tables AS (SELECT
                table_schema
                FROM
                `{self.project}`.`region-europe-north1`.INFORMATION_SCHEMA.TABLES
                WHERE
                table_schema IN ("{dataset}")
            )
                SELECT
                DISTINCT table_schema,
                CONCAT( "SELECT * FROM `{self.project}.", table_schema, "`.INFORMATION_SCHEMA.COLUMNS" ) AS column_query
                FROM
                tables
        """
        return self.execute_query(query)   
   
