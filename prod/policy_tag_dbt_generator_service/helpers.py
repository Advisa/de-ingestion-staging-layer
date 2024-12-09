import yaml
from google.cloud import bigquery

class SchemaAndSourceGenerator:
    def __init__(self, config_file):
        # Load configuration from config.yml
        with open(config_file, "r") as file:
            self.config = yaml.safe_load(file)
        
        # Initialize BigQuery client
        self.client = bigquery.Client()
        self.policy_tag_mappings = None

    def load_policy_tag_mappings(self):
        """load policy tag mappings from a YAML file if using direct links"""
        if self.config["policy_tag_reference"]:
            with open(self.config["policy_tags_file"], "r") as file:
                self.policy_tag_mappings = yaml.safe_load(file)

    def get_policy_tag_metadata(self):
        """fetch policy tag metadata from the source table"""
        metadata_query = f"""
        SELECT taxonomy_id, display_name, policy_tag_id
        FROM `{self.config['source_project']}.{self.config['source_dataset']}.{self.config['metadata_table']}`
        """
        return self.client.query(metadata_query).to_dataframe()

    def get_matching_tables(self):
        """get a list of tables matching the specified suffix"""
        tables_query = f"""
        SELECT table_name
        FROM `{self.config['target_project']}.{self.config['target_dataset']}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE '%{self.config['table_suffix']}'
        """
        return self.client.query(tables_query).to_dataframe()["table_name"].tolist()

    def get_table_schema(self, table_name):
        """fetch schema of a table"""
        schema_query = f"""
        SELECT column_name
        FROM `{self.config['target_project']}.{self.config['target_dataset']}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        """
        return self.client.query(schema_query).to_dataframe()

    def map_columns_to_policy_tags(self, table_schema, policy_metadata):
        """map columns to policy tags based on metadata"""
        policy_tag_mapping = {}
        for _, row in policy_metadata.iterrows():
            tag_name = row["display_name"].lower()
            tag_suffix = row["policy_tag_id"]

            # Use the pre-defined policy_tag_reference as a prefix
            tag_prefix = self.config["policy_tag_reference"].strip('"')
            tag_link = f"{tag_prefix}{tag_suffix}"

            for _, col in table_schema.iterrows():
                if tag_name in col["column_name"].lower():
                    policy_tag_mapping[col["column_name"]] = tag_link
        return policy_tag_mapping

    def generate_schema_yml(self, table_name, table_schema, column_policy_mapping):
        """generate schema.yml structure for a single table"""
        model = {
            "name": table_name,
            "description": "",
            "columns": [],
        }

        for _, column in table_schema.iterrows():
            if column["column_name"] in column_policy_mapping:
                column_entry = {
                    "name": column["column_name"],
                    "description": "",
                    "policy_tags": [column_policy_mapping[column["column_name"]]],
                }
                model["columns"].append(column_entry)

        return model if model["columns"] else None

    def generate_source_yml(self, matching_tables):
        """generate source.yml structure for a single source"""
        source = {
            "name": self.config['source_name'],
            "database": self.config['database'], 
            "schema": self.config['schema'], 
            "tables": []  
        }

        for table in matching_tables:
            table_entry = {
                "name": table,
                "description": f"Source data for {table}",
                "freshness": None,  
                "+enabled": True  
            }

            source["tables"].append(table_entry)

        return source

    def write_schema_yml(self, models, output_file="schema.yml"):
        """write the generated schema to a YAML file"""
        schema_output = {"version": 2, "models": models}

        with open(output_file, "w") as file:
            yaml.dump(
                schema_output,
                file,
                sort_keys=False,
                default_flow_style=False,
                allow_unicode=True
            )
        print(f"{output_file} generated successfully!")

    def write_source_yml(self, source, output_file="source.yml"):
        """write the generated sources to a YAML file"""
        source_output = {"version": 2, "sources": [source]}

        with open(output_file, "w") as file:
            yaml.dump(
                source_output,
                file,
                sort_keys=False,
                default_flow_style=False,
                allow_unicode=True
            )
        print(f"{output_file} generated successfully!")

    def run(self):
        """run the entire workflow to generate both schema.yml and source.yml"""
        self.load_policy_tag_mappings()

        # Fetch metadata and matching tables
        policy_metadata = self.get_policy_tag_metadata()
        matching_tables = self.get_matching_tables()

        # Generate the schema.yml and source.yml
        models = []
        source = self.generate_source_yml(matching_tables)
        
        for table in matching_tables:
            table_schema = self.get_table_schema(table)
            column_policy_mapping = self.map_columns_to_policy_tags(table_schema, policy_metadata)
            schema = self.generate_schema_yml(table, table_schema, column_policy_mapping)
            
            if schema:
                models.append(schema)

        if models:
            self.write_schema_yml(models)
        if source:
            self.write_source_yml(source)


if __name__ == "__main__":
    generator = SchemaAndSourceGenerator(config_file="config.yml")
    generator.run()
