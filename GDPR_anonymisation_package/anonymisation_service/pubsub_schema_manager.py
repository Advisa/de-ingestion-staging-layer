from google.api_core.exceptions import AlreadyExists, NotFound, InvalidArgument
from google.cloud.pubsub import SchemaServiceClient, PublisherClient
from google.pubsub_v1.types import Schema, Encoding
import logging
import os
import sys
import yaml
from google.protobuf.duration_pb2 import Duration

class PubSubSchemaManager:
    def __init__(self, config_path, avsc_file, env='dev'):
        self.config = self.load_config(config_path)
        self.env = env
        self.anonymization_config = self.config.get(self.env, self.config.get('default', {})).get('anonymization_service', {})

        if not self.anonymization_config:
            logging.error(f"Configuration for environment '{env}' or 'default' not found.")
            sys.exit(1)

        # Extract values from the YAML config for the selected environment
        self.project_id = self.anonymization_config.get('raw_layer_project')
        self.schema_id = self.anonymization_config.get('pubsub_schema_id')
        self.topic_id = self.anonymization_config.get('pubsub_push_topic')
        self.dl_topic_id = self.anonymization_config.get('pubsub_dead_letter_topic')
        self.kms_key = self.anonymization_config.get('kms_key')

        self.schema_client = SchemaServiceClient()
        self.publisher = PublisherClient()

        self.project_path = f"projects/{self.project_id}"
        self.schema_path = self.schema_client.schema_path(self.project_id, self.schema_id)
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        self.dl_topic_path = self.publisher.topic_path(self.project_id, self.dl_topic_id)
        self.retention_duration = Duration(seconds=self.anonymization_config.get('topic_retention_days', 1) * 24 * 60 * 60)

        # Read AVRO schema file
        with open(avsc_file, "rb") as f:
            self.avsc_source = f.read().decode("utf-8")


    def load_config(self, config_path):
        """Load YAML configuration from the provided path."""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            logging.info("YAML configuration loaded successfully.")
            return config
        except FileNotFoundError as e:
            logging.error(f"Config file not found: {str(e)}")
            raise e
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML file: {str(e)}")
            raise e

    def schema_exists(self):
        """Check if the schema already exists in Pub/Sub."""
        try:
            return self.schema_client.get_schema(request={"name": self.schema_path})
        except NotFound:
            return None
        
    def determine_delete_schema_revision(self):
        # Get all schema revisions
        schemas = list(self.schema_client.list_schema_revisions(request={"name": self.schema_path}))
        num_schemas = len(schemas)
        logging.info(f"Number of schema revisions: {num_schemas}")
            
        # If there are 15 revisions, find and delete the oldest one based on create time
        if num_schemas >= 15:
            oldest_revision = min(schemas, key=lambda s: s.revision_create_time).name
            try:
                self.schema_client.delete_schema_revision(request={"name": oldest_revision})
                logging.info(f"Deleted oldest schema revision: {oldest_revision}")
            except NotFound:
                logging.error(f"Failed to delete schema revision '{oldest_revision}', it was not found.")

        # Check if the latest schema revision matches the new schema
        if num_schemas > 0:
            latest_schema = self.schema_client.get_schema(request={"name": self.schema_path}).definition
            if latest_schema == self.avsc_source:
                logging.info("Schema has not changed. Skipping update.")
                return False  # No update needed

    def revise_schema(self):
        """Create the schema if it doesn't exist, or update it if it has changed."""
        existing_schema = self.schema_exists()

        if existing_schema:
            result = self.determine_delete_schema_revision()
            if result:
                try:
                    self.schema_client.commit_schema(
                        request={
                            "schema": Schema(name=self.schema_path ,type_=Schema.Type.AVRO, definition=self.avsc_source),
                            "name": self.schema_path
                        }
                    )
                    logging.info(f"Committed a schema revision using an Avro schema file")
                    return True
                except NotFound:
                    logging.error(f"Schema '{self.schema_id}' does not exist.")
        else:
            logging.info("Schema does not exist. Creating...")
            try:
                self.schema_client.create_schema(
                    request={
                        "parent": self.project_path,
                        "schema_id": self.schema_id,
                        "schema": Schema(name=self.schema_path ,type_=Schema.Type.AVRO, definition=self.avsc_source),
                    }
                )
                logging.info(f"Schema '{self.schema_id}' created.")
                return True
            except AlreadyExists:
                logging.info(f"Current version of schema '{self.schema_id}' already exist")

    def create_topic(self, topic_path, schema_path=None):
        """Create the topic if it doesn't exist."""
        try:
            # base request with required fields
            request = {
                "name": topic_path,
                "message_retention_duration": self.retention_duration,
                "kms_key_name": self.kms_key  # Add KMS key
            }
            
            # Add schema settings only if schema_path is provided
            if schema_path:
                request["schema_settings"] = {
                    "schema": schema_path,
                    "encoding": Encoding.JSON,
                }
                
            self.publisher.create_topic(request=request)
            logging.info(f"Topic at '{topic_path}' created.")
            return True
            
        except AlreadyExists:
            logging.info(f"Topic at '{topic_path}' already exists")
            return False

    def update_topic_schema(self):
        """Update the topic to use the latest schema."""
        try:
            # Step 1: Get the existing topic details
            topic = self.publisher.get_topic(request={"topic": self.topic_path})

            # Step 2: Check if a schema is already attached
            if not topic.schema_settings.schema or topic.schema_settings.schema == "_deleted-schema_":
                logging.info(f"No schema attached to {self.topic_id}. Attaching schema now...")

                response = self.publisher.update_topic(
                    request={
                        "topic": {
                            "name": self.topic_path,
                            "schema_settings": {
                                "schema": self.schema_path,
                            },
                        },
                        "update_mask": "schemaSettings.schema"
                    },
                )
                logging.info(f"Schema attached successfully:\n{response}")
            else:
                logging.info(f"Topic {self.topic_id} already has a schema attached. No changes made.")

        except NotFound:
            logging.info(f"Topic {self.topic_id} not found.")
        except InvalidArgument as e:
            logging.info(f"Schema settings are not valid: {e}")
        except Exception as e:
            logging.info(f"An error occurred: {e}")

    def main(self):
        """Run schema and topic setup."""
        schema = self.revise_schema()
        topic = self.create_topic(self.topic_path, self.schema_path)
        if not topic: # If no new topic created, ensure that old topic uses right schema
            self.update_topic_schema()

        self.create_topic(self.dl_topic_path)
        
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    manager = PubSubSchemaManager(
        config_path="config.yaml",
        env = os.getenv('ENV', 'dev'),
        avsc_file="anonymisation_service/schemas/anonymization_event.avsc",
    )
    manager.main()