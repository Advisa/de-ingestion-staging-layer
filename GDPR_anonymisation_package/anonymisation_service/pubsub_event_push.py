import logging
import yaml
import json
import sys
from jinja2 import Template
from google.cloud import bigquery, pubsub_v1
from datetime import datetime
import fastavro
from io import BytesIO
import time

class PubsubPost:
    def __init__(self, config_path, env='dev'):
        self.config = self.load_config(config_path)
        self.env = env
        self.anonymization_config = self.config.get(self.env, self.config.get('default', {})).get('anonymization_service', {})

        if not self.anonymization_config:
            logging.error(f"Configuration for environment '{env}' or 'default' not found.")
            sys.exit(1)

        # Extract values from the YAML config for the selected environment
        self.exposure_project = self.anonymization_config.get('exposure_project')
        self.raw_layer_project = self.anonymization_config.get('raw_layer_project')
        self.compliance_project = self.anonymization_config.get('compliance_project')
        self.gdpr_events_dataset = self.anonymization_config.get('gdpr_events_dataset')
        self.gdpr_vault_table = self.anonymization_config.get('gdpr_vault_table')
        self.pubsub_topic = self.anonymization_config.get('pubsub_push_topic')
        self.dl_topic = self.anonymization_config.get('pubsub_dead_letter_topic')

        # Initialize BigQuery client and PubSub publisher
        self.clients = self.initialize_bigquery_clients()
        self.publisher = pubsub_v1.PublisherClient()

        
        with open('anonymisation_service/schemas/anonymization_event.avsc', 'r') as f:
            self.avro_schema = json.load(f)

        # Load SQL Template and set LIMIT for sql template for DEV testing
        self.get_anonymisation_events = self.load_template("anonymisation_customer_data.sql")
        self.limit_clause = "LIMIT 100" if env == "dev" else ""

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

    def initialize_bigquery_clients(self):
        """Initialize BigQuery Clients using Application Default Credentials."""
        clients = {}
        try:
            clients['raw_layer_project'] = bigquery.Client(project=self.raw_layer_project)
            return clients
        except Exception as e:
            logging.error(f"Error initializing BigQuery clients: {str(e)}")
            raise e

    def load_template(self, template_name):
        """Load SQL templates from the templates directory."""
        try:
            with open(f"templates/{template_name}") as f:
                return Template(f.read())
        except FileNotFoundError as e:
            logging.error(f"Template file not found: {str(e)}")
            raise e

    def execute_query(self, client, query):
        """Execute a query using a BigQuery client."""
        try:
            logging.info(f"Executing query: {query}")
            query_job = client.query(query)
            result = query_job.result()
            logging.info("Query executed successfully.")
            return result
        except Exception as e:
            logging.error(f"Error executing query: {str(e)}")
            raise e

    def process_row(self, row):
        """
        Process a message, if processing fails, send the message to a Dead Letter Topic (DLT).
        """
        try:
            row_dict = {}
            
            # Iterate over the schema and process each field
            for field in self.avro_schema['fields']:
                field_name = field['name']
                field_value = row.get(field_name, None)  # Get the value from the row dictionary
                field_type = field['type']

                # Handle nullable fields
                if isinstance(field_type, list) and 'null' in field_type:
                    # If the value is None, handle it as a null field
                    if field_value is None:
                        row_dict[field_name] = None
                    else:
                        # If the field is not null, we need to check the other type in the union
                        non_null_type = next(t for t in field_type if t != 'null')
                        
                        if isinstance(non_null_type, dict) and 'type' in non_null_type:
                            # Handle complex types like 'array'
                            row_dict[field_name] = {non_null_type['type']: field_value}
                        else:
                            row_dict[field_name] = {non_null_type: field_value}
                else:
                    # For fields with single type (non-nullable), just assign the value
                    row_dict[field_name] = field_value

            return row_dict

        except Exception as e:
            logging.error(f"Processing failed: {e}")
            logging.error(f"Row data: {row}")
            return None
        
    def publish_event(self, event_payload, max_retries=2):
        """
        Publish an event to a Pub/Sub topic with a push timestamp.
        If publishing fails after retries, send the message to a Dead Letter Topic (DLT).
        """
        try:
            message_bytes = json.dumps(event_payload).encode("utf-8")
            topic_path = self.publisher.topic_path(self.raw_layer_project, self.pubsub_topic)

            for attempt in range(max_retries):
                try:
                    future = self.publisher.publish(topic_path, message_bytes)
                    message_id = future.result()
                    logging.info(f"Published message ID: {message_id}")
                    return 1  # Success
                except Exception as e:
                    logging.warning(f"Attempt {attempt + 1} failed: {e}")
                    time.sleep(2 ** attempt)  # Exponential backoff

            # If all retries fail, send to Dead Letter Topic
            logging.error("All publish attempts failed. Sending message to Dead Letter Topic.")
            self.send_to_dead_letter_topic(event_payload)
            return 0  # Failure

        except Exception as e:
            logging.error(f"Unexpected failure in publish_event: {e}")
            return 0  # Failure

    def send_to_dead_letter_topic(self, event_payload):
        """Send failed messages to a Dead Letter Topic (DLT)."""
        try:
            dead_letter_topic_path = self.publisher.topic_path(self.raw_layer_project, self.dl_topic)

            # Convert to JSON and encode
            message_json = json.dumps(event_payload)
            message_bytes = message_json.encode("utf-8")

            # Publish the failed message to the Dead Letter Topic
            future = self.publisher.publish(dead_letter_topic_path, message_bytes)
            message_id = future.result()
            logging.info(f"Failed message sent to Dead Letter Topic. Message ID: {message_id}")

        except Exception as e:
            logging.error(f"Failed to send message to Dead Letter Topic: {e}")

    def main(self):
        """Main function to execute the workflow."""
        try:
            logging.info("Pushing anonymisation events to pubsub")
            pull_anonymisation_events_query = self.get_anonymisation_events.render(
                exposure_project=self.exposure_project,
                compliance_project=self.compliance_project,
                gdpr_events_dataset=self.gdpr_events_dataset,
                gdpr_vault_table=self.gdpr_vault_table,
                limit_clause = self.limit_clause
            )
            customers = self.execute_query(self.clients['raw_layer_project'], pull_anonymisation_events_query)
            
            message_count = 0
            for row in customers:
                payload = self.process_row(dict(row))
                if payload:
                    count = self.publish_event(payload)
                    message_count += count
            logging.info(f"Workflow completed successfully. Messages pushed: {message_count}")

        except Exception as e:
            logging.error(f"An error occurred during the process: {e}")
            raise