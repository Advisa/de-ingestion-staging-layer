import logging
import yaml
import json
import sys
from jinja2 import Template
from google.cloud import bigquery
from anonymisation_service.models import AnonymizationEvent 
from datetime import datetime
from google.cloud import pubsub_v1

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

        # Initialize BigQuery client and PubSub publisher
        self.clients = self.initialize_bigquery_clients()
        self.publisher = pubsub_v1.PublisherClient()

        # Load SQL Template
        self.get_anonymisation_events = self.load_template("anonymisation_customer_data.sql")

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
        """Validate and transform a row into a JSON payload."""
        try:
            event = AnonymizationEvent(**dict(row))
            return event.to_json()
        except Exception as e:
            logging.error(f"Validation failed: {e}")
            return None
        
    def publish_event(self, event_payload):
        """Publish an event to a Pub/Sub topic with a push timestamp."""
        try:
            # Add push_time to the payload
            event_payload["push_time"] = datetime.utcnow().isoformat()  # Add push_time in UTC

            # Convert to JSON string
            message_json = json.dumps(event_payload)
            # print(message_json)
            message_bytes = message_json.encode("utf-8")

            # get the full Pub/Sub topic path
            topic_path = self.publisher.topic_path(self.exposure_project, self.pubsub_topic)

            # Publish the message
            future = self.publisher.publish(topic_path, message_bytes)
            message_id = future.result()

            logging.info(f"Published message ID: {message_id}")
            return 1
        except Exception as e:
            logging.error(f"Failed to publish event: {e}")
            return 0

    def main(self):
        """Main function to execute the workflow."""
        try:
            logging.info("Pushing anonymisation events to pubsub")
            pull_anonymisation_events_query = self.get_anonymisation_events.render(
                exposure_project=self.exposure_project,
                compliance_project=self.compliance_project,
                gdpr_events_dataset=self.gdpr_events_dataset,
                gdpr_vault_table=self.gdpr_vault_table,
            )
            customers = self.execute_query(self.clients['raw_layer_project'], pull_anonymisation_events_query)
            
            message_count = 0
            for row in customers:
                payload = self.process_row(row)
                if payload:
                    count = self.publish_event(payload)
                    message_count += count
            logging.info(f"Workflow completed successfully. Messages pushed: {message_count}")

        except Exception as e:
            logging.error("An error occurred during the process")
            raise