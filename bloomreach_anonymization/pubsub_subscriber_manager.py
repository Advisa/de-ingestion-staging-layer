from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound
import yaml
import logging
import sys
import os

class PubSubSubscriberManager:
    def __init__(self, config_path, env='dev'):
        self.config = self.load_config(config_path)
        self.env = env
        self.anonymization_config = self.config.get(self.env, self.config.get('default', {})).get('anonymization_service', {})

        if not self.anonymization_config:
            logging.error(f"Configuration for environment '{env}' or 'default' not found.")
            sys.exit(1)

        # Extract values from the YAML config for the selected environment
        self.project_id = self.anonymization_config.get('raw_layer_project')
        self.subscription_name = self.anonymization_config.get('pubsub_subscriber')
        self.topic_name = self.anonymization_config.get('pubsub_topic')
        self.deadletter_topic_name = self.anonymization_config.get('deadletter_topic')
        self.topic_path = f"projects/{self.project_id}/topics/{self.topic_name}"
        self.topic_path = f"projects/{self.project_id}/topics/{self.deadletter_topic_name}"
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(self.project_id, self.subscription_name)
        self.retention_duration = self.anonymization_config.get('topic_retention_days', 1) * 24 * 60 * 60

    def load_config(self, config_path):
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

    def manage_subscription(self):
        try:
            # Check if the subscription exists with the correct request format
            request = pubsub_v1.types.GetSubscriptionRequest(subscription=self.subscription_path)
            self.subscriber.get_subscription(request=request)
            logging.info(f"Subscription {self.subscription_name} already exists.")
        except NotFound:
            logging.info(f"Subscription {self.subscription_name} not found. Creating...")
            
            # Define subscription settings
            subscription = pubsub_v1.types.Subscription(
                name=self.subscription_path,
                topic=self.topic_path,
                ack_deadline_seconds=60,
                retain_acked_messages=False,
                message_retention_duration={"seconds": self.retention_duration},
                enable_exactly_once_delivery=False,
                enable_message_ordering=False,
                retry_policy=pubsub_v1.types.RetryPolicy(
                    minimum_backoff={"seconds": 10},
                    maximum_backoff={"seconds": 600},
                ),
                dead_letter_policy=pubsub_v1.types.DeadLetterPolicy(
                    dead_letter_topic=self.topic_path,
                    max_delivery_attempts=5,
                ),
            )
            
            # Create subscription
            self.subscriber.create_subscription(subscription)
            logging.info(f"Subscription {self.subscription_name} created successfully.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config_path = "config.yaml"
    manager = PubSubSubscriberManager(
        config_path = config_path,
        env = os.getenv('ENV', 'dev') 
    )
    manager.manage_subscription()