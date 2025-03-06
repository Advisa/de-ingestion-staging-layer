import requests
import hashlib
import json
import logging
import yaml
from google.cloud import pubsub_v1, secretmanager
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class BloomreachAnonymizer:
    def __init__(self, config_path, env='dev'):
        self.config = self.load_config(config_path)
        self.env = env
        self.anonymization_config = self.config.get(self.env, self.config.get('default', {})).get('anonymization_service', {})
        self.url_template = 'https://api.eu1.exponea.com/data/v2/projects/{}/customers/anonymize'
        self.project_id = self.anonymization_config.get('raw_layer_project')
        self.headers = {'Content-Type': 'application/json'}
        self.subscription_name = self.anonymization_config.get('pubsub_subscriber')
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(self.project_id, self.subscription_name)
        self.market_configs = self.anonymization_config.get('anonymize_markets', [])
        self.project_secrets = self.get_secrets(self.anonymization_config.get('secret_name'))

        # Create a requests session with retries
        self.session = requests.Session()
        retries = Retry(
            total=5,  # Max retries
            backoff_factor=1,  # Exponential backoff (1s, 2s, 4s, etc.)
            status_forcelist=[500, 502, 503, 504],  # Retry on these HTTP errors
            allowed_methods={"POST"}  # Only retry POST requests
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

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

    def get_secrets(self, name):
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(request={'name': name})
        return json.loads(response.payload.data.decode('UTF-8'))

    def get_project_config(self, market):
        """Retrieve all project configurations for a given market."""
        if market.upper() == "ALL":
            return [entry.get('bloomreach_projects', []) for entry in self.market_configs]
        return next((entry.get('bloomreach_projects') for entry in self.market_configs if entry.get('market') == market), None)

    def extract_value(self, field):
        """Extracts the actual value from Avro JSON encoded fields."""
        if isinstance(field, dict) and len(field) == 1:
            return list(field.values())[0]
        return field

    def extract_payload(self, message):
        """Extracts necessary fields from the message to create anonymization payload."""
        customer_ids = {}

        if "market" not in message:
            logging.warning("No 'market' field in message. Using all markets")
            market = "ALL"
        else:
            market = self.extract_value(message["market"])

        project_keys = self.get_project_config(market)

        if not project_keys:
            logging.warning(f"No anonymization config found for market '{market}'. Skipping.")
            return None, None

        if "national_id" in message:
            national_id = self.extract_value(message["national_id"])
            if national_id:
                customer_ids["SSN"] = hashlib.sha256(national_id.encode('utf-8')).hexdigest()

        if "latest_email" in message:
            email = self.extract_value(message["latest_email"])
            if email:
                customer_ids["registered"] = email

        if "latest_mobile" in message:
            phone = self.extract_value(message["latest_mobile"])
            if phone:
                customer_ids["phone"] = phone

        return {"customer_ids": customer_ids}, project_keys

    def send_request(self, payload, project_keys, message_id):
        """Send anonymization request to the corresponding projects with retries."""
        anonymised = False

        if isinstance(project_keys, str):
            project_keys = [project_keys]

        for project_key in project_keys:
            secret_data = self.project_secrets.get(project_key)
            if not secret_data:
                logging.error(f"No secret found for project {project_key}. Skipping.")
                continue

            url = self.url_template.format(secret_data['project_token'])
            try:
                response = self.session.post(
                    url, headers=self.headers, json=payload,
                    auth=(secret_data['api_key_id'], secret_data['api_secret'])
                )

                if response.status_code == 200:
                    logging.info(f"Successfully anonymized customer for project {project_key}, message ID {message_id}")
                    anonymised = True
                elif response.status_code == 404:
                    # Customer does not exist in project
                    logging.warning(f"Failed to anonymize for project {project_key}, message ID {message_id}, status: {response.status_code}, response: {response.text}")
                else:
                    logging.error(f"Failed to anonymize for project {project_key}, message ID {message_id}, status: {response.status_code}, response: {response.text}")
            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed for project {project_key}: {e}")
        return anonymised

    def pull_messages(self):
        """Continuously pull messages until the queue is empty."""
        success_count = 0
        unsuccessful_count = 0

        while True:
            response = self.subscriber.pull(subscription=self.subscription_path, max_messages=1000)
            
            if not response.received_messages:
                break  # Exit when there are no more messages

            ack_ids = []
            for received_message in response.received_messages:
                message_id = received_message.message.message_id
                try:
                    decoded_data = json.loads(received_message.message.data.decode('utf-8'))
                    payload, project_keys = self.extract_payload(decoded_data)

                    if payload and project_keys:
                        result = self.send_request(payload, project_keys, message_id)
                        if result:
                            success_count += 1
                        else:
                            unsuccessful_count += 1
                    else:
                        unsuccessful_count += 1

                    ack_ids.append(received_message.ack_id)

                except Exception as e:
                    logging.error(f"Error processing message {message_id}: {e}")

            if ack_ids:
                self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=ack_ids)

        logging.info(f"Anonymized {success_count} customers successfully in bloomreach. Failed {unsuccessful_count} times to anonymize a customer in any project in bloomreach")


# Uncomment for testing
# if __name__ == "__main__": 
#     logging.basicConfig(level=logging.INFO)
#     anonymizer = BloomreachAnonymizer(config_path="config.yaml", env=os.getenv('ENV', 'dev'))
#     anonymizer.pull_messages()