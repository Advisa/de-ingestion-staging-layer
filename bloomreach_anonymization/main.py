# bloomreach_anonymization/main.py

import logging
import os
from bloomreach_anonymization import BloomreachAnonymizer
import google.cloud.logging

def run_br_anonymization(request):
    """Cloud Function entry point"""
    client = google.cloud.logging.Client()
    client.setup_logging()
    logging.basicConfig(level=logging.INFO)
    config_path = "config.yaml" 
    env = os.getenv('ENV', 'dev') 

    # Initialize and execute the anonymization service
    br_anonymization_service = BloomreachAnonymizer(config_path, env)
    br_anonymization_service.pull_messages() 

    return "Bloomreach anonymization job completed", 200

# if __name__ == '__main__': # Uncomment for testing
#     run_anonymization('')