# anonymization_service/main.py

import logging
import os
from anonymisation_service.anon_service import AnonymizationService 
from anonymisation_service.pubsub_event_push import PubsubPost

def run_anonymization(request):
    """Cloud Function entry point"""
    logging.basicConfig(level=logging.INFO)
    config_path = "config.yaml" 
    env = os.getenv('ENV', 'dev') 

    # Initialize and execute the anonymization service
    anonymization_service = AnonymizationService(config_path, env)
    anonymization_service.main() 
    
    pubsub_push = PubsubPost(config_path, env)
    pubsub_push.main()


    return "Anonymization job completed", 200

# if __name__ == '__main__': # Uncomment for testing
#     run_anonymization('')