# anonymization_service/main.py

import logging
import os
from anonymisation_service.helpers import AnonymizationService 

def run_anonymization(request):
    """Cloud Function entry point"""
    logging.basicConfig(level=logging.INFO)
    config_path = "config.yaml" 
    env = os.getenv('ENV', 'dev') 

    # Initialize and execute the anonymization service
    anonymization_service = AnonymizationService(config_path, env)
    anonymization_service.main() 

    return "Anonymization job completed", 200
