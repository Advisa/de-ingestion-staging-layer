import logging
import os
from helpers import AuthorizedViewService

def run_authorized_view_service():
    logging.basicConfig(level=logging.INFO)
    env = os.getenv('ENV', 'dev')
    config_path = "./prod/authorized_view_service/config.yaml"
    try:
        # Create an instance of AnonymizationService and run the main workflow
        anonymization_service = AuthorizedViewService(config_path, env)
        anonymization_service.main()
        return "Anonymization job completed", 200
    except Exception as e:
        logging.error("An error occurred while executing the anonymization service.")
        raise e

if __name__ == "__main__":
    run_authorized_view_service()
