import logging
import os
from helpers import AuthorizedViewService

def run_authorized_view_service():
    logging.basicConfig(level=logging.INFO)
    env = os.getenv('ENV', 'dev')
    config_path = "./prod/authorized_view_service/config.yaml"
    try:
        # Create an instance of AuthorizedViewService and run the main workflow
        authorized_view_service = AuthorizedViewService(config_path, env)
        authorized_view_service.main()
        return "View Authorization job completed", 200
    except Exception as e:
        logging.error("An error occurred while executing the authorized view service.")
        raise e

if __name__ == "__main__":
    run_authorized_view_service()
