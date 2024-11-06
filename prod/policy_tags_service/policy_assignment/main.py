import logging
import os
from helpers import PolicyAssignmentService

def run_policy_assignment_service():
    logging.basicConfig(level=logging.INFO)
    env = os.getenv('ENV', 'dev')
    config_path = "./prod/policy_tags_service/config.yaml"
    try:
        # Create an instance of PolicyAssignmentService and run the main workflow
        policy_assignment_service = PolicyAssignmentService(config_path, env)
        policy_assignment_service.main()
        return "Policy assignment job is completed", 200
    except Exception as e:
        logging.error("An error occurred while executing the policy assignment service.")
        raise e

if __name__ == "__main__":
    run_policy_assignment_service()