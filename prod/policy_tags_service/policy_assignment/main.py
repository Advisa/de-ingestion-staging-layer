import logging
import os
from helpers import PolicyAssignmentService
from pathlib import Path

def run_policy_assignment_service():
    logging.basicConfig(level=logging.INFO)
    env = os.getenv('ENV', 'dev')
    project_root = Path(__file__).resolve().parent.parent  
    config_path = project_root / "config.yaml"
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