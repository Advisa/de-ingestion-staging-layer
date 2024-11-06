import logging
from csv_exporter import CsvExporterService  # Assuming it's in a file called csv_exporter.py
from policy_assignment import PolicyAssignmentService  # Assuming it's in a file called policy_assignment.py
import os 

def run_csv_exporter(config_path,env):
    """Run the CSV Exporter service."""
    try:
        # Instantiate the CsvExporterService
        csv_exporter_service = CsvExporterService(config_path, env)
        
        # Run the CSV Exporter's workflow (main logic)
        csv_exporter_service.main()
        logging.info("CSV Exporter executed successfully.")
    except Exception as e:
        logging.error(f"Error running CSV Exporter: {str(e)}")
        raise e

def run_policy_assignment_service(config_path,env):
    """Run the Policy Assignment Service."""
    try:
        # Instantiate the PolicyAssignmentService
        policy_assignment_service = PolicyAssignmentService(config_path, env)
        
        # Run the Policy Assignment Service's workflow (main logic)
        policy_assignment_service.main()
        logging.info("Policy Assignment Service executed successfully.")
    except Exception as e:
        logging.error(f"Error running Policy Assignment Service: {str(e)}")
        raise e

def main():
    """Main function to execute both services in sequence."""
    logging.basicConfig(level=logging.INFO)
    env = os.getenv('ENV', 'dev')
    config_path = "./prod/policy_tags_service/config.yaml"
    try:
        # Run the CSV Exporter service
        run_csv_exporter(config_path,env)
        # Run the Policy Assignment Service
        run_policy_assignment_service(config_path,env)

        logging.info("All services executed successfully.")
    
    except Exception as e:
        logging.error(f"An error occurred during the execution: {str(e)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
