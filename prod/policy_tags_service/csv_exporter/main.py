import logging
import os
from helpers import CsvExporterService

def run_csv_exporter_service():
    logging.basicConfig(level=logging.INFO)
    env = os.getenv('ENV', 'dev')
    config_path = "./prod/policy_tags_service/config.yaml"
    try:
        # Create an instance of CsvExporterService and run the main workflow
        csv_exporter_service = CsvExporterService(config_path, env)
        csv_exporter_service.main()
        return "CSV export is completed", 200
    except Exception as e:
        logging.error("An error occurred while executing the csv exporter service.")
        raise e

if __name__ == "__main__":
    run_csv_exporter_service()