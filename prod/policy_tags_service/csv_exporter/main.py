import logging
import os
from helpers import CsvExporterService
from pathlib import Path

def run_csv_exporter_service():
    logging.basicConfig(level=logging.INFO)
    env = os.getenv('ENV', 'dev')
    project_root = Path(__file__).resolve().parent.parent  
    config_path = project_root / "config.yaml"
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

    