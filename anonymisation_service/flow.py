#In the future when we do the complete implementation this will be the setup, 
#for now this is sample prefect flow this is not used or tested for prototype.
# we have to add prefect.yaml to deploy the flow.

from prefect import task, Flow
import logging
from helpers import AnonymizationService

# Set up logging
logging.basicConfig(level=logging.INFO)

# Initialize AnonymizationService outside of tasks to pass around in the flow
def get_anonymization_service(config_path, env):
    return AnonymizationService(config_path, env)

@task
def key_generation_task(anonymization_service):
    """Task to handle key generation using AnonymizationService"""
    logging.info("Generating keys...")
    key_generation_query = anonymization_service.key_generation_query_template.render(
        exposure_project=anonymization_service.exposure_project,
        compliance_project=anonymization_service.compliance_project
    )
    return anonymization_service.execute_query(anonymization_service.clients['raw_layer_project'], key_generation_query)

@task
def update_anonymized_flags_task(anonymization_service, join_keys):
    """Task to update anonymization flags in the raw layer project."""
    logging.info("Updating anonymization flags...")
    anonymization_service.update_anonymized_flags(join_keys)

# Define the flow
with Flow("Anonymization Workflow") as flow:
    config_path = "config.yaml"  # Path to the config file
    env = "dev"  # Environment variable ('dev' or 'prod')
    
    # Instantiate AnonymizationService
    anonymization_service = get_anonymization_service(config_path, env)
    encryption_key = key_generation_task(anonymization_service)

    join_keys = {}
    update_anonymized_flags_task(anonymization_service, join_keys)

if __name__ == "__main__":
    flow.run()
