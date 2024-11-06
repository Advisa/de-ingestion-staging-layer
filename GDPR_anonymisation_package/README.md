# GDPR Anonymization Service

This repository contains the implementation of the GDPR Anonymization Service, which processes sensitive data based on the configurations in the provided YAML file. The service is deployed as a Google Cloud Function, triggered via Cloud Scheduler at a specified time. This flow is conceptual for now, and we plan to transition to Prefect for orchestrating the workflows. Once Prefect is integrated, we will migrate from Google Cloud Functions to Prefect workflows for more control over execution and better monitoring. Additionally, we will integrate CI/CD pipelines to automate the deployment process.

├── anonymisation_service/
│   ├── __init__.py              
│   ├── helpers.py               
│   └── templates/               
│       ├── key_generation_template.sql   
│       ├── update_vault_template.sql    
├── config.yaml                 
├── deploy_dev.sh               
├── deploy_prod.sh              
├── main.py                     
└── requirements.txt            


## Folder Structure
### Key Files:
1. **anonymisation_service/**: Contains the anonymization logic and helpers.
    - `__init__.py`: Initializes the package and imports necessary modules.
    - `helpers.py`: Contains the `AnonymizationService` class with the core anonymization logic.
    - `config.yaml`: Contains configuration values for the anonymization service.
    - **templates/**: Folder containing sample templates for anonymization.
        - `key_generation_template.sql` and `update_flag_template.sql`: These 2 sql templates holds key generation logic and update flag logic.
    
2. **main.py**: The entry point for the Google Cloud Function, which triggers the anonymization job.

3. **deploy_dev.sh**: Shell script for deploying the Cloud Function to the **development** environment.

4. **deploy_prod.sh**: Shell script for deploying the Cloud Function to the **production** environment.

5. **requirements.txt**: Lists the required Python dependencies.

---

## Dependencies

Install the dependencies using `pip`:
pip install -r requirements.txt

---

## Deployment

run sh deploy_prod.sh




