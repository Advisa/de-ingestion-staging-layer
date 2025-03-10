# GDPR Anonymization Service

This repository contains the implementation of the GDPR Anonymization Service, which processes sensitive data based on the configurations in the provided YAML file. The service is deployed as a Google Cloud Function, triggered via Cloud Scheduler at a specified time. This flow is conceptual for now, and we plan to transition to Prefect for orchestrating the workflows. Once Prefect is integrated, we will migrate from Google Cloud Functions to Prefect workflows for more control over execution and better monitoring. Additionally, we will integrate CI/CD pipelines to automate the deployment process.

├── anonymisation_service/
│   ├── schemas/
│       ├── anonymization_event.avsc
│   ├── __init__.py              
│   ├── anon_service.py
│   ├── pubsub_event_push.py    
│   ├── pubsub_schema_manager.py            
│   └── templates/               
│       ├── key_generation_template.sql   
│       ├── update_vault_template.sql
│       ├── anonymisation_customer_data.sql
├── config.yaml                 
├── deploy_dev.sh               
├── deploy_prod.sh              
├── main.py                     
└── requirements.txt            


## Folder Structure
### Key Files:
1. **anonymisation_service/**: Contains the anonymization classes, methods and schemas.
    - `__init__.py`: Initializes the package and imports necessary modules.
    - `anon_service.py`: Contains the `AnonymizationService` class with the core anonymization logic.
    - `pubsub_event_push.py`: Contains the `PubsubPost` class with the core pubsub producer logic.
    - `pubsub_schema_manager.py`: Contains the `PubSubSchemaManager` class with the core schema management logic.
    - `config.yaml`: Contains configuration values for the services.
    - **templates/**: Folder containing sample templates for anonymization.
        - `key_generation_template.sql`, `update_flag_template.sql` and `anonymisation_customer_data`: These 3 sql templates holds key generation logic and update flag logic and anonymization event creation logic.
    
2. **main.py**: The entry point for the Google Cloud Function, which triggers the anonymization job, as well as the event pushing job to PubSub.

3. **deploy_dev.sh**: Shell script for running the schema manager and deploying the Cloud Function + Cloud Scheduler to the **development** environment.

4. **deploy_prod.sh**: Shell script for running the schema manager and deploying the Cloud Function + Cloud Scheduler to the **production** environment.

5. **requirements.txt**: Lists the required Python dependencies.

---

## Schema deployment
- The PubSub schema manager uses an AVRO schema file, defined at `anonymisation_service/schemas/anonymization_event.avsc` to create/update the schema of the messages expected for the PubSub AnonymizationEvent structure.

- During the run of the deployment script, the ['pubsub_schema_manager'](anonymisation_service/pubsub_schema_manager.py) is triggered to update the schema structure for the AnonymizationEvent.

- To update the schema of the AnonymizationEvent, edit the `anonymisation_service/schemas/anonymization_event.avsc` file. The schema evolution allowed can be found here: https://avro.apache.org/docs/++version++/specification/#schema-resolution

- In the case of schema evolution that CANNOT be handled by the deployment script, the topic will have to be manually attached to the new schema, or the schema definition needs to be deleted, and then the deployment script can be runned again. Currently the script does not handle this so that big schema migrations can be tested manually in DEV first before any potential production changes are made.

- The first step compares the current schema file with the latest schema revision in GCP.
    - If no schema is found in GCP, the schema is created.
    - If a difference in schemas is found, a new schema revision is created and is used as the latest schema for events.
    - If no difference in schemas is found, the schema update step is skipped.
    - Additionally, if more than 15 schema revisions are found for a schema defintion (the maximum allowed is 20), the older revision is removed to allow for future revisions to be deployed.
    - Finally, the PubSub topic is created if not found yet, and the latest schema revision is attached to it.
    - If the topic already exists, the topic creation is naturally skipped.
    - A dead letter topic is also created if not configured already

---

## Dependencies

Install the dependencies using `pip`:
pip install -r requirements.txt

---

## Deployment

run sh deploy_prod.sh




