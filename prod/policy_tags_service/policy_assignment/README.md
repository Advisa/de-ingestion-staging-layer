# Policy Assignment Service

This repository contains the implementation of the **Policy Assignment Service**, which is responsible for managing IAM policy assignments to the relevant columns in BigQuery tables. The service generates the necessary SQL queries, applies encryption policies, and assigns appropriate IAM policies to columns based on the schema files and policy mappings.

### Folder Structure
    policy_assignment/
    │
    ├── __init__.py                        
    ├── helpers.py                        
    ├── main.py                            
    └── templates/
        └── get_matching_sensitive_fields.sql    


### Key Files:

1. **policy_assignment/**
    - `__init__.py`: Initializes the package and imports necessary modules. This file is required to turn the folder into a Python package, allowing for easy imports within the service.
    - `helpers.py`: Contains utility functions and classes necessary for the core logic of the Policy Assignment Service. This includes functions for generating SQL queries, interacting with BigQuery, and managing IAM policies.
    - **templates/**: This folder contains the SQL templates used to generate dynamic queries.
        - `get_matching_sensitive_fields.sql`: Template for generating queries related to identifying sensitive fields to assign policies.

2. **main.py**: The entry point for running the Policy Assignment Service. This Python file handles the main workflow of generating SQL queries, applying IAM policies, and updating schema files. It triggers the necessary actions to assign policies to columns based on the schema.

---

## Dependencies

Ensure that you have the required dependencies installed:

1. Update the `terraform_sa_key` in the configuration file with your service account key file path.

2. Install the required Python dependencies by running:

```bash
pip install -r requirements.txt
```

To run the Python program to assign policies to tables, execute:

```bash
cd prod 
python policy_tags_service/policy_assignment/main.py
