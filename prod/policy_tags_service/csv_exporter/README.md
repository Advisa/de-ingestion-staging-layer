# CSV Exporter Service

This repository contains the implementation of the **CSV Exporter Service**, which is responsible for exporting taxonomies and policy tags into CSV files. The service interacts with Google Cloud Data Catalog, extracts the required data (taxonomies and policy tags), and writes the data into CSV format.


### Folder Structure

    csv_exporter/
    │   ├── __init__.py    
    │   ├── helpers.py   
    │   ├── main.py                     
    │   └── outputs/  
    │       ├── taxonomy.csv                   
    │       └── policy_tags.csv



### Key Files:

1. **csv_exporter_service/**
    - `__init__.py`: Initializes the package and imports necessary modules. This file is required to turn the folder into a Python package, allowing for easy imports within the service.
    - `helpers.py`: Contains utility functions and classes necessary for the core logic of exporting taxonomies and policy tags to CSV. It includes functions for interacting with Google Cloud services, formatting data, and handling errors.
    - **outputs/**: This folder includes output files that are generated during the CSV export process.
        - `taxonomy.csv`: A CSV file used for structuring and storing taxonomy data. It contains the exported taxonomies in the required format.
        - `policy_tags.csv`: A CSV file used for structuring and storing policy tags data. It contains the exported policy tags in the required format.

2. **main.py**: The entry point for running the CSV Exporter Service. This Python file triggers the process of exporting taxonomies and policy tags. It manages the core workflow of interacting with Google Cloud Data Catalog, generating CSV files, and logging output.


---

## Dependencies
Update the terraform_sa_key in the config.yaml file with terraform-admin service account key file path

Install the dependencies using `pip`:

```bash
pip install -r requirements.txt


