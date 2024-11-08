# Authorized View Service

This repository contains the implementation of the **Authorized View Service**, which performs tasks based on the configurations in the provided YAML file to create an encrypted authorized view definition. The service utilizes SQL and txt templates for operations such as generating encryption query and mapping information of encrypted source data.

## Folder Structure

├── authorized_view_service/
│   ├── __init__.py    
|   ├── config.yaml            
│   ├── helpers.py   
|   ├── main.py                     
|   └── requirements.txt              
│   └── templates/  
|       ├── auth_view_mapping.txt                
│       ├── encryption_query_template.sql   
│       ├── generated_source_query.sql    
|           
|                   


### Key Files:

1. **anonymisation_service/**
    - `__init__.py`: Initializes the package and imports necessary modules. This file is required to turn the folder into a Python package, allowing for easy imports within the service.
    - `config.yaml`: Contains configuration values for the anonymisation service. This file includes settings such as project details, key management configurations, and any other environment-specific configurations.
    - `helpers.py`: Contains utility functions and classes necessary for the core logic of the authorized view creation process. It may include helper functions for data processing, encryption, or any necessary transformation operations.
    - **templates/**: This folder includes SQL and txt templates that are used during the anonymisation process.
        - `auth_view_mapping.txt`: A text file that maps various views or tables to their encryption queries. It helps in identifying the required authorized view definition for each view or table within datasets.
        - `encryption_query_template.sql`: This SQL template file is a dynamically generated to extract column information from all datasets in BigQuery. This file is created by functions within the helpers.py file.
        - `generated_source_query.sql`: This SQL file contains the query logic for . It may be used to define the data extraction or transformation steps before applying anonymisation techniques.

2. **main.py**: The entry point for running the authorized view service. This Python file triggers the authorized view logic defined in other parts of the project. It integrates with cloud platforms or systems and processes data based on the configurations set in the service.

3. **requirements.txt**: This file lists all the Python dependencies that are required to run the anonymisation service. These include libraries for interacting with databases, performing encryption, and any other necessary packages.

---

## Dependencies

Install the dependencies using `pip`:

```bash
pip install -r requirements.txt
