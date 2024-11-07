from google.cloud import bigquery
import subprocess
import re

def get_rahalaitos_ext_table(project_id,dataset_id,location):
    # initialize the bigquery client
    client = bigquery.Client(location=location,project=project_id)
    # sql query to retrieve the names and ddl of rahalaitos external tables 
    ## that use gcs paths from the rahalaitos data source, where data is loaded daily
    query = f"""
        SELECT table_name,ddl
        FROM {dataset_id}.INFORMATION_SCHEMA.TABLES 
        WHERE ddl LIKE '%gs://rahalaitos-data-dump/%';
    """
    try:
        # run the query and store the results
        query_tables = client.query(query)
        results = query_tables.result()
        
        # define the lists to hold table names and GCS locations
        table_names = []
        gcs_locations = []

        # regular expression to extract the gcs paths from the ddl column values
        gcs_pattern = r'gs://[^"]+'

        for row in results:
            table_names.append(row.table_name)
            ddl = row.ddl
            # extract all gcs locations from the ddl
            # this ensures that when creating external tables in a new project,the tables will reference valid gcs paths for the data
            matches = re.findall(gcs_pattern, ddl)
            gcs_locations.extend(matches)

        return table_names, gcs_locations
    except Exception as e:
        # if no table names are found in the desired gcs location, print a message indicating that
        print(f"An Error occured: {e}")
        return [], []
    

def generate_and_run_bq_commands(project,dataset,table_names):
    # check if there are any table names provided
    if table_names:
       for table_name in table_names:
        # construct the bq command to retrieve the schema in pretty json format
        # the output will be saved to a json file (e.g., table_name_sch ma.json)
        bq_command = f'bq show --format=prettyjson --schema {project}:{dataset}.{table_name} > {table_name}_schema.json'
        print(f"Running command: {bq_command}")
        
        try:
            # run the bq command in the terminal; 
            # make sure you are authenticated to gcloud before running this
            subprocess.run(bq_command, shell=True, check=True)
        except Exception as e:
            # if no table names are found, print a message indicating that
            print(f"Failed to execute command: {bq_command}")
            print("Error:", e)
        
    else:
        print("No tables found.")

def generate_file_txt(name,table_names, gcs_locations):
    if table_names and gcs_locations:
        with open(name, 'w') as file:
            # for each table_name and its corresponding gcs location, write them on the same line
            # using a comma (",") as the separator
            for table_name, gcs_uri in zip(table_names, gcs_locations):
                file.write(f"{table_name},{gcs_uri}\n")
    else:
        print("No valid table names or GCS locations found.")
           


def main():
    # define the parameters for the project and dataset in bigquery for rahalaitos
    # The project id, dataset id, and location are specified for the external tables
    project_id = "data-domain-data-warehouse"
    dataset_id = "rahalaitos_data"
    location = "europe-north1"
    # retrieve external table names and their corresponding gcs locations
    table_names, gcs_locations = get_rahalaitos_ext_table(project_id, dataset_id, location)
    print(table_names)
    # generate and execute bigquery commands to create json schema files for external tables
    generate_and_run_bq_commands(project_id,dataset_id,table_names)
    # generate a text file that maps the external table names to their respective gcs bucket locations
    generate_file_txt("raha_external_table_info.txt", table_names, gcs_locations)



if __name__ == "__main__":
    main()