#!/bin/bash

# Set your project and dataset
PROJECT_ID="data-domain-data-warehouse"
DATASET_ID="helios_staging"

# Path to your list of table names
TABLE_LIST="tables_list.txt"

# Path to the folder where you want to save the schema files
OUTPUT_FOLDER="/Users/aruldharani/Sambla/de-ingestion-staging-layer-2/prod/schemas/sambla_legacy"

# Ensure the output folder exists (create it if it doesn't)
mkdir -p "$OUTPUT_FOLDER"

# Loop through each table in the list
while IFS= read -r TABLE; do
    # Check if the table ends with sambq_p
    if [[ "$TABLE" == *"sambq_p" ]]; then
        echo "Exporting schema for table: $TABLE"
        # Export full schema and filter out everything except the "fields" section
        bq show --format=prettyjson ${PROJECT_ID}:${DATASET_ID}.${TABLE} > "${OUTPUT_FOLDER}/${TABLE}_fields_schema.json"
        echo "Schema fields exported to ${OUTPUT_FOLDER}/${TABLE}_fields_schema.json"
    else
        echo "Skipping table: $TABLE (doesn't match pattern)"
    fi
done < "$TABLE_LIST"
