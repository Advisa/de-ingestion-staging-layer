import datetime
from google.cloud import bigquery
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import os


#Uncomment for testing
SCOPES = ['https://www.googleapis.com/auth/cloud-platform']

SERVICE_ACCOUNT_KEYS = {
    "raw_layer_project": "/Users/aruldharani/Downloads/sambla-data-staging-compliance-5d68a484424a.json"
}


clients = {}
for project_name, key_path in SERVICE_ACCOUNT_KEYS.items():
    if os.path.exists(key_path):
        credentials = service_account.Credentials.from_service_account_file(key_path, scopes=['https://www.googleapis.com/auth/cloud-platform'])
        credentials.refresh(Request()) 
        clients[project_name] = bigquery.Client(credentials=credentials)
    else:
        raise FileNotFoundError(f"Service account key file not found: {key_path}")



raw_layer_client = clients['raw_layer_project']

CHECKSUM_CHARS = "0123456789ABCDEFHJKLMNPRSTUVWXY"

def calculate_checksum(ssn_base):
    if not ssn_base.isdigit():
        raise ValueError(f"Invalid SSN base: {ssn_base}. Must contain only digits.")
    remainder = int(ssn_base) % 31
    return CHECKSUM_CHARS[remainder]

def clean_ssn(ssn):
    if ssn is None: 
        return "" 
    return ''.join(filter(str.isdigit, ssn))

def validate_finnish_ssn(ssn):
    if len(ssn) != 11:
        return False
    try:
        day, month, year = int(ssn[:2]), int(ssn[2:4]), int(ssn[4:6])
        century_marker = ssn[6]
        individual_number = ssn[7:10]
        checksum_char = ssn[10]
        century = {'+': 1800, '-': 1900, 'A': 2000}.get(century_marker)
        if century is None:
            return False
        birth_year = century + year
        birth_date = datetime.date(birth_year, month, day)
        if not individual_number.isdigit() or int(individual_number) < 0 or int(individual_number) > 999:
            return False
        ssn_base = ssn[:6] + ssn[7:10]
        expected_checksum = calculate_checksum(ssn_base)
        return checksum_char == expected_checksum
    except ValueError:
        return False

def correct_finnish_ssn(ssn):
    if validate_finnish_ssn(ssn):
        return ssn

    if not ssn:
        return "000000-0000"

    day = ssn[:2]
    month = ssn[2:4]
    year = ssn[4:6]
    century_marker = ssn[6] if len(ssn) > 6 and ssn[6] in '+-A' else '-'
    individual_number = ssn[7:10].zfill(3) if len(ssn) >= 9 else "000"
    
    if not day.isdigit() or not month.isdigit() or not year.isdigit():
        day, month = "01", "01"

    ssn_base = f"{day}{month}{year}{individual_number}"
    checksum = calculate_checksum(ssn_base)

    return f"{day}{month}{year}{century_marker}{individual_number}{checksum}"


def validate_swedish_ssn(ssn):
    if len(ssn) != 10 and len(ssn) != 12:
        return False
    ssn_cleaned = ssn.replace('-', '')
    if len(ssn_cleaned) != 10:
        return False
    try:
        day = int(ssn_cleaned[:2])
        month = int(ssn_cleaned[2:4])
        year = int(ssn_cleaned[4:6])
        birth_date = datetime.date(year + (1900 if year >= 0 else 2000), month, day)
    except ValueError:
        return False
    return True

def correct_swedish_ssn(ssn):
    if validate_swedish_ssn(ssn):
        return ssn
    return "000000-0000"

def validate_norwegian_ssn(ssn):
    if len(ssn) != 11:
        return False
    try:
        day, month, year = int(ssn[:2]), int(ssn[2:4]), int(ssn[4:6])
        birth_date = datetime.date(year + 1900, month, day)
    except ValueError:
        return False
    return True

def correct_norwegian_ssn(ssn):
    if validate_norwegian_ssn(ssn):
        return ssn
    return "00000000000"

def validate_danish_ssn(ssn):
    if len(ssn) != 10:
        return False
    try:
        day, month, year = int(ssn[:2]), int(ssn[2:4]), int(ssn[4:6])
        birth_date = datetime.date(year + 1900, month, day)
    except ValueError:
        return False
    return True

def correct_danish_ssn(ssn):
    if validate_danish_ssn(ssn):
        return ssn
    return "0000000000"

def validate_and_correct_ssn(ssn, country):
    if ssn is None or ssn.strip() == "":
        return "000000-0000", False  # Handle None or empty SSN case

    validation_functions = {
        "FI": validate_finnish_ssn,
        "SE": validate_swedish_ssn,
        "NO": validate_norwegian_ssn,
        "DK": validate_danish_ssn
    }

    correction_functions = {
        "FI": correct_finnish_ssn,
        "SE": correct_swedish_ssn,
        "NO": correct_norwegian_ssn,
        "DK": correct_danish_ssn
    }

    if country in validation_functions:
        if validation_functions[country](ssn):
            return ssn, True
        else:
            corrected_ssn = correction_functions[country](ssn)
            return corrected_ssn, False
    else:
        raise ValueError("Unsupported country code")

def process_raw_ssns(client, dataset_name):
    possible_ssn_columns = ["ssn", "ssn_id", "national_id"]
    tables = client.list_tables(dataset_name)
    
    results = []
    
    for table in tables:
        table_id = table.table_id
        if "lvs" in table_id.lower():
            country = "FI"
        elif "se" in table_id.lower():
            country = "SE"
        elif "no" in table_id.lower():
            country = "NO"
        elif "dk" in table_id.lower():
            country = "DK"
        else:
            continue

        table_ref = client.dataset(dataset_name).table(table_id)
        table_schema = client.get_table(table_ref).schema
        
        ssn_column = next((field.name for field in table_schema if field.name in possible_ssn_columns), None)
        
        if not ssn_column:
            print(f"No SSN column found in {table_id}. Skipping this table.")
            continue

        query = f"SELECT {ssn_column} FROM `{dataset_name}.{table_id}`"
        ssn_rows = client.query(query).result()
        
        for row in ssn_rows:
            ssn = getattr(row, ssn_column)
            if ssn:
                ssn = clean_ssn(ssn)
                corrected, valid = validate_and_correct_ssn(ssn, country)
                results.append({
                    "original_ssn": ssn,
                    "corrected_ssn": corrected,
                    "valid": valid,
                    "table": table_id
                })
            else:
                print(f"Null SSN value in {table_id}")

    return results

def main(raw_layer_client,dataset_name):
    results = process_raw_ssns(raw_layer_client,dataset_name)
    return {"results": results}

if __name__ == '__main__':
    dataset_name = "lvs_integration_legacy" 
    results = main(raw_layer_client,dataset_name)
    print(results)

