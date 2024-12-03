from google.cloud import datacatalog_v1
import json

# Initialize the Data Catalog client using the default credentials
def create_data_catalog_client():
    client = datacatalog_v1.PolicyTagManagerClient()
    return client

# Function to create policy tags dynamically under a parent tag
def create_policy_tags(client, taxonomy_id, normalized_data, project_id, parent_policy_tag):
    # The parent path for the policy tags should be the taxonomy path
    taxonomy_path = f"projects/{project_id}/locations/europe-north1/taxonomies/{taxonomy_id}"

    print(f"Taxonomy Path: {taxonomy_path}")  # Logging for taxonomy path

    # Build the full resource name of the parent policy tag
    parent_tag = f"projects/{project_id}/locations/europe-north1/taxonomies/{taxonomy_id}/policyTags/{parent_policy_tag}"

    for entry in normalized_data:
        # Create a policy tag for each column under the specified parent policy tag
        for column in entry['columns']:
            policy_tag = datacatalog_v1.PolicyTag()
            policy_tag.display_name = column
            policy_tag.description = f"Policy tag for {column}"

            # Set the parent_policy_tag to the existing parent tag
            policy_tag.parent_policy_tag = parent_tag

            print(f"Creating policy tag for column: {column}")  # Logging for columns

            try:
                # Create the policy tag under the parent policy tag
                new_policy_tag = client.create_policy_tag(
                    parent=taxonomy_path,  # Taxonomy path, not a policy tag path
                    policy_tag=policy_tag
                )
                print(f"Created policy tag: {new_policy_tag.display_name} under parent tag {parent_policy_tag}")
            except Exception as e:
                print(f"Failed to create policy tag for {column}: {e}")

# Normalize your input data as previously discussed
def normalize_json_with_policy_tags(data):
    taxonomy_name = "gdpr_compliance"
    parent_taxonomy = "gdpr_5year_compliant"
    policy_tag_columns = {}

    # Loop through the categories and fields to prepare policy tags for each column
    for category, fields in data['high_sensitivity_tags'].items():
        for parent, details in fields.items():
            children = details.get('children', [])
            
            # Ensure the parent column (like 'email') is also added to the columns list
            all_columns = [parent] + children  # Add parent column first

            policy_tag = f"gdpr_{parent.lower()}_tag"
            if policy_tag not in policy_tag_columns:
                policy_tag_columns[policy_tag] = []
            policy_tag_columns[policy_tag].extend(all_columns)  # Add both parent and children

    result = []
    # Collect columns under each policy tag in the expected format
    for policy_tag, columns in policy_tag_columns.items():
        result.append({
            "taxonomy": taxonomy_name,
            "parent": parent_taxonomy,
            "columns": columns
        })
    return result

# Read data from your input JSON file
def read_json_from_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

# Path to your input JSON file
file_path = '/Users/aruldharani/Sambla/de-ingestion-staging-layer-1/prod/schemas/policy_tags/taxonomy_struct_test.json'

# Read the data
data = read_json_from_file(file_path)

# Normalize the JSON structure
normalized_result = normalize_json_with_policy_tags(data)

# Specify your project ID (replace with your actual project ID)
project_id = "sambla-data-staging-compliance"

# Create the Data Catalog client
client = create_data_catalog_client()

# Create policy tags under the existing taxonomy
create_policy_tags(client, '6126692965998272750', normalized_result, project_id, "1064433561942680153")
