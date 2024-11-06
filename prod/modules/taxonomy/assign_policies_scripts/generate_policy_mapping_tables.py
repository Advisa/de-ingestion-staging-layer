import csv
import subprocess
# Set variables
LOCATION = "europe-north1"  # Set your location
PROJECT_ID = "sambla-data-staging-compliance"  # Set your project ID

def list_taxonomies(location):
    """List all taxonomies in the specified location and return their details."""
    command = [
        'gcloud', 'beta', 'data-catalog', 'taxonomies', 'list',
        '--location', location,
        '--format', 'yaml'
    ]
    
    result = subprocess.run(command, capture_output=True, text=True)
    taxonomies = []

    # Parse the YAML output to extract taxonomy information
    for entry in result.stdout.split('---'):
        if entry.strip():  # Ensure entry is not empty
            taxonomy_info = {}
            for line in entry.splitlines():
                line = line.strip()
                if line.startswith('displayName:'):
                    taxonomy_info['displayName'] = line.split('displayName: ')[1].strip()
                elif line.startswith('name:'):
                    taxonomy_info['name'] = line.split('name: ')[1].strip()
            if taxonomy_info:  # If taxonomy_info is not empty, append it
                taxonomies.append(taxonomy_info)
    return taxonomies
def retrieve_policy_tags(location, taxonomy_id):
    """Retrieve all policy tags for the specified taxonomy ID."""
    command = [
        'gcloud', 'beta', 'data-catalog', 'taxonomies', 'policy-tags', 'list',
        '--taxonomy', taxonomy_id,
        '--location', location,
        '--format', 'yaml'
    ]
    
    result = subprocess.run(command, capture_output=True, text=True)
    tags_info = []

    # Parse the YAML output
    for entry in result.stdout.split('---'):
        if entry.strip():  # Ensure entry is not empty
            tag_info = {}
            for line in entry.splitlines():
                line = line.strip()
                if line.startswith('displayName:'):
                    tag_info['displayName'] = line.split('displayName: ')[1].strip()
                elif line.startswith('name:'):
                    tag_info['name'] = line.split('name: ')[1].strip()
                elif line.startswith('parentPolicyTag:'):
                    tag_info['parentPolicyTag'] = line.split('parentPolicyTag: ')[1].strip()
            if tag_info:  # If tag_info is not empty, append it
                tags_info.append(tag_info)

    return tags_info

def extract_id(full_id):
    """Extract the last part of the ID (e.g., taxonomy or policy tag ID) from the full ID."""
    return full_id.split('/')[-1]  # Split by '/' and take the last part

def write_taxonomies_to_csv(taxonomies, file_path):
    """Write taxonomy data to CSV file."""
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['taxonomy_display_name', 'description', 'id'])  # Define columns
        
        for taxonomy in taxonomies:
            taxonomy_display_name = taxonomy['displayName']
            taxonomy_id = extract_id(taxonomy['name'])  # Extract just the ID
            description = ''  # Add logic for description if needed

            writer.writerow([taxonomy_display_name, description, taxonomy_id])

    print(f"Taxonomy data written to {file_path}")

def write_policy_tags_to_csv(taxonomies, file_path):
    """Write policy tags data to CSV file, including taxonomy ID."""
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['taxonomy_id', 'policy_tag_id', 'display_name', 'description', 'parent_policy_tag_id'])

        for taxonomy in taxonomies:
            taxonomy_id = extract_id(taxonomy['name'])  # Extract the taxonomy ID
            tags_info = retrieve_policy_tags(LOCATION, taxonomy['name'])
            for tag_info in tags_info:
                display_name = tag_info['displayName']
                tag_id = extract_id(tag_info['name'])  # Extract just the ID
                description = ''  # Add logic for description if needed
                parent_policy_tag_id = tag_info.get('parentPolicyTag', None)
                is_parent_policy_tag_id = extract_id(parent_policy_tag_id) if parent_policy_tag_id else None

                # Write the row with the taxonomy_id
                writer.writerow([taxonomy_id, tag_id, display_name, description, is_parent_policy_tag_id])

    print(f"Policy tags data written to {file_path}")


# Example usage
taxonomies = list_taxonomies(LOCATION)

# Write taxonomies and policy tags data to CSV
write_taxonomies_to_csv(taxonomies, '/Users/duygugenc/Documents/de-ingestion-staging-layer/prod/modules/taxonomy/assign_policies_scripts/taxonomy.csv')
write_policy_tags_to_csv(taxonomies, '/Users/duygugenc/Documents/de-ingestion-staging-layer/prod/modules/taxonomy/assign_policies_scripts/policy_tags.csv')