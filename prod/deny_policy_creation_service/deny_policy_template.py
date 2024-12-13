import os
import yaml

# Load configuration from config.yml
def load_config(config_file="config.yml"):
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
    return config

# Function to create deny policy and generate Terraform configuration
def create_deny_policy():
    config = load_config("config.yml")
    project_id = config.get("project_id")
    deny_principals = config.get("deny_principals", [])
    tag_key_id = config.get("tag_key_id")
    tag_value_id = config.get("tag_value_id")
    denied_permissions = config.get("denied_permissions", [])

    if not project_id or not deny_principals or not tag_key_id or not tag_value_id or not denied_permissions:
        print("Error: Missing required configuration values.")
        return

    # Convert deny_principals and denied_permissions into correctly formatted lists for Terraform
    deny_principals_str = ", ".join([f'"{principal}"' for principal in deny_principals])
    denied_permissions_str = ", ".join([f'"{permission}"' for permission in denied_permissions])

    # Generate Terraform HCL for the deny policy
    terraform_config = f"""
resource "google_iam_deny_policy" "deny_policy_creation" {{
  parent      = urlencode("cloudresourcemanager.googleapis.com/projects/${{var.project_id}}")  # Corrected parent format with urlencode
  name        = "gdpr-deny-policy"
  display_name = "GDPR deny policy"
  rules {{
    description = "First rule"
    deny_rule {{
      denied_principals = [{deny_principals_str}]  # Dynamic principal list
      denial_condition {{
        title       = "denial condition expression"
        expression = "resource.matchTagId(\\"tagKeys/{tag_key_id}\\", \\"tagValues/{tag_value_id}\\")"
      }}
      denied_permissions = [{denied_permissions_str}]  # Dynamic permissions list
    }}
  }}
}}
"""

    tf_file_path = "../../prod/modules/deny_policy/main.tf"
    os.makedirs(os.path.dirname(tf_file_path), exist_ok=True)
    with open(tf_file_path, "w") as f:
        f.write(terraform_config)
    
    print(f"Terraform configuration written to {tf_file_path}")

create_deny_policy()
