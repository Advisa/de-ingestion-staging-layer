import os
import yaml
from google.auth import default
from googleapiclient.discovery import build

# Load configuration from config.yml
def load_config(config_file="config.yml"):
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
    return config

# Function to fetch service accounts from the project
def fetch_service_accounts(project_id):
    credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    service = build('iam', 'v1', credentials=credentials)

    # Get the list of service accounts in the project
    service_accounts = []
    request = service.projects().serviceAccounts().list(name=f'projects/{project_id}')
    while request is not None:
        response = request.execute()
        for account in response.get('accounts', []):
            service_accounts.append(f"serviceAccount:{account['email']}")
        request = service.projects().serviceAccounts().list_next(previous_request=request, previous_response=response)

    return service_accounts

# Function to fetch all users in the project (including groups, service accounts, etc.)
def fetch_all_iam_users(project_id):
    credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    service = build('cloudresourcemanager', 'v1', credentials=credentials)
    users = []
    request = service.projects().getIamPolicy(resource=project_id)
    response = request.execute()

    for binding in response.get('bindings', []):
        for member in binding.get('members', []):
            users.append(member)

    return users

def create_deny_policy():
    config = load_config("config.yml")
    project_id = config.get("project_id")
    tag_key_id = config.get("tag_key_id")
    tag_value_id = config.get("tag_value_id")
    denied_permissions = config.get("denied_permissions", [])

    if not project_id or not tag_key_id or not tag_value_id or not denied_permissions:
        print("Error: Missing required configuration values.")
        return


    service_accounts = fetch_service_accounts(project_id)

    all_users = fetch_all_iam_users(project_id)

    allow_principals = [
        *service_accounts,
        'group:data@samblagroup.com'
    ]
    
    deny_principals = [user for user in all_users if user not in allow_principals and not user.startswith("serviceAccount:")]

    deny_principals = list(set(deny_principals))

    deny_principals_str = ", ".join([f'"{principal}"' for principal in deny_principals])
    denied_permissions_str = ", ".join([f'"{permission}"' for permission in denied_permissions])

    terraform_config = f"""
    resource "google_iam_deny_policy" "deny_policy_creation" {{
    parent      = "cloudresourcemanager.googleapis.com/projects/${{var.project_id}}"  # Corrected parent format
    name        = "gdpr-deny-policy"
    display_name = "GDPR deny policy"
    rules {{
        description = "First rule"
        deny_rule {{
        denied_principals = ["principalSet://goog/public:all"]  # Denying everyone
        exception_principals = [{', '.join([f'"{sa}"' for sa in service_accounts])}]  # Allowing service accounts
        denial_condition {{
            title       = "denial condition expression"
            expression = ""
        }}
        denied_permissions = [{denied_permissions_str}]  # Permissions to be denied
        }}
    }}
    }}
"""

    tf_file_path = "../../prod/deny_policy_creation_service/main.tf"
    os.makedirs(os.path.dirname(tf_file_path), exist_ok=True)
    with open(tf_file_path, "w") as f:
        f.write(terraform_config)
    
    print(f"Terraform configuration written to {tf_file_path}")

create_deny_policy()
