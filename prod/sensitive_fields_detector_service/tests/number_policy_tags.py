import json
import pandas as pd

def process_sensitivity(data, sensitivity_level):
    """
    Processes the input JSON data for a given sensitivity level
    and generates a detailed report with categories, parents, children, and their masking rules.
    """
    sensitivity_data = data.get(f"{sensitivity_level}_sensitivity_tags_prod", {})

    # Initialize lists for storing rows
    report_rows = []
    summary_data = {
        "sensitivity_level": sensitivity_level,
        "total_categories": 0,
        "total_parents": 0,
        "total_children": 0,
    }

    category_count = 0
    parent_count = 0
    child_count = 0

    for category, category_data in sensitivity_data.items():
        category_count += 1

        parents = category_data  # Parents are directly under category_data
        if not parents:
            continue

        for parent, parent_data in parents.items():
            parent_count += 1  # Count the parents
            
            # Extract parent masking rule (if available)
            parent_masking_rule = parent_data.get("masking_rule", "UNKNOWN")
            parent_data_type = parent_data.get("type", "UNKNOWN")

            children = parent_data.get("children", {})
            if isinstance(children, dict) and children:
                child_count += len(children)  # Count the children
                
                for child, child_data in children.items():
                    child_masking_rule = child_data.get("masking_rule", "UNKNOWN")  # Get child masking rule
                    child_data_type = child_data.get("type", "UNKNOWN")
                    
                    report_rows.append({
                        "sensitivity_level": sensitivity_level,
                        "category_name": category,
                        "parent_name": parent,
                        "parent_masking_rule": parent_masking_rule,
                        "parent_data_type": parent_data_type,   # Include parent masking rule
                        "child_name": child,
                        "child_masking_rule": child_masking_rule,
                        "child_data_type": child_data_type,  # Include child masking rule
                    })

    # Store the summary data
    summary_data["total_categories"] = category_count
    summary_data["total_parents"] = parent_count
    summary_data["total_children"] = child_count
    summary_data["total_count"] = category_count + parent_count + child_count

    return report_rows, summary_data


def generate_excel_report(json_file_path, output_excel):
    """
    Generates an Excel report with two sheets:
    1. Detailed breakdown of categories, parents, children, and masking rules.
    2. Summary of total counts.
    """
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    sensitivity_levels = ['high', 'medium', 'low']
    all_report_rows = []
    summary_rows = []

    for sensitivity_level in sensitivity_levels:
        report_rows, summary_data = process_sensitivity(data, sensitivity_level)
        all_report_rows.extend(report_rows)
        summary_rows.append(summary_data)

    # Convert lists to Pandas DataFrames
    df_report = pd.DataFrame(all_report_rows)
    df_summary = pd.DataFrame(summary_rows)

    # Write to Excel
    with pd.ExcelWriter(output_excel) as writer:
        df_report.to_excel(writer, sheet_name="Detailed Report", index=False)
        df_summary.to_excel(writer, sheet_name="Summary", index=False)

    print(f"Excel report saved as '{output_excel}'")


# Run the function
json_file_path = '/prod/schemas/policy_tags/sensitive_fields_updated.json'
output_excel = 'gdpr_compliance_report.xlsx'

generate_excel_report(json_file_path, output_excel)
