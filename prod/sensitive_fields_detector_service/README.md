# **Sensitive Fields Detector Service**

This repository provides an end-to-end process for managing sensitive data governance tasks, including column lineage extraction, sensitive field identification, and creating policy tags for data masking. The final output includes taxonomy and policy definitions that can be deployed using Terraform for effective data masking and compliance enforcement.


sensitive_fields_detector_service/
├── main.py                         
├── requirements.txt                
├── data_input/                     
│   ├── manifest.json               
│   ├── catalog.json                
│   ├── sensitive_field_mapping.csv                 
├── data_output/                    
│   ├── column_lineage.yml          
│   ├── sensitive_fields_output.json 
├── log_files/                     
├── utils/                          
│   ├── column_lineage_extractor.py 
│   ├── sensitive_fields_processor.py 
│   └── config.yml                  


## **Column Lineage Extraction**

**Script**: `column_lineage_extractor.py`  
This script analyzes dbt metadata (`manifest.json` and `catalog.json`) to map relationships between source and target columns across datasets.

### **Key Features**:
- Automates the creation of column-level lineage maps.
- Outputs a YAML file (`column_lineage.yml`) containing lineage details.

### **Sample**:
```yaml
target_table.column_name:
  source_column: source_table.column_name
  source_model: "model_name"
```

## **Sensitive Field Identification**

**Script**: `sensitive_fields_processor.py`  
This script uses the column lineage map to identify sensitive fields based on configuration rules. Fields are categorized into sensitivity levels (e.g., PII, Restricted, Confidential).

### **Key Features**:
- Cleans and processes sensitive field data from a CSV.
- Groups related columns and assigns sensitivity tags.
- Outputs a JSON file (`sensitive_fields_output.json`) with grouped columns.

### **Sample**:
```json
{
  "high_sensitivity_tags": {
    "PII": {
      "email": {
        "children": ["email", "user_email"],
        "sensitivity": "high",
        "masking_rule": "HASH",
        "category": "PII"
      }
    }
  }
}
```

## **Taxonomy and Policy Tags**

Policy tags define hierarchical taxonomies for enforcing data masking and security policies. These are generated dynamically using the resolved columns JSON.

### **Proposed Structure**:

**Taxonomy**: `Data_Sensitivity`
```
  ├── High
  │   ├── PII
  │   ├── Confidential
  ├   ├── Restricted
  ├── Medium
  └── Low
```

### **Folder**: `prod/modules/policy_tags`

Terraform is used to define and deploy policy tags into BigQuery.

prod/
├── modules/
│   └── policy_tags/
│       ├── main.tf                    
│       ├── variables.tf              
│── schema/
│    └── policy_tags/
│       └── sensitive_fields_output.json  



### **Key Files**:

- **`main.tf`**:  
  This `main.tf` script automates the creation of Google Data Catalog taxonomies, policy tags, and BigQuery data masking policies for data governance. It dynamically generates hierarchical tags (categories, parents, and children) based on sensitivity levels (high, medium, low) using a JSON configuration. Masking policies like SHA256 or DEFAULT_MASKING_VALUE are applied to sensitive data. The setup ensures compliance with data protection regulations and simplifies managing sensitive data classification. All configurations are scalable and easily customizable via the JSON input.

- **`variables.tf`**:  
  Contains variables for customizing the taxonomy structure.

---

## **Setup and Installation**

1. `git clone https://github.com/Advisa/de-ingestion-staging-layer.git`
2. `pip install -r requirements.txt`
3. `python main.py`
4. `terraform init`
5. `terraform plan`
6. `terraform apply`
