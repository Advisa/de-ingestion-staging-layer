UPDATE `{{ compliance_project }}.compilance_database.{{ gdpr_vault_table }}`
SET is_anonymized = TRUE, ingestion_timestamp = CURRENT_TIMESTAMP()
WHERE encrypted_ssn IN (
    {{ exists_clauses }}
);