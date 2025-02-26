UPDATE `{{ compliance_project }}.compilance_database.{{ gdpr_vault_table }}`
SET is_anonymized = TRUE, ingestion_timestamp = CURRENT_TIMESTAMP()
WHERE is_anonymized IS DISTINCT FROM TRUE
AND ssn IN (
    {{ exists_clauses }}
) -- #TODO fix below when secondary join key added
OR
market IN (
    {{ other_exists_clauses }}
);