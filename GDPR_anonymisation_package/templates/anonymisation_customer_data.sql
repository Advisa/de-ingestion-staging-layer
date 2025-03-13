SELECT 
    ssn as national_id,
    customer_id,
    ARRAY_AGG(DISTINCT IFNULL(history_entry.email, 'null')) AS history_emails,
    ARRAY_AGG(DISTINCT IFNULL(history_entry.mobile_phone, 'null')) AS history_mobile_phones,
    vault.market,
    last_brand_interaction,
    gdpr.email AS latest_email,
    gdpr.mobile_phone AS latest_mobile,
    compliance_event
FROM `{{compliance_project}}.compilance_database.{{gdpr_vault_table}}` vault
LEFT JOIN `{{exposure_project}}.{{gdpr_events_dataset}}.gdpr_events` gdpr
    ON vault.ssn = gdpr.national_id
    AND vault.market = gdpr.market
LEFT JOIN UNNEST(vault.history) AS history_entry -- Flatten the array to extract values
WHERE vault.ingestion_timestamp >= current_timestamp() - interval 12 hour
AND is_anonymized = TRUE
GROUP BY vault.ssn, vault.market, gdpr.customer_id, gdpr.last_brand_interaction, gdpr.email, gdpr.mobile_phone, compliance_event
{{limit_clause}}

-- ## -- ## -- ## -- ## -- ## -- ## -- ## RENDERED SQL BELOW -- ## -- ## -- ## -- ## -- ## -- ## -- ##
-- SELECT 
--     ssn as national_id,
--     customer_id,
--     ARRAY_AGG(DISTINCT IFNULL(history_entry.email, 'null')) AS history_emails,
--     ARRAY_AGG(DISTINCT IFNULL(history_entry.mobile_phone, 'null')) AS history_mobile_phones,
--     vault.market,
--     last_brand_interaction,
--     gdpr.email AS latest_email,
--     gdpr.mobile_phone AS latest_mobile,
--     compliance_event
-- FROM `sambla-group-compliance-db.compilance_database.gdpr_vault_rudolf` vault
-- LEFT JOIN `data-domain-data-warehouse.helios_dm_master.gdpr_events` gdpr
--     ON vault.ssn = gdpr.national_id
--     AND vault.market = gdpr.market
-- LEFT JOIN UNNEST(vault.history) AS history_entry -- Flatten the array to extract values
-- WHERE vault.ingestion_timestamp >= current_timestamp() - interval 12 hour
-- AND is_anonymized = TRUE
-- GROUP BY vault.ssn, vault.market, gdpr.customer_id, gdpr.last_brand_interaction, gdpr.email, gdpr.mobile_phone, compliance_event
-- limit 100