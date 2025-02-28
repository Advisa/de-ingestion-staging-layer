SELECT 
    ssn as national_id,
    customer_id,
    ARRAY(SELECT DISTINCT email FROM UNNEST(emails) AS email WHERE email != 'null' ) AS emails,
    ARRAY(SELECT DISTINCT phone FROM UNNEST(mobile_phones) AS phone WHERE phone != 'null') AS mobile_phones,
    vault.market,
    last_brand_interaction,
    compliance_event,
FROM `{{compliance_project}}.compilance_database.{{gdpr_vault_table}}` vault
LEFT JOIN `{{exposure_project}}.{{gdpr_events_dataset}}.gdpr_events` gdpr
on vault.ssn = gdpr.national_id
and vault.market = gdpr.market
WHERE DATE(vault.ingestion_timestamp) = current_date('Europe/Stockholm')
and is_anonymized = True
{{limit_clause}}

-- ## -- ## -- ## -- ## -- ## -- ## -- ## RENDERED SQL BELOW -- ## -- ## -- ## -- ## -- ## -- ## -- ##
-- SELECT 
--     ssn as national_id,
--     customer_id,
--     ARRAY(SELECT DISTINCT email FROM UNNEST(emails) AS email WHERE email != 'null' ) AS emails,
--     ARRAY(SELECT DISTINCT phone FROM UNNEST(mobile_phones) AS phone WHERE phone != 'null') AS mobile_phones,
--     vault.market,
--     last_brand_interaction,
--     compliance_event,
-- FROM `sambla-group-compliance-db.compilance_database.gdpr_vault_rudolf` vault
-- LEFT JOIN `data-domain-data-warehouse.dbt_16a02f5fff.gdpr_events` gdpr
-- on vault.ssn = gdpr.national_id
-- and vault.market = gdpr.market
-- WHERE DATE(vault.ingestion_timestamp) = current_date('Europe/Stockholm')
-- and is_anonymized = True
-- limit 100