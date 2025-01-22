CREATE OR REPLACE TABLE `{{compliance_project}}.compilance_database.{{temp_encr_table}}` AS (
    WITH unique_keys AS (
        SELECT 
            DISTINCT contacts.national_id AS ssn,
            KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256') AS aead_key ,
            GENERATE_UUID() AS uuid
        FROM (
            SELECT b.*
            FROM `{{exposure_project}}.{{gdpr_events_dataset}}.gdpr_events` b
            WHERE compliance_event != 'flag_gdpr_5_year_complaint'
            AND is_valid_national_id != 'invalid' -- #TODO address when decide how to encrypt invalid national id's
        ) AS contacts
    )

    SELECT 
        uk.uuid,
        contacts.national_id AS ssn,
        CAST(uk.aead_key AS BYTES) AS aead_key,
        TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(
            uk.aead_key, 
            CAST(contacts.national_id AS BYTES),
            CAST(uk.uuid as bytes))
        ) AS encrypted_ssn,
        FALSE AS is_anonymized,
        contacts.is_valid_national_id AS is_valid_national_id,
        CURRENT_TIMESTAMP() AS ingestion_timestamp
    FROM unique_keys uk
    JOIN (
        SELECT b.*
        FROM `{{exposure_project}}.{{gdpr_events_dataset}}.gdpr_events` b
        WHERE compliance_event != 'flag_gdpr_5_year_complaint'
    ) AS contacts ON contacts.national_id = uk.ssn
);

INSERT INTO `{{compliance_project}}.compilance_database.{{gdpr_vault_table}}` (uuid, ssn, aead_key, encrypted_ssn, is_anonymized, 
    is_valid_national_id, ingestion_timestamp)
SELECT 
    uuid,
    ssn,
    aead_key,
    encrypted_ssn,
    is_anonymized,
    is_valid_national_id,
    ingestion_timestamp
FROM `{{compliance_project}}.compilance_database.{{temp_encr_table}}` AS temp_data
WHERE NOT EXISTS (
    SELECT 1
    FROM `{{compliance_project}}.compilance_database.{{gdpr_vault_table}}` AS existing
    WHERE existing.ssn = temp_data.ssn
);