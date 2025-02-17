with raw_data AS (
    select * except(consents), consents.* FROM `${project_id}.${dataset_id}.clients_lvs_r`
)

select * except(ssn), CASE 
        WHEN SAFE_CAST(ssn AS INT64) = 0 THEN NULL
        WHEN 'FI' = 'SE' THEN
            left(REGEXP_REPLACE(cast(ssn AS string),'[^0-9]',''),12)
        WHEN 'FI' = 'NO' THEN
            left(REGEXP_REPLACE(cast(ssn AS string),'[^0-9]',''),11)
        WHEN 'FI' = 'DK' THEN
            left(REGEXP_REPLACE(cast(ssn AS string),'[^0-9]',''),10)
        WHEN 'FI' = 'FI' THEN
            left(REGEXP_REPLACE(cast(UPPER(ssn) AS string),'[^0-9-+A-Z]',''),11)
        ELSE
            REGEXP_REPLACE(cast(ssn as string),'[^0-9]','')
    END national_id,
FROM raw_data
where true