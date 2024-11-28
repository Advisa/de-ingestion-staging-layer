WITH pii_tables AS (
    SELECT * FROM `{{exposure_project}}.playgrounds.metadata_pii_columns`
),
table_columns AS (
    SELECT 
        table_name, 
        column_name
    FROM `{{raw_layer_project}}.lvs_integration_legacy.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_schema = 'lvs_integration_legacy'
    AND table_name IN (SELECT table_name FROM pii_tables)
),
join_keys AS (
    SELECT 
        table_name, 
        ARRAY_AGG(column_name) AS join_keys
    FROM `{{raw_layer_project}}.lvs_integration_legacy.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_schema = 'lvs_integration_legacy'
    AND (
        column_name LIKE '%ssn%' OR
        column_name LIKE '%national_id%' OR
        column_name LIKE '%ssn_id%'
    )
    GROUP BY table_name
),

unnested_join_keys AS (
    SELECT
        table_name,
        join_key 
    FROM join_keys, UNNEST(join_keys.join_keys) AS join_key 
),
encryption_queries AS (
    
    SELECT
        pt.table_name,ujk.join_key,
        CONCAT(
            'SELECT ', 
            STRING_AGG(
                CASE 
                    WHEN tc.column_name IN UNNEST(pt.pii_columns) THEN 
                        CONCAT(
                            'CASE WHEN VAULT.uuid IS NOT NULL THEN TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, raw.', tc.column_name, ', VAULT.uuid)) ELSE raw.', tc.column_name, ' END AS ', tc.column_name
                        )
                    ELSE 
                        NULL
                END,
                ', '
            ),
            ', raw.* EXCEPT(', 
            STRING_AGG(
                CASE 
                    WHEN tc.column_name IN UNNEST(pt.pii_columns) THEN tc.column_name
                    ELSE NULL
                END, ', '
            ),
            ') ', 
            'FROM `{{raw_layer_project}}.lvs_integration_legacy.', pt.table_name, '` raw '
            'LEFT JOIN `{{complaince_project}}.compilance_database.gdpr_vault` VAULT ON raw.', 
            ujk.join_key,
            ' = VAULT.ssn '
        ) AS encryption_query
    FROM pii_tables pt
    JOIN unnested_join_keys ujk ON pt.table_name = ujk.table_name 
    JOIN table_columns tc ON pt.table_name = tc.table_name
    GROUP BY pt.table_name, ujk.join_key 
)


SELECT table_name, encryption_query,join_key
FROM encryption_queries