WITH table_columns AS (
    -- replace this with your actual query for table columns
 SELECT * FROM `{{raw_layer_project}}.maxwell_integration_legacy`.INFORMATION_SCHEMA.COLUMNS
UNION ALL 
SELECT * FROM `{{raw_layer_project}}.lvs_integration_legacy`.INFORMATION_SCHEMA.COLUMNS
UNION ALL 
SELECT * FROM `{{raw_layer_project}}.salus_integration_legacy`.INFORMATION_SCHEMA.COLUMNS
UNION ALL 
SELECT * FROM `{{raw_layer_project}}.advisa_history_integration_legacy`.INFORMATION_SCHEMA.COLUMNS
--UNION ALL 
--SELECT * FROM `{{raw_layer_project}}.sambla_legacy_integration_legacy`.INFORMATION_SCHEMA.COLUMNS
UNION ALL 
SELECT * FROM `{{raw_layer_project}}.rahalaitos_integration_legacy`.INFORMATION_SCHEMA.COLUMNS
),

table_columns_filtered AS (
    SELECT 
      * 
    FROM table_columns 
    WHERE 
      -- discard the raw layer tables that is not in use 
      table_name NOT LIKE '%_lvs_r'
),


policy_tags_all AS (
  SELECT *
  FROM `{{raw_layer_project}}.policy_tags_metadata.policy_tags`
  -- gdpr_compliance_measures_prod_high, gdpr_compliance_measures_prod_medium, gdpr_compliance_measures_prod_low
  WHERE taxonomy_id IN ('462501529798891334', '1348545653474742340', '8452725999489655507')
),

policy_tags_pii_child_tags AS (
  SELECT 
    t1.display_name
  FROM 
    policy_tags_all t1 
  INNER JOIN 
    policy_tags_all t2 
  ON t1.parent_policy_tag_id = t2.policy_tag_id
  WHERE t2.display_name IN ('employer', 'email', 'phone', 'ssn', 'first_name', 'last_name', 'bank_account_number', 'address', 'post_code')
  
),
policy_tags_pii_parent_tags AS (
  SELECT 
    DISTINCT t2.display_name
  FROM 
    policy_tags_all t1 
  INNER JOIN 
    policy_tags_all t2 
  ON t1.parent_policy_tag_id = t2.policy_tag_id
  WHERE t2.display_name IN ('email', 'phone', 'ssn', 'first_name', 'last_name', 'bank_account_number','address', 'post_code')
  
),

policy_tags_pii AS (
  SELECT * FROM policy_tags_pii_child_tags
  UNION ALL
  SELECT * FROM policy_tags_pii_parent_tags
),


sensitive_fields AS (
  -- Identify non-struct sensitive fields based on policy tags match
  SELECT
    r.table_schema,
    r.table_name,
    r.column_name,
    LOWER(REPLACE(r.column_name, "_", "")) AS normalized_column
  FROM table_columns_filtered r
  INNER JOIN policy_tags_pii p 
    ON LOWER(REPLACE(r.column_name, "_", "")) = LOWER(REPLACE(p.display_name, "_", ""))
  WHERE r.data_type NOT LIKE "STRUCT%" 
    AND r.data_type NOT LIKE "ARRAY%" 
),


join_keys AS (
  -- Identify join keys for linking tables to VAULT
  SELECT
    table_schema,
    table_name,
    ARRAY_AGG(column_name) AS join_keys,
    -- Create a flag to mark tables that includes ssn
    IF (
      "ssn" IN UNNEST(ARRAY_AGG(normalized_column)) 
      OR "customerssn" IN UNNEST(ARRAY_AGG(normalized_column)) 
      OR "foreignerssn" IN UNNEST(ARRAY_AGG(normalized_column))
      OR "ssnid" IN UNNEST(ARRAY_AGG(normalized_column)) 
      OR "nationalid" IN UNNEST(ARRAY_AGG(normalized_column)) 
      OR "sotu" IN UNNEST(ARRAY_AGG(normalized_column)) 
      OR "yvsotu" IN UNNEST(ARRAY_AGG(normalized_column)),
      TRUE, FALSE
    ) AS is_table_contains_ssn
  FROM sensitive_fields
  GROUP BY table_schema, table_name
),

unnested_join_keys AS (
  -- Extract sensitive fields and parent fields
  SELECT
    table_schema,
    table_name,
    join_key AS sensitive_field,
    is_table_contains_ssn,
    CASE 
      WHEN table_name != "people_adhis_r" 
        AND LOWER(REPLACE(join_key, "_", "")) 
        IN ('ssn', 'ssnid', 'nationalid', 'customerssn', 'sotu', 'yvsotu') 
        THEN  join_key
     -- Exceptional case
      WHEN table_name = "people_adhis_r" 
        THEN "national_id_sensitive" 
    END AS j_key
  FROM join_keys, UNNEST(join_keys.join_keys) AS join_key
),


non_nested_field_encryption AS (
  -- Encrypt standard fields
  SELECT
    table_schema,
    table_name,
    STRING_AGG(DISTINCT CONCAT(
      "CASE WHEN VAULT.uuid IS NOT NULL THEN ",
      "TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, CAST(raw.", sensitive_field, " AS STRING), VAULT.uuid)) ",
      "ELSE CAST(raw.", sensitive_field, " AS STRING) END AS ", sensitive_field
    )) AS encrypted_fields
  FROM unnested_join_keys
  GROUP BY table_schema, table_name
),


market_legacystack_mapping AS (
  SELECT 
    t1.table_schema,
    t1.table_name,
    t2.is_table_contains_ssn,
    t2.sensitive_field,
    t2.j_key,
    t1.encrypted_fields,
    CASE 
      -- add cdc source dataset later on
      WHEN t1.table_schema IN ( 'advisa_history_integration_legacy', 'maxwell_integration_legacy') THEN 'SE'
      WHEN t1.table_schema IN ('rahalaitos_integration_legacy', 'lvs_integration_legacy') THEN 'FI'
      ELSE 'OTHER MARKETS'
    END AS market_identifier
  FROM 
    non_nested_field_encryption t1
  INNER JOIN unnested_join_keys t2 
  ON t1.table_name = t2.table_name AND t1.table_schema = t2.table_schema
),


final AS (
  -- Construct the final query for each table
  SELECT
    table_schema,
    table_name,
    is_table_contains_ssn,
    market_identifier,
    CASE 
      WHEN is_table_contains_ssn THEN CONCAT(
        'WITH data_with_ssn_rules AS (',
        'SELECT ',
        '*, ',
        -- Market-specific logic for SSN extraction
        CASE 
          WHEN market_identifier = 'OTHER MARKETS' THEN 
          CONCAT(
            'CASE ',
            'WHEN ', CASE WHEN table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p') THEN 'market' ELSE 'country_code' END ,'= "SE" THEN LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT j_key, ', '), ' AS STRING), "[^0-9]", ""), 12) ',
            'WHEN ', CASE WHEN table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p') THEN 'market' ELSE 'country_code' END ,'= "NO" THEN LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT j_key, ', '), ' AS STRING), "[^0-9]", ""), 11) ',
            'WHEN ', CASE WHEN table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p') THEN 'market' ELSE 'country_code' END ,'= "DK" THEN LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT j_key, ', '), ' AS STRING), "[^0-9]", ""), 10) ',
            'WHEN ', CASE WHEN table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p') THEN 'market' ELSE 'country_code' END ,'= "FI" THEN LEFT(REGEXP_REPLACE(UPPER(CAST(raw.', STRING_AGG(DISTINCT j_key, ', '), ' AS STRING)), "[^0-9-+A-Z]", ""), 11) ' ,
            'END AS ssn_clean'
          ) 
          WHEN market_identifier = 'SE' THEN 
            CONCAT(
              'LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT j_key, ', '), ' AS STRING), "[^0-9]", ""), 12) AS ssn_clean'
            )
          WHEN market_identifier = 'FI' THEN 
            CONCAT(
              'LEFT(REGEXP_REPLACE(UPPER(CAST(raw.', STRING_AGG(DISTINCT j_key, ', '), ' AS STRING)), "[^0-9-+A-Z]", ""), 11) AS ssn_clean'
            )
        END
        ,
        ' FROM `{{raw_layer_project}}.', table_schema, '.', table_name, '` raw) ',
        
        'SELECT ',
        STRING_AGG(DISTINCT encrypted_fields, ", "),
        ', raw.* EXCEPT(',
        STRING_AGG(DISTINCT sensitive_field, ', '),
        ') FROM `data_with_ssn_rules` raw ',
        'LEFT JOIN `{{compliance_project}}.compilance_database.{{gdpr_vault_table}}` VAULT ',
        'ON CAST(raw.ssn_clean AS STRING) = VAULT.ssn'
      )

      ELSE CONCAT(
        'SELECT * FROM `{{raw_layer_project}}.',
        table_schema,
        '.',
        table_name,
        '`'
      ) 
      
    END AS final_encrypted_columns
FROM market_legacystack_mapping
GROUP BY table_schema, table_name, is_table_contains_ssn, market_identifier
)

SELECT * FROM final WHERE final_encrypted_columns IS NOT NULL AND table_schema = "lvs_integration_legacy"