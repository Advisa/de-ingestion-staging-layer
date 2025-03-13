WITH table_columns AS (
    -- Replace this with your actual query for table columns
    SELECT * FROM `{{raw_layer_project}}.maxwell_integration_legacy`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    WHERE TABLE_NAME LIKE '%sgmw_p'
    UNION ALL 
    SELECT * FROM `{{raw_layer_project}}.lvs_integration_legacy`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
    WHERE TABLE_NAME NOT LIKE '%_lvs_r'
    UNION ALL 
    SELECT * FROM `{{raw_layer_project}}.salus_integration_legacy`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    UNION ALL 
    SELECT * FROM `{{exposure_project}}.salus_group_integration`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    WHERE TABLE_NAME LIKE '%_salus_incremental_r'
    UNION ALL
    SELECT * FROM `{{raw_layer_project}}.advisa_history_integration_legacy`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    UNION ALL 
    SELECT * FROM `{{raw_layer_project}}.sambla_legacy_integration_legacy`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
    WHERE TABLE_NAME LIKE '%sambq_p'
    UNION ALL 
    SELECT * FROM `{{raw_layer_project}}.rahalaitos_integration_legacy`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
),

policy_tags_all AS (
  SELECT *
  FROM `{{raw_layer_project}}.policy_tags_metadata.policy_tags`
  -- gdpr_compliance_measures_prod_high, gdpr_compliance_measures_prod_medium, gdpr_compliance_measures_prod_low
  WHERE taxonomy_id IN ('7698000960465061299', '8248486934170083143', '655384675748637071')
),

policy_tags_pii_child_tags AS (
  SELECT 
    t1.display_name
  FROM 
    policy_tags_all t1 
  INNER JOIN 
    policy_tags_all t2 
  ON t1.parent_policy_tag_id = t2.policy_tag_id
  WHERE t2.display_name IN ('employer', 'email', 'phone', 'ssn', 'first_name', 'last_name', 'bank_account_number', 'address', 'post_code','data','business_organization_number','attributes_raw_json','employment_industry')
  
),
policy_tags_pii_parent_tags AS (
  SELECT 
    DISTINCT t2.display_name
  FROM 
    policy_tags_all t1 
  INNER JOIN 
    policy_tags_all t2 
  ON t1.parent_policy_tag_id = t2.policy_tag_id
  WHERE t2.display_name IN ('email', 'phone', 'ssn', 'first_name', 'last_name', 'bank_account_number','address', 'post_code','data','business_organization_number','attributes_raw_json','employment_industry')
  
),

policy_tags_pii AS (
  SELECT * FROM policy_tags_pii_child_tags
  UNION ALL
  SELECT * FROM policy_tags_pii_parent_tags
),

sensitive_fields AS (
    -- Identify non-struct and struct sensitive fields based on policy tag match
    SELECT
        r.table_schema,
        r.table_name,
        r.column_name,
        r.field_path,
        r.data_type,
        LOWER(REPLACE(
            CASE 
                WHEN r.field_path LIKE '%.%' THEN SUBSTR(r.field_path, STRPOS(r.field_path, '.') + 1)
                ELSE r.field_path
            END, "_", ""
        )) AS normalized_column,
        CASE 
        -- adding this to handle this edge as this column is array string but we can remove the table_name and column_name in the future
            WHEN r.data_type = 'ARRAY<STRING>' and column_name = 'comments' and table_name in ('applications_all_versions_sambq_p','applications_sambq_p') THEN TRUE
            WHEN r.field_path LIKE '%.%' THEN TRUE
            ELSE FALSE
        END AS is_nested,   
        COUNT(p.display_name) over (partition by r.column_name) as valid_nested_field, 
        -- Extract the nested field after the dot (for struct or array fields)
        CASE 
            WHEN r.field_path LIKE '%.%' THEN SUBSTR(r.field_path, STRPOS(r.field_path, '.') + 1)
            ELSE NULL
        END AS nested_field,
        p.*
    FROM table_columns r
    LEFT JOIN policy_tags_pii p 
        ON LOWER(REPLACE(
            CASE 
                WHEN r.field_path LIKE '%.%' THEN SUBSTR(r.field_path, STRPOS(r.field_path, '.') + 1)
                ELSE r.field_path
            END, "_", ""
        )) = LOWER(REPLACE(p.display_name, "_", ""))
        --edge cases where these columns have encrypted values already
    WHERE NOT (r.table_name = 'people_adhis_r' AND r.column_name IN ('national_id','contact_id'))
    AND NOT (r.table_name = 'archived_ssn' AND r.column_name = 'contact_id')
    AND NOT (r.table_name IN ('applications_sambq_p','applications_all_versions_sambq_p') AND r.column_name = 'text')
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
      OR "yvsotu" IN UNNEST(ARRAY_AGG(normalized_column))
      OR "nationalidsensitive" IN UNNEST(ARRAY_AGG(normalized_column))
      OR (table_name = 'applications_customers_sambq_p' AND "idnumber" IN UNNEST(ARRAY_AGG(normalized_column))),
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
        IN ('ssn', 'ssnid', 'nationalid', 'customerssn', 'sotu', 'yvsotu','nationalidsensitive') 
        THEN  join_key
     -- Exceptional case
      WHEN table_name = "people_adhis_r" 
        THEN "national_id_sensitive" 
      WHEN table_name = "applications_customers_sambq_p" 
        THEN "idNumber" 
    END AS j_key
  FROM join_keys, UNNEST(join_keys.join_keys) AS join_key
),

non_nested_field_encryption AS (
    -- Encryption logic for non-nested fields
    SELECT
        t1.table_schema,
        t1.table_name,
        t2.column_name,
        CONCAT(
            STRING_AGG(
                DISTINCT 
                CASE 
                    -- If sensitive field exists (i.e. t2.display_name is not null), apply encryption
                    WHEN t2.display_name IS NOT NULL THEN
                        CONCAT(
                          "CASE WHEN raw.", column_name, " IS NOT NULL ",  
                          CASE  
                              WHEN data_type NOT IN ('INT', 'INTEGER', 'INT64', 'FLOAT64', 'FLOAT') 
                              THEN " AND raw." || column_name || " <> ''"  
                              WHEN data_type IN ('INT', 'INTEGER', 'INT64', 'FLOAT64', 'FLOAT') 
                              THEN " AND raw." || column_name || " <> 0"  
                              ELSE ""  
                          END,  
                          " AND VAULT.uuid IS NOT NULL THEN ",  
                          "TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, CAST(raw.", column_name, " AS STRING), VAULT.uuid)) ",  
                          "ELSE CAST(raw.", column_name, " AS STRING) END AS ", column_name
                      )
                END
            )
        ) AS encrypted_fields
    FROM unnested_join_keys AS t1
    INNER JOIN sensitive_fields AS t2
        ON t1.sensitive_field = t2.field_path and t1.table_name = t2.table_name
    WHERE is_nested = FALSE 
    GROUP BY t1.table_schema, t1.table_name,t2.column_name
),

nested_field_encryption AS (
    -- Encryption logic for nested fields and only for sambla legacy
    SELECT
        t1.table_schema,
        t1.table_name,
        t2.column_name,
        -- [TODO] remove struct for array<string>
        CONCAT(
            "ARRAY(SELECT STRUCT(",
            STRING_AGG(
                DISTINCT 
                CASE 
                    WHEN t2.display_name IS NOT NULL THEN
                    CASE WHEN data_type = 'ARRAY<STRING>' THEN 
                          CONCAT(
                              "CASE WHEN f_", t2.column_name, " IS NOT NULL AND f_", t2.column_name, " <> '' ",
                              "AND VAULT.uuid IS NOT NULL THEN TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, CAST(f_", t2.column_name, " AS STRING), VAULT.uuid)) ",
                              "ELSE CAST(f_", t2.column_name, " AS STRING) END "
                          )
                          ELSE
                          -- Apply encryption logic for sensitive fields (based on display_name)
                          CONCAT(
                              "CASE WHEN f_", t2.column_name, ".", t2.nested_field, " IS NOT NULL ",
                              CASE  
                                  WHEN data_type IN ('INT', 'INTEGER', 'INT64', 'FLOAT64', 'FLOAT') 
                                  THEN " AND f_" || t2.column_name || "." || t2.nested_field || " <> 0"  
                                  WHEN data_type NOT IN ('INT', 'INTEGER', 'INT64', 'FLOAT64', 'FLOAT') 
                                  THEN " AND f_" || t2.column_name || "." || t2.nested_field || " <> ''"  
                                  ELSE ""  
                              END,  
                              " AND VAULT.uuid IS NOT NULL THEN ",  
                              "TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, CAST(f_", t2.column_name, ".", t2.nested_field, " AS STRING), VAULT.uuid)) ",  
                              "ELSE CAST(f_", t2.column_name, ".", t2.nested_field, " AS STRING) END AS ", t2.nested_field)
                              END                           
                      ELSE
                          -- If no encryption needed, return the field as is
                          CONCAT("f_", t2.column_name, ".", t2.nested_field)
                  END,
                  ", "
              ),
            ") FROM UNNEST(", t2.column_name, ") AS f_", t2.column_name ,") AS ", t2.column_name
        ) AS encrypted_fields
    FROM unnested_join_keys AS t1
    INNER JOIN sensitive_fields AS t2
       ON t1.sensitive_field = t2.column_name and t1.table_name = t2.table_name
    WHERE t2.is_nested = TRUE and valid_nested_field > 1 AND t1.table_schema = "sambla_legacy_integration_legacy"
    GROUP BY t1.table_schema, t1.table_name,t1.sensitive_field, t2.column_name
),

all_fields_encryption as (
    SELECT
    *
    FROM non_nested_field_encryption 
    union all
    SELECT
    *
    FROM nested_field_encryption
),

market_legacystack_mapping AS (
    SELECT 
        t1.table_schema,
        t1.table_name,
        t2.is_table_contains_ssn,
        t2.sensitive_field,
        t2.j_key,
        t1.encrypted_fields,
        t1.column_name,
        CASE 
            WHEN t1.table_schema IN ('advisa_history_integration_legacy', 'maxwell_integration_legacy') THEN 'SE'
            WHEN t1.table_schema IN ('rahalaitos_integration_legacy', 'lvs_integration_legacy') THEN 'FI'
            ELSE 'OTHER MARKETS'
        END AS market_identifier
    FROM 
        all_fields_encryption t1
    INNER JOIN unnested_join_keys t2 
    ON t1.table_name = t2.table_name AND t1.table_schema = t2.table_schema and t2.sensitive_field = t1.column_name
),

final AS (
  -- Construct the final query for each table
  SELECT
    mlm.table_schema,
    mlm.table_name,
    mlm.is_table_contains_ssn,
    mlm.market_identifier,
    CASE 
      WHEN mlm.is_table_contains_ssn AND mlm.table_schema != 'salus_integration_legacy' AND  mlm.table_name != 'credit_remarks_lvs_p'
      THEN CONCAT(
        'WITH data_with_ssn_rules AS (',
        'SELECT ',
        '*, ',
        -- Market-specific logic for SSN extraction
        CASE 
          WHEN mlm.market_identifier = 'OTHER MARKETS' THEN 
          CONCAT(
            'CASE ',
            'WHEN ', CASE WHEN mlm.table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p') THEN 'market' WHEN mlm.table_name IN ('applications_customers_sambq_p') THEN 'coalesce(market, citizenship)' ELSE 'country_code' END ,'= "SE" THEN LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING), "[^0-9]", ""), 12) ',
            'WHEN ', CASE WHEN mlm.table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p') THEN 'market' WHEN mlm.table_name IN ('applications_customers_sambq_p') THEN 'coalesce(market, citizenship)' ELSE 'country_code' END ,'= "NO" THEN LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING), "[^0-9]", ""), 11) ',
            'WHEN ', CASE WHEN mlm.table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p') THEN 'market' WHEN mlm.table_name IN ('applications_customers_sambq_p') THEN 'coalesce(market, citizenship)' ELSE 'country_code' END ,'= "DK" THEN LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING), "[^0-9]", ""), 10) ',
            'WHEN ', CASE WHEN mlm.table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p') THEN 'market' WHEN mlm.table_name IN ('applications_customers_sambq_p') THEN 'coalesce(market, citizenship)' ELSE 'country_code' END ,'= "FI" THEN LEFT(REGEXP_REPLACE(UPPER(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING)), "[^0-9-+A-Z]", ""), 11) ' ,
            'END AS ssn_clean'
          ) 
          WHEN mlm.market_identifier = 'SE' THEN 
            CONCAT(
              'LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING), "[^0-9]", ""), 12) AS ssn_clean'
            )
          WHEN mlm.market_identifier = 'FI' THEN 
            CONCAT(
              'LEFT(REGEXP_REPLACE(UPPER(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING)), "[^0-9-+A-Z]", ""), 11) AS ssn_clean'
            )
        END
        ,
        ' FROM', CASE WHEN mlm.table_schema = 'salus_group_integration' THEN '`{{ exposure_project }}.' ELSE '`{{ raw_layer_project }}.' END, mlm.table_schema, '.', mlm.table_name, '` raw) ',
        
        'SELECT ',
        STRING_AGG(DISTINCT mlm.encrypted_fields, ", "),
        ', raw.* EXCEPT(',
        STRING_AGG(
            DISTINCT CASE 
              WHEN sf.display_name IS NOT NULL THEN sf.column_name
              ELSE NULL 
            END, ', ' 
          ),
        '),',
        'CASE ',
          'WHEN (CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING) IS NOT NULL AND CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING) <> "" AND CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING) <> "0" AND VAULT.uuid IS NOT NULL) OR LOWER(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING)) =  "anonymized" THEN TRUE ',
          'ELSE FALSE ',
        'END AS is_anonymised ',
        'FROM `data_with_ssn_rules` raw ',
        'LEFT JOIN `{{compliance_project}}.compilance_database.{{gdpr_vault_table}}` VAULT ',
                'ON CAST(raw.ssn_clean AS STRING) = VAULT.ssn'
              )
      ELSE CONCAT(
        'SELECT *, False AS is_anonymised FROM ', CASE WHEN mlm.table_schema = 'salus_group_integration' THEN '`{{ exposure_project }}.' ELSE '`{{ raw_layer_project }}.' END,
        mlm.table_schema,
        '.',
        mlm.table_name,
        '`'
      )     
    END AS final_encrypted_columns
FROM market_legacystack_mapping mlm
left join sensitive_fields sf
on mlm.table_schema=sf.table_schema and mlm.table_name=sf.table_name
GROUP BY table_schema, table_name, is_table_contains_ssn, market_identifier
)
SELECT distinct * FROM final 
WHERE final_encrypted_columns IS NOT NULL
AND table_schema IN ("salus_integration_legacy","salus_group_integration")
