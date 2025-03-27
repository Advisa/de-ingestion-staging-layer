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
    WHERE TABLE_NAME LIKE '%sambq_p' and TABLE_NAME != 'applications_all_versions_history_sambq_p'
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
  WHERE t2.display_name IN ('employer', 'email', 'phone', 'ssn', 'first_name', 'last_name', 'bank_account_number', 'address', 'post_code','data','business_organization_number','attributes_raw_json', 'PII')
  
),
policy_tags_pii_parent_tags AS (
  SELECT 
    DISTINCT t2.display_name
  FROM 
    policy_tags_all t1 
  INNER JOIN 
    policy_tags_all t2 
  ON t1.parent_policy_tag_id = t2.policy_tag_id
  WHERE t2.display_name IN ('email', 'phone', 'ssn', 'first_name', 'last_name', 'bank_account_number','address', 'post_code','data','business_organization_number','attributes_raw_json') OR t1.display_name = "last_application_employer_name"
  
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
            WHEN r.data_type = 'ARRAY<STRING>' and column_name = 'comments' and table_name in ('applications_all_versions_sambq_p','applications_sambq_p', 'applications_bids_sambq_p') THEN TRUE
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
    AND NOT (r.table_name IN ('applications_sambq_p','applications_all_versions_sambq_p', 'applications_bids_sambq_p') AND r.column_name = 'text')
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
      OR (table_name = 'insurance_person_identify_raha_r' AND "identify" IN UNNEST(ARRAY_AGG(normalized_column)))
      OR (table_name = 'applications_customers_sambq_p' AND "idnumber" IN UNNEST(ARRAY_AGG(normalized_column)))
      OR (table_name = 'applications_credit_reports_sambq_p' AND "idnumber" IN UNNEST(ARRAY_AGG(normalized_column))),
      TRUE, FALSE
    ) AS is_table_contains_ssn,
    IF (
      "email" IN UNNEST(ARRAY_AGG(normalized_column)) 
      OR "emailaddress" IN UNNEST(ARRAY_AGG(normalized_column))
      OR (table_name = 'unsubscriptions_sambq_p' AND "identifier" IN UNNEST(ARRAY_AGG(normalized_column)))
      , TRUE, FALSE
    ) AS is_table_contains_email,
    IF (
      "phone" IN UNNEST(ARRAY_AGG(normalized_column)) 
      OR "mobilephone" IN UNNEST(ARRAY_AGG(normalized_column))
      OR "phonenumber" IN UNNEST(ARRAY_AGG(normalized_column))
      OR "puhelin" IN UNNEST(ARRAY_AGG(normalized_column))
      , TRUE, FALSE
    ) AS is_table_contains_mobile,
    IF (
      "applicationid" IN UNNEST(ARRAY_AGG(normalized_column)) 
      OR "loanapplicationoid" IN UNNEST(ARRAY_AGG(normalized_column))
      OR "loanapplicationid" IN UNNEST(ARRAY_AGG(normalized_column))
      , TRUE, FALSE
    ) AS is_table_contains_app_id,
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
    is_table_contains_email,
    is_table_contains_mobile,
    is_table_contains_app_id,
    CASE 
     -- Exceptional cases
      WHEN table_name = "people_adhis_r" 
        THEN "national_id_sensitive" 
      WHEN table_name in ("applications_customers_sambq_p", "applications_credit_reports_sambq_p")
        THEN "idNumber" 
     -- Normal cases
      WHEN table_name != "people_adhis_r" 
        AND LOWER(REPLACE(join_key, "_", "")) 
        IN ('ssn', 'ssnid', 'nationalid', 'customerssn', 'sotu', 'yvsotu','nationalidsensitive', 'identify') 
        THEN join_key
    END AS j_key,
    CASE 
      WHEN LOWER(REPLACE(join_key, "_", "")) 
        IN ('email', 'emailaddress', 'identifier') 
        THEN join_key
    END AS j_key_email,
    CASE 
      WHEN LOWER(REPLACE(join_key, "_", "")) 
        IN ('phone', 'mobilephone', 'puhelin', 'phonenumber') 
        THEN join_key
    END AS j_key_mobile,
    CASE 
      WHEN LOWER(REPLACE(join_key, "_", "")) 
        IN ('applicationid', 'loanapplicationoid', 'loanapplicationid') 
        THEN join_key
    END AS j_key_app_id,
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
                          CASE WHEN is_table_contains_email AND is_table_contains_mobile AND not is_table_contains_ssn THEN CONCAT(
                          " AND COALESCE(vault_email.uuid, vault_mobile.uuid) IS NOT NULL THEN ",  
                          "TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(COALESCE(vault_email.aead_key, vault_mobile.aead_key), CAST(raw.", column_name, " AS STRING), COALESCE(vault_email.uuid, vault_mobile.uuid))) ",  
                          "ELSE CAST(raw.", column_name, " AS STRING) END AS ", column_name
                          )
                          ELSE CONCAT(
                          " AND VAULT.uuid IS NOT NULL THEN ",  
                          "TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, CAST(raw.", column_name, " AS STRING), VAULT.uuid)) ",  
                          "ELSE CAST(raw.", column_name, " AS STRING) END AS ", column_name
                          )
                          END
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
        t2.is_table_contains_email,
        t2.is_table_contains_mobile,
        t2.is_table_contains_app_id,
        t2.sensitive_field,
        t2.j_key,
        t2.j_key_email,
        t2.j_key_mobile,
        t2.j_key_app_id,
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

exclude_tables_list AS (
  select ["users_sambq_p", "providers_lvs_p", "credit_remarks_lvs_p", "crm_user_raha_r", "applications_all_versions_sambq_p", "applications_allpaidoutbysambla_sambq_p", 
  "applications_scheduledcalls_sambq_p", "applications_utmhistory_sambq_p", "invites_adhis_r", "loan_application_drafts_sgmw_p", "cookie_mappings_sgmw_p",
  "loan_application_versions_sgmw_p", "invites_sgmw_p"] as exclude_tables
),

final AS (
  -- Construct the final query for each table
  SELECT
    mlm.table_schema,
    mlm.table_name,
    mlm.is_table_contains_ssn,
    mlm.is_table_contains_email,
    mlm.is_table_contains_mobile,
    mlm.is_table_contains_app_id,
    mlm.market_identifier,
    CASE 
--------------- SSN join ---------------
      WHEN mlm.is_table_contains_ssn AND mlm.table_schema != 'salus_integration_legacy' AND mlm.table_name NOT IN (SELECT table FROM exclude_tables_list, UNNEST(exclude_tables) AS table)
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
,'applications_sambq_p', 'applications_credit_reports_sambq_p') THEN 'market' WHEN mlm.table_name IN ('applications_customers_sambq_p') THEN 'coalesce(market, citizenship)' ELSE 'country_code' END ,'= "SE" THEN LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING), "[^0-9]", ""), 12) ',
            'WHEN ', CASE WHEN mlm.table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p', 'applications_credit_reports_sambq_p') THEN 'market' WHEN mlm.table_name IN ('applications_customers_sambq_p') THEN 'coalesce(market, citizenship)' ELSE 'country_code' END ,'= "NO" THEN LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING), "[^0-9]", ""), 11) ',
            'WHEN ', CASE WHEN mlm.table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p', 'applications_credit_reports_sambq_p') THEN 'market' WHEN mlm.table_name IN ('applications_customers_sambq_p') THEN 'coalesce(market, citizenship)' ELSE 'country_code' END ,'= "DK" THEN LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING), "[^0-9]", ""), 10) ',
            'WHEN ', CASE WHEN mlm.table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p', 'applications_credit_reports_sambq_p') THEN 'market' WHEN mlm.table_name IN ('applications_customers_sambq_p') THEN 'coalesce(market, citizenship)' ELSE 'country_code' END ,'= "FI" THEN LEFT(REGEXP_REPLACE(UPPER(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING)), "[^0-9-+A-Z]", ""), 11) ' ,
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
 ------ APPLICATION ID join -----------
      WHEN mlm.is_table_contains_app_id AND mlm.table_schema != 'salus_integration_legacy' AND mlm.table_name NOT IN (SELECT table FROM exclude_tables_list, UNNEST(exclude_tables) AS table) THEN 
        CONCAT(
          'WITH raw_data_with_stack AS (',
            'SELECT *, ',
            CASE WHEN mlm.table_schema = 'salus_group_integration' THEN '"salus" as stack'
            WHEN mlm.table_schema = 'sambla_legacy_integration_legacy' THEN '"sambla" as stack'
            WHEN mlm.table_schema = 'lvs_integration_legacy' THEN '"LVS" as stack'
            WHEN mlm.table_schema = 'rahalaitos_integration_legacy' THEN '"rahalaitos" as stack'
            WHEN mlm.table_schema = 'advisa_history_integration_legacy' THEN '"advisa" as stack' END,
            ' FROM', CASE WHEN mlm.table_schema = 'salus_group_integration' THEN '`data-domain-data-warehouse.' ELSE '`sambla-data-staging-compliance.' END, mlm.table_schema, '.', mlm.table_name, '` raw), ',
            ---
            'stack_vault as ( '
                'select uuid, aead_key, loan_application_oid, stack from `{{compliance_project}}.compilance_database.{{gdpr_vault_table}}`, unnest(history) as history ',
                'where history.stack  = ',
            CASE WHEN mlm.table_schema = 'salus_group_integration' THEN '"salus"'
            WHEN mlm.table_schema = 'sambla_legacy_integration_legacy' THEN '"sambla"'
            WHEN mlm.table_schema = 'lvs_integration_legacy' THEN '"LVS"'
            WHEN mlm.table_schema = 'rahalaitos_integration_legacy' THEN '"rahalaitos"'
            WHEN mlm.table_schema = 'advisa_history_integration_legacy' THEN '"advisa"' END,
            ' )',
            ---
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
                'WHEN (raw.', STRING_AGG(DISTINCT mlm.j_key_app_id, ', '), ' IS NOT NULL AND raw.', STRING_AGG(DISTINCT mlm.j_key_app_id, ', '),' <> "" AND raw.', STRING_AGG(DISTINCT mlm.j_key_app_id, ', '), ' <> "0" AND VAULT.uuid IS NOT NULL) OR LOWER(raw.', STRING_AGG(DISTINCT mlm.j_key_app_id, ', '), ') =  "anonymized" THEN TRUE ',
                'ELSE FALSE ',
              'END AS is_anonymised ',
              ' FROM raw_data_with_stack raw ',
              'LEFT JOIN stack_vault vault ',
                  'ON raw.', STRING_AGG(DISTINCT mlm.j_key_app_id, ', '), ' = vault.loan_application_oid '
                  'AND raw.stack = vault.stack'
                    )
 ------ double join ----------- 
      WHEN mlm.is_table_contains_mobile AND mlm.is_table_contains_email AND not mlm.is_table_contains_ssn AND mlm.table_schema != 'salus_integration_legacy' AND mlm.table_name NOT IN (SELECT table FROM exclude_tables_list, UNNEST(exclude_tables) AS table) THEN 
        CONCAT(
          'WITH vault_mobiles_flattened_intial AS (',
            'SELECT history.mobile_phone AS mobile,',
              'history.stack,',
              'history.created_at,',
              'vault.uuid,',
              'vault.aead_key ',
              'FROM `{{compliance_project}}.compilance_database.{{gdpr_vault_table}}` AS vault '
              'CROSS JOIN UNNEST(vault.history) AS history '
              'WHERE history.mobile_phone IS NOT NULL AND history.mobile_phone != ""), '
          'vault_mobiles_flattened AS ( '
          'SELECT mobile, stack, uuid, aead_key FROM vault_mobiles_flattened_intial '
          'QUALIFY ROW_NUMBER() OVER (PARTITION BY mobile ORDER BY created_at DESC) = 1 ), '
          'vault_emails_flattened_initial AS (',
            'SELECT history.email AS email,',
              'history.stack,',
              'history.created_at,',
              'vault.uuid,',
              'vault.aead_key ',
              'FROM `{{compliance_project}}.compilance_database.{{gdpr_vault_table}}` AS vault '
              'CROSS JOIN UNNEST(vault.history) AS history '
              'WHERE history.email IS NOT NULL AND history.email != ""), '
          'vault_emails_flattened AS ( '
          'SELECT email, stack, uuid, aead_key FROM vault_emails_flattened_initial '
          'QUALIFY ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at DESC) = 1 ), '
  -- Mobile clean CTE
      'mobile_cleaned AS ( ',
        'SELECT *, ',
        'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = " +" THEN SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) ',
        'WHEN raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ' NOT LIKE "%+%" THEN ',
CASE
-- For the FI market
    WHEN mlm.market_identifier = "FI" THEN 
        CONCAT(
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "358" THEN ',
                'CASE WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) BETWEEN 9 AND 15 THEN CONCAT("+", REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) ELSE NULL END ',

            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN ',
            'CASE WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) BETWEEN 7 AND 13 THEN CONCAT("+358", SUBSTRING(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", ""), 2)) ELSE NULL END ',

            'WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) BETWEEN 6 AND 12 THEN "+358" || REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "") ELSE NULL END '
        )

-- For the SE market
    WHEN mlm.market_identifier = "SE" THEN 
        CONCAT(
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = "46" THEN CONCAT("+46", raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ')',
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "046" THEN CONCAT("+46", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 4) = "0046" THEN CONCAT("+46", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN CONCAT("+46", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'ELSE "+46" || ', STRING_AGG(DISTINCT mlm.j_key_mobile, ', ') ,' END '
        )

-- -- For the DK market
    WHEN mlm.market_identifier = "DK" THEN 
        CONCAT(
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = "45" THEN CONCAT("+45", raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ')',
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "045" THEN CONCAT("+45", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 4) = "0045" THEN CONCAT("+45", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN CONCAT("+45", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'ELSE "+45" || ', STRING_AGG(DISTINCT mlm.j_key_mobile, ', ') ,' END '
        )

-- -- For the NO market and other markets
    WHEN mlm.market_identifier = "NO" THEN 
        CONCAT(
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = "47" THEN ',
                'CASE WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 10 THEN CONCAT("+", REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) ELSE NULL END ',
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "047" AND LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 11 THEN CONCAT("+", REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", 2)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 4) = "0047" AND LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 12 THEN CONCAT("+", REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", 3)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN CASE WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 9 THEN CONCAT("+47", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) ELSE NULL END '
            'WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 8 THEN "+47" || REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "") END '
        )
END,

      'ELSE CASE WHEN LENGTH(REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', " ", "")) > 6 AND LENGTH(REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', " ", "")) < 18 THEN REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', " ", "") END END AS mobile_clean ',
  -- Mobile clean CTE ends
  CONCAT(
    ' FROM', CASE WHEN mlm.table_schema = 'salus_group_integration' THEN '`data-domain-data-warehouse.' ELSE '`sambla-data-staging-compliance.' END, mlm.table_schema, '.', mlm.table_name, '` raw) ',
    'SELECT ',
      STRING_AGG(DISTINCT mlm.encrypted_fields, ", "), ', ',
      'raw.* EXCEPT( ',
        STRING_AGG(
            DISTINCT CASE 
              WHEN sf.display_name IS NOT NULL THEN sf.column_name
              ELSE NULL 
            END, ', ' 
        ), 
      '), ',
      'CASE ',
        'WHEN ((raw.mobile_clean IS NOT NULL AND raw.mobile_clean <> "" AND raw.mobile_clean <> "0" AND COALESCE(vault_email.uuid, vault_mobile.uuid) IS NOT NULL) OR LOWER(raw.mobile_clean) = "anonymized" OR LOWER(raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ') =  "anonymized" OR (raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' IS NOT NULL AND raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' <> "0" AND raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' <> "" AND COALESCE(vault_email.uuid, vault_mobile.uuid) IS NOT NULL)) THEN TRUE ',
        'ELSE FALSE ',
      'END AS is_anonymised ',
      'FROM mobile_cleaned raw ',
              'LEFT JOIN vault_mobiles_flattened vault_mobile ',
                      'ON raw.mobile_clean = vault_mobile.mobile '
              'LEFT JOIN vault_emails_flattened vault_email ',
                      'ON raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' = vault_email.email '
                    )
        )
 ------ EMAIL join -----------
      WHEN mlm.is_table_contains_email AND mlm.table_schema != 'salus_integration_legacy' AND mlm.table_name NOT IN (SELECT table FROM exclude_tables_list, UNNEST(exclude_tables) AS table) THEN 
        CONCAT(
          'WITH vault_emails_flattened_initial AS (',
            'SELECT history.email AS email,',
              'history.stack,',
              'history.created_at,',
              'vault.uuid,',
              'vault.aead_key ',
              'FROM `{{compliance_project}}.compilance_database.{{gdpr_vault_table}}` AS vault '
              'CROSS JOIN UNNEST(vault.history) AS history '
              'WHERE history.email IS NOT NULL AND history.email != ""), '
          'vault_emails_flattened AS ( '
          'SELECT email, stack, uuid, aead_key FROM vault_emails_flattened_initial '
          'QUALIFY ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at DESC) = 1 ) '
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
                'WHEN (raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' IS NOT NULL AND raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '),' <> "" AND raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' <> "0" AND VAULT.uuid IS NOT NULL) OR LOWER(raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ') =  "anonymized" THEN TRUE ',
                'ELSE FALSE ',
              'END AS is_anonymised ',
              ' FROM', CASE WHEN mlm.table_schema = 'salus_group_integration' THEN '`data-domain-data-warehouse.' ELSE '`sambla-data-staging-compliance.' END, mlm.table_schema, '.', mlm.table_name, '` raw ',
              'LEFT JOIN vault_emails_flattened vault ',
                      'ON raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' = vault.email'
                    )
 ------ mobile join -----------
WHEN mlm.is_table_contains_mobile AND mlm.table_schema != 'salus_integration_legacy' AND mlm.table_name NOT IN (SELECT table FROM exclude_tables_list, UNNEST(exclude_tables) AS table) THEN 
  CONCAT(
          'WITH vault_mobiles_flattened_initial AS (',
            'SELECT history.mobile_phone AS mobile,',
              'history.stack,',
              'history.created_at,',
              'vault.uuid,',
              'vault.aead_key ',
              'FROM `{{compliance_project}}.compilance_database.{{gdpr_vault_table}}` AS vault '
              'CROSS JOIN UNNEST(vault.history) AS history '
              'WHERE history.mobile_phone IS NOT NULL AND history.mobile_phone != ""), '
          'vault_mobiles_flattened AS ( '
          'SELECT mobile, stack, uuid, aead_key FROM vault_mobiles_flattened_initial '
          'QUALIFY ROW_NUMBER() OVER (PARTITION BY mobile ORDER BY created_at DESC) = 1 ), '
  -- Mobile clean CTE
      'mobile_cleaned AS ( ',
        'SELECT *, ',
        'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = " +" THEN SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) ',
        'WHEN raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ' NOT LIKE "%+%" THEN ',
CASE
-- For the FI market
    WHEN mlm.market_identifier = "FI" THEN 
        CONCAT(
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "358" THEN ',
                'CASE WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) BETWEEN 9 AND 15 THEN CONCAT("+", REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) ELSE NULL END ',

            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN ',
            'CASE WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) BETWEEN 7 AND 13 THEN CONCAT("+358", SUBSTRING(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", ""), 2)) ELSE NULL END ',

            'WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) BETWEEN 6 AND 12 THEN "+358" || REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "") ELSE NULL END '
        )

-- For the SE market
    WHEN mlm.market_identifier = "SE" THEN 
        CONCAT(
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = "46" THEN CONCAT("+46", raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ')',
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "046" THEN CONCAT("+46", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 4) = "0046" THEN CONCAT("+46", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN CONCAT("+46", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'ELSE "+46" || ', STRING_AGG(DISTINCT mlm.j_key_mobile, ', ') ,' END '
        )

-- -- For the DK market
    WHEN mlm.market_identifier = "DK" THEN 
        CONCAT(
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = "45" THEN CONCAT("+45", raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ')',
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "045" THEN CONCAT("+45", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 4) = "0045" THEN CONCAT("+45", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN CONCAT("+45", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'ELSE "+45" || ', STRING_AGG(DISTINCT mlm.j_key_mobile, ', ') ,' END '
        )

-- -- For the NO market and other markets
    WHEN mlm.market_identifier = "NO" THEN 
        CONCAT(
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = "47" THEN ',
                'CASE WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 10 THEN CONCAT("+", REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) ELSE NULL END ',
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "047" AND LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 11 THEN CONCAT("+", REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", 2)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 4) = "0047" AND LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 12 THEN CONCAT("+", REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", 3)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN CASE WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 9 THEN CONCAT("+47", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) ELSE NULL END '
            'WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 8 THEN "+47" || REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "") END '
        )
END,
      'ELSE CASE WHEN LENGTH(REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', " ", "")) > 6 AND LENGTH(REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', " ", "")) < 18 THEN REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', " ", "") END END AS mobile_clean ',
  -- Mobile clean CTE ends
  CONCAT(
    ' FROM', CASE WHEN mlm.table_schema = 'salus_group_integration' THEN '`data-domain-data-warehouse.' ELSE '`sambla-data-staging-compliance.' END, mlm.table_schema, '.', mlm.table_name, '` raw) ',
    'SELECT ',
      STRING_AGG(DISTINCT mlm.encrypted_fields, ", "), ', ',
      'raw.* EXCEPT( ',
        STRING_AGG(
            DISTINCT CASE 
              WHEN sf.display_name IS NOT NULL THEN sf.column_name
              ELSE NULL 
            END, ', ' 
        ), 
      '), ',
      'CASE ',
        'WHEN (raw.mobile_clean IS NOT NULL AND raw.mobile_clean <> "" AND raw.mobile_clean <> "0" AND vault.uuid IS NOT NULL) OR LOWER(raw.mobile_clean) = "anonymized" THEN TRUE ',
        'ELSE FALSE ',
      'END AS is_anonymised ',
      'FROM mobile_cleaned raw ',
      'LEFT JOIN vault_mobiles_flattened vault ',
        'ON raw.mobile_clean = vault.mobile '
)
  )
------ NO join ---------
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
GROUP BY table_schema, table_name, is_table_contains_ssn, is_table_contains_email, is_table_contains_mobile, is_table_contains_app_id, market_identifier
)
SELECT distinct * FROM final 
WHERE final_encrypted_columns IS NOT NULL
--AND table_schema IN ('sambla_legacy_integration_legacy')
--("salus_group_integration","salus_integration_legacy")
