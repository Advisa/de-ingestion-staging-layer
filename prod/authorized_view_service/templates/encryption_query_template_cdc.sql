WITH table_columns AS (
    -- Replace this with your actual query for table columns
    SELECT * FROM `{{exposure_project}}.sambla_group_data_stream`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
    WHERE table_name IN (
            {% for table in se_table_names %}
                "{{ table }}"{% if not loop.last %}, {% endif %}
            {% endfor %})
    UNION ALL
    SELECT * FROM `{{exposure_project}}.sambla_group_data_stream_fi`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
    WHERE table_name IN (
            {% for table in fi_table_names %}
                "{{ table }}"{% if not loop.last %}, {% endif %}
            {% endfor %})
    UNION ALL
    SELECT * FROM `{{exposure_project}}.sambla_group_data_stream_no`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
    WHERE table_name IN (
            {% for table in no_table_names %}
                "{{ table }}"{% if not loop.last %}, {% endif %}
            {% endfor %})
    UNION ALL
    SELECT * FROM `{{exposure_project}}.helios_staging`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
    WHERE table_name LIKE "%_ppi_p" AND table_name NOT IN ('insurances_ppi_p')
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
    -- Combine both the logic of parent and sensitive fields into a single CTE
    SELECT 
        r.table_schema,
        r.table_name,
        r.column_name,
        r.field_path,
        r.data_type,
        p.display_name,
        CASE 
            WHEN r.field_path LIKE '%.%' THEN SPLIT(r.field_path, '.')[OFFSET(0)]  -- Extract parent field
            ELSE NULL 
        END AS parent_field,
        LOWER(REPLACE(
            CASE 
                WHEN r.field_path LIKE '%.%' THEN SUBSTR(r.field_path, STRPOS(r.field_path, '.') + 1)  -- Extract nested field
                ELSE r.field_path
            END, "_", ""
        )) AS normalized_column,
        CASE 
            WHEN r.data_type = 'ARRAY<STRING>' and r.column_name = 'comments' 
                AND r.table_name IN ('applications_all_versions_sambq_p', 'applications_sambq_p', 'applications_bids_sambq_p') THEN TRUE
            WHEN r.field_path LIKE '%.%' THEN TRUE
            ELSE FALSE
        END AS is_nested,
        COUNT(p.display_name) OVER (PARTITION BY r.column_name) AS valid_nested_field,
        CASE 
            WHEN r.field_path LIKE '%.%' THEN SUBSTR(parent_cp.data_type, 1, 6)
        ELSE 
        r.data_type
        END AS parent_datatype,
        CASE 
            WHEN r.field_path LIKE '%.%' THEN SUBSTR(r.field_path, STRPOS(r.field_path, '.') + 1)
            ELSE NULL
        END AS nested_field       
    FROM 
        table_columns r
    LEFT JOIN 
        policy_tags_pii p 
        ON LOWER(REPLACE(
            CASE 
                WHEN r.field_path LIKE '%.%' THEN SUBSTR(r.field_path, STRPOS(r.field_path, '.') + 1)
                ELSE r.field_path
            END, "_", ""
        )) = LOWER(REPLACE(p.display_name, "_", ""))
    
    LEFT JOIN 
        table_columns parent_cp 
        ON r.field_path LIKE CONCAT(parent_cp.field_path, '.%')
    WHERE NOT (r.table_name = 'people_adhis_r' AND r.column_name IN ('national_id', 'contact_id'))
    AND NOT (r.table_name = 'archived_ssn' AND r.column_name = 'contact_id')
    AND NOT (r.table_name IN ('applications_sambq_p', 'applications_all_versions_sambq_p') AND r.column_name = 'text')
),

join_keys AS (
  -- Identify join keys for linking tables to VAULT
  SELECT distinct
    table_schema,
    table_name,
    field_path,
    ARRAY_AGG(field_path) OVER (PARTITION BY table_schema, table_name) AS join_keys,
    -- Create a flag to mark tables that includes ssn
    IF (
      "ssn" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name)) 
      OR "customerssn" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name)) 
      OR "foreignerssn" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name))
      OR "ssnid" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name)) 
      OR "nationalid" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name)) 
      OR "sotu" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name)) 
      OR "yvsotu" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name))
      OR "identify" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name))
      OR "contactdetail" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name))
      OR "nationalidsensitive" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name)),
      TRUE, FALSE
    ) AS is_table_contains_ssn,
    IF (
      "email" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name)) 
      OR "emailaddress" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name))
      OR "contactdetail" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name))
      , TRUE, FALSE
    ) AS is_table_contains_email,
    IF (
      "phone" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name)) 
      OR "contactdetail" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name))
      OR "mobilephone" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name))
      OR "phonenumber" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name))
      OR "puhelin" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name))
      , TRUE, FALSE
    ) AS is_table_contains_mobile,
    IF (
      "applicationid" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name)) 
      OR "loanapplicationoid" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name))
      OR "loanapplicationid" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name))
      , TRUE, FALSE
    ) AS is_table_contains_app_id,
  FROM sensitive_fields 
),

unnested_join_keys AS (
  -- Extract sensitive fields and parent fields
  SELECT distinct
    table_schema,
    table_name,
    join_key AS sensitive_field,
    field_path,
    is_table_contains_ssn,
    is_table_contains_email,
    is_table_contains_mobile,
    is_table_contains_app_id,
    CASE 
      WHEN table_name != "people_adhis_r" 
        AND LOWER(REPLACE(
            CASE 
                WHEN join_key LIKE '%.%' THEN SUBSTR(join_key, STRPOS(join_key, '.') + 1)
                ELSE join_key
            END, "_", ""
        )) 
        IN ('ssn', 'ssnid', 'nationalid', 'customerssn', 'sotu', 'yvsotu','nationalidsensitive','identify', 'contactdetail') 
        THEN  join_key
    END AS j_key,
    CASE 
      WHEN table_name != "providers_lvs_p" 
        AND LOWER(REPLACE(
            CASE 
                WHEN join_key LIKE '%.%' THEN SUBSTR(join_key, STRPOS(join_key, '.') + 1)
                ELSE join_key
            END, "_", ""
        )) 
        IN ('email', 'emailaddress', 'contactdetail') 
        THEN  join_key 
    END AS j_key_email,
    CASE 
      WHEN 1 != 0
        AND LOWER(REPLACE(
            CASE 
                WHEN join_key LIKE '%.%' THEN SUBSTR(join_key, STRPOS(join_key, '.') + 1)
                ELSE join_key
            END, "_", ""
        )) 
        IN ('phone', 'mobilephone', 'puhelin', 'phone_number', 'contactdetail') 
        THEN  join_key
    END AS j_key_mobile,
    CASE 
      WHEN 1 != 0
        AND LOWER(REPLACE(
            CASE 
                WHEN join_key LIKE '%.%' THEN SUBSTR(join_key, STRPOS(join_key, '.') + 1)
                ELSE join_key
            END, "_", ""
        )) 
        IN ('applicationid', 'loanapplicationoid', 'loanapplicationid') 
        THEN  join_key
    END AS j_key_app_id,
  FROM join_keys, UNNEST(join_keys.join_keys) AS join_key
),

non_nested_field_encryption AS (
    -- Encryption logic for non-nested fields
    SELECT DISTINCT
        t1.table_schema,
        t1.table_name,
        t2.column_name,
        t2.field_path,
        t1.sensitive_field,
        t2.data_type,
        t2.parent_datatype,
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
                          WHEN is_table_contains_email AND is_table_contains_mobile AND is_table_contains_ssn AND t1.table_name = 'marketing_contact_service_contact_blocks_sgds_r' THEN CONCAT(
                          " AND COALESCE(vault.uuid, vault_email.uuid, vault_mobile.uuid) IS NOT NULL THEN ",  
                          "TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(COALESCE(vault.aead_key, vault_email.aead_key, vault_mobile.aead_key), CAST(raw.", column_name, " AS STRING), COALESCE(vault.uuid, vault_email.uuid, vault_mobile.uuid))) ",  
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
         AS encryption_logic
    FROM unnested_join_keys AS t1
    INNER JOIN sensitive_fields AS t2
        ON t1.sensitive_field = t2.field_path and t1.table_name = t2.table_name and t1.table_schema = t2.table_schema
    WHERE is_nested = FALSE
),

nested_field_encryption AS (
    -- Encryption logic for nested fields and only for sambla legacy
    SELECT DISTINCT
        t1.table_schema,
        t1.table_name,
        t2.column_name,
        t2.field_path,
        t1.sensitive_field,
        t2.data_type,
        t2.parent_datatype,
        CASE 
              WHEN t2.display_name IS NOT NULL THEN
                    CASE WHEN parent_datatype LIKE 'STRUCT%' THEN 
                          CONCAT(
                              "CASE WHEN raw.", t2.field_path, " IS NOT NULL ",
                                CASE  
                                WHEN data_type NOT IN ('INT', 'INTEGER', 'INT64', 'FLOAT64', 'FLOAT') 
                                THEN " AND raw." || t2.field_path || " <> ''"  
                                WHEN data_type IN ('INT', 'INTEGER', 'INT64', 'FLOAT64', 'FLOAT') 
                                THEN " AND raw." || t2.field_path || " <> 0"  
                                ELSE ""  
                                END,  
                                  CASE WHEN is_table_contains_email AND is_table_contains_mobile AND not is_table_contains_ssn THEN CONCAT(
                              " AND COALESCE(vault_email.uuid, vault_mobile.uuid) IS NOT NULL THEN TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(COALESCE(vault_email.aead_key, vault_mobile.aead_key), CAST(raw.", t2.field_path, " AS STRING), COALESCE(vault_email.uuid, vault_mobile.uuid))) ",
                              "ELSE CAST(raw.", t2.field_path, " AS STRING) END AS ", t2.nested_field
                          )
                                  WHEN is_table_contains_email AND is_table_contains_mobile AND is_table_contains_ssn AND t1.table_name = 'marketing_contact_service_contact_blocks_sgds_r' THEN CONCAT(
                              " AND COALESCE(vault.uuid, vault_email.uuid, vault_mobile.uuid) IS NOT NULL THEN TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(COALESCE(vault.aead_key, vault_email.aead_key, vault_mobile.aead_key), CAST(raw.", t2.field_path, " AS STRING), COALESCE(vault.uuid, vault_email.uuid, vault_mobile.uuid))) ",
                              "ELSE CAST(raw.", t2.field_path, " AS STRING) END AS ", t2.nested_field
                          )
                                  ELSE CONCAT(
                              " AND VAULT.uuid IS NOT NULL THEN TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, CAST(raw.", t2.field_path, " AS STRING), VAULT.uuid)) ",
                              "ELSE CAST(raw.", t2.field_path, " AS STRING) END AS ", t2.nested_field
                          )
                          END )
                    WHEN data_type = 'ARRAY<STRING>' THEN 
                          CONCAT(
                              "CASE WHEN f_", t2.column_name, " IS NOT NULL ",
                              CASE  
                                WHEN data_type NOT IN ('INT', 'INTEGER', 'INT64', 'FLOAT64', 'FLOAT') 
                                THEN " AND f_" || t2.column_name || " <> ''"  
                                WHEN data_type IN ('INT', 'INTEGER', 'INT64', 'FLOAT64', 'FLOAT') 
                                THEN " AND f_" || t2.column_name || " <> 0"  
                                ELSE ""  
                                END, 
                                  CASE WHEN is_table_contains_email AND is_table_contains_mobile AND not is_table_contains_ssn THEN CONCAT(
                              " AND COALESCE(vault_email.uuid, vault_mobile.uuid) IS NOT NULL THEN TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(COALESCE(vault_email.aead_key, vault_mobile.aead_key), CAST(f_", t2.column_name, " AS STRING), COALESCE(vault_email.uuid, vault_mobile.uuid))) ",
                              "ELSE CAST(f_", t2.column_name, " AS STRING) END AS ", t2.display_name
                          )
                                  WHEN is_table_contains_email AND is_table_contains_mobile AND is_table_contains_ssn AND t1.table_name = 'marketing_contact_service_contact_blocks_sgds_r' THEN CONCAT(
                              " AND COALESCE(vault.uuid, vault_email.uuid, vault_mobile.uuid) IS NOT NULL THEN TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(COALESCE(vault.aead_key, vault_email.aead_key, vault_mobile.aead_key), CAST(raw.", t2.column_name, " AS STRING), COALESCE(vault.uuid, vault_email.uuid, vault_mobile.uuid))) ",
                              "ELSE CAST(raw.", t2.column_name, " AS STRING) END AS ", t2.display_name
                          )
                                  ELSE CONCAT(
                              " AND VAULT.uuid IS NOT NULL THEN TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, CAST(f_", t2.column_name, " AS STRING), VAULT.uuid)) ",
                              "ELSE CAST(f_", t2.column_name, " AS STRING) END AS ", t2.display_name
                          )
                          END )
                    WHEN parent_datatype  LIKE 'ARRAY%' THEN
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
                                  CASE WHEN is_table_contains_email AND is_table_contains_mobile AND not is_table_contains_ssn THEN CONCAT(
                              " AND COALESCE(vault_email.uuid, vault_mobile.uuid) IS NOT NULL THEN ",  
                              "TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(COALESCE(vault_email.aead_key, vault_mobile.aead_key), CAST(f_", t2.column_name, ".", t2.nested_field, " AS STRING), COALESCE(vault_email.uuid, vault_mobile.uuid))) ",  
                              "ELSE CAST(f_", t2.column_name, ".", t2.nested_field, " AS STRING) END AS ", t2.nested_field)
                                  -- other cases not required for now
                                  ELSE CONCAT(
                              " AND VAULT.uuid IS NOT NULL THEN ",  
                              "TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, CAST(f_", t2.column_name, ".", t2.nested_field, " AS STRING), VAULT.uuid)) ",  
                              "ELSE CAST(f_", t2.column_name, ".", t2.nested_field, " AS STRING) END AS ", t2.nested_field)
                          END )
                              END                           
            ELSE
                  CASE WHEN parent_datatype LIKE 'STRUCT%' THEN 
                          -- If no encryption needed, return the field as is
                          CONCAT("raw.", t2.column_name, ".", t2.nested_field)
                  ELSE 
                          CONCAT("f_", t2.column_name, ".", t2.nested_field)
                  END
            END
         AS encryption_logic
    FROM unnested_join_keys AS t1
    INNER JOIN sensitive_fields AS t2
       ON t1.sensitive_field = t2.column_name and t1.table_name = t2.table_name and t1.table_schema = t2.table_schema
    WHERE t2.is_nested = TRUE and valid_nested_field > 0 AND t1.table_schema in ("sambla_legacy_integration_legacy","sambla_group_data_stream",'sambla_group_data_stream_fi','sambla_group_data_stream_no')
),

all_fields_encryption AS (
    -- Handle non-nested fields (e.g., simple column types)
    SELECT 
      table_schema,
      table_name,
      sensitive_field,
      column_name,
      field_path,
      parent_datatype,
      CONCAT(STRING_AGG(
          encryption_logic,
          ", "
        ) OVER (PARTITION BY table_schema, table_name, column_name)
      ) AS encrypted_fields
    FROM non_nested_field_encryption

    UNION ALL

    SELECT 
      table_schema,
      table_name,
      sensitive_field,
      column_name,
      field_path,
      parent_datatype,
      CASE 
  WHEN parent_datatype = 'ARRAY<STRING>' THEN 
    CONCAT(
      "ARRAY(SELECT ",
      STRING_AGG(
        encryption_logic, 
        ", "
      ) OVER (PARTITION BY table_schema, table_name, sensitive_field, column_name),
      " FROM UNNEST(", column_name, ") AS f_", column_name, ") AS ", column_name
    )
    
  WHEN parent_datatype LIKE 'ARRAY<STRUCT%' THEN 
    CONCAT(
      "ARRAY(SELECT STRUCT(",
      STRING_AGG(
        encryption_logic, 
        ", "
      ) OVER (PARTITION BY table_schema, table_name, sensitive_field, column_name),
      ") FROM UNNEST(", column_name, ") AS f_", column_name, ") AS ", column_name
    ) END AS encrypted_fields
    FROM nested_field_encryption
    where parent_datatype like 'ARRAY%'

    UNION ALL

    -- Handle STRUCT fields (nested)
    SELECT
      table_schema,
      table_name,
      sensitive_field,
      column_name,
      field_path,
      parent_datatype,
      CONCAT(
          "STRUCT(",
            STRING_AGG(
              encryption_logic,
              ", "
            ) OVER (PARTITION BY table_schema, table_name, sensitive_field, column_name),
            ") AS ", column_name
          ) AS encrypted_fields
    FROM nested_field_encryption
    WHERE parent_datatype LIKE 'STRUCT%'
),

market_legacystack_mapping AS (
    SELECT distinct
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
        t1.field_path,
        CASE 
            WHEN t1.table_schema IN ('advisa_history_integration_legacy', 'maxwell_integration_legacy','sambla_group_data_stream') THEN 'SE'
            WHEN t1.table_schema IN ('rahalaitos_integration_legacy', 'lvs_integration_legacy','sambla_group_data_stream_fi') THEN 'FI'
            WHEN t1.table_schema IN ('sambla_group_data_stream_no') THEN 'NO'
            ELSE 'OTHER MARKETS'
        END AS market_identifier
    FROM 
        all_fields_encryption t1
    INNER JOIN unnested_join_keys t2 
    ON t1.table_name = t2.table_name AND t1.table_schema = t2.table_schema and t2.sensitive_field = t1.field_path 
    
),

exclude_tables_list AS (
  select ["marketing_contact_service_prospects_sgds_r", "exp_user_service_users_sgds_r", "exp_notification_service_mail_logs_sgds_r", "exp_notification_service_email_logs_sgds_r", "exp_tracking_service_sent_events_sgds_r", 
  "advisory_service_application_creditor_notes_sgds_r", "identity_verification_service_identity_verification_log_sgds_r", "exp_tracking_service_application_cookie_mappings_sgds_r",
  "exp_bidding_service_bidding_logs_sgds_r", "eiendomsverdi_service_reports_sgds_r"] as exclude_tables
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
--------------- SSN join ---------------
    CASE 
      WHEN mlm.is_table_contains_ssn AND mlm.table_name NOT IN (SELECT table FROM exclude_tables_list, UNNEST(exclude_tables) AS table) AND mlm.table_name != 'marketing_contact_service_contact_blocks_sgds_r' THEN CONCAT(
        'WITH data_with_ssn_rules AS (',
        'SELECT ',
        '*, ',
        CASE 
          WHEN mlm.market_identifier = 'OTHER MARKETS' THEN 
          CONCAT(
            'CASE ',
            'WHEN ', CASE WHEN mlm.table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p','customers_ppi_p','insurance_products_ppi_p') THEN 'market'  ELSE 'country_code' END ,'= "SE" THEN LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING), "[^0-9]", ""), 12) ',
            'WHEN ', CASE WHEN mlm.table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p','customers_ppi_p','insurance_products_ppi_p') THEN 'market' ELSE 'country_code' END ,'= "NO" THEN LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING), "[^0-9]", ""), 11) ',
            'WHEN ', CASE WHEN mlm.table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p','customers_ppi_p','insurance_products_ppi_p') THEN 'market' ELSE 'country_code' END ,'= "DK" THEN LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING), "[^0-9]", ""), 10) ',
            'WHEN ', CASE WHEN mlm.table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p','customers_ppi_p','insurance_products_ppi_p') THEN 'market' ELSE 'country_code' END ,'= "FI" THEN LEFT(REGEXP_REPLACE(UPPER(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING)), "[^0-9-+A-Z]", ""), 11) ' ,
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
          WHEN mlm.market_identifier = 'NO' THEN 
            CONCAT(
              'LEFT(REGEXP_REPLACE(UPPER(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING)), "[^0-9]", ""), 11) AS ssn_clean'
            )
        END
        ,
        CASE 
            WHEN mlm.table_schema IN ('sambla_group_data_stream', 'sambla_group_data_stream_fi','sambla_group_data_stream_no') THEN ' ,_PARTITIONTIME AS date_partition ' ELSE ''
        END
        ,
        ' FROM', CASE WHEN mlm.table_schema in ('salus_group_integration','sambla_group_data_stream',"sambla_group_data_stream_fi","sambla_group_data_stream_no","helios_staging") THEN '`data-domain-data-warehouse.' ELSE '`sambla-data-staging-compliance.' END, mlm.table_schema, '.', mlm.table_name, '` raw) ',
        
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
        'LEFT JOIN `sambla-group-compliance-db.compilance_database.gdpr_vault_rudolf` VAULT ',
        'ON CAST(raw.ssn_clean AS STRING) = VAULT.ssn'
      )
 ------ APPLICATION ID join -----------
      WHEN mlm.is_table_contains_app_id AND mlm.table_name NOT IN (SELECT table FROM exclude_tables_list, UNNEST(exclude_tables) AS table) AND mlm.table_name != 'marketing_contact_service_contact_blocks_sgds_r' THEN 
        CONCAT(
          'WITH raw_data_with_stack AS (',
            'SELECT *, ',
            '"cdc" as stack',
              CASE 
                  WHEN mlm.table_schema IN ('sambla_group_data_stream', 'sambla_group_data_stream_fi','sambla_group_data_stream_no') THEN ' ,_PARTITIONTIME AS date_partition ' ELSE ''
              END
              ,
              ' FROM', CASE WHEN mlm.table_schema in ('salus_group_integration','sambla_group_data_stream',"sambla_group_data_stream_fi","sambla_group_data_stream_no","helios_staging") THEN '`data-domain-data-warehouse.' ELSE '`sambla-data-staging-compliance.' END, mlm.table_schema, '.', mlm.table_name, '` raw), ',
            ---
            'stack_vault as ( '
                'select uuid, aead_key, loan_application_oid, stack from `sambla-group-compliance-db.compilance_database.gdpr_vault_rudolf`, unnest(history) as history ',
                'where history.stack  = "cdc" )',
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
              'WHEN (CAST(raw.', STRING_AGG(DISTINCT mlm.j_key_app_id, ', '), ' AS STRING) IS NOT NULL AND CAST(raw.', STRING_AGG(DISTINCT mlm.j_key_app_id, ', '), ' AS STRING) <> "" AND CAST(raw.', STRING_AGG(DISTINCT mlm.j_key_app_id, ', '), ' AS STRING) <> "0" AND VAULT.uuid IS NOT NULL) OR LOWER(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key_app_id, ', '), ' AS STRING)) =  "anonymized" THEN TRUE ',
                'ELSE FALSE ',
              'END AS is_anonymised ',
              ' FROM raw_data_with_stack raw ',
              'LEFT JOIN stack_vault vault ',
                  'ON CAST(raw.', STRING_AGG(DISTINCT mlm.j_key_app_id, ', '), ' AS STRING) = vault.loan_application_oid '
                  'AND raw.stack = vault.stack'
                    )
 ------ double join -----------
      WHEN mlm.is_table_contains_mobile AND mlm.is_table_contains_email AND not mlm.is_table_contains_ssn AND mlm.table_name NOT IN (SELECT table FROM exclude_tables_list, UNNEST(exclude_tables) AS table) AND mlm.table_name != 'marketing_contact_service_contact_blocks_sgds_r' THEN 
        CONCAT(
          'WITH vault_mobiles_flattened_intial AS (',
            'SELECT history.mobile_phone AS mobile,',
              'history.stack,',
              'history.created_at,',
              'vault.uuid,',
              'vault.aead_key ',
              'FROM `sambla-group-compliance-db.compilance_database.gdpr_vault_rudolf` AS vault '
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
              'FROM `sambla-group-compliance-db.compilance_database.gdpr_vault_rudolf` AS vault '
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
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = "46" THEN CONCAT("+", raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ')',
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "046" THEN CONCAT("+", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 4) = "0046" THEN CONCAT("+", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN CONCAT("+46", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'ELSE "+46" || ', STRING_AGG(DISTINCT mlm.j_key_mobile, ', ') ,' END '
        )

-- -- For the DK market
    WHEN mlm.market_identifier = "DK" THEN 
        CONCAT(
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = "45" THEN CONCAT("+", raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ')',
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "045" THEN CONCAT("+", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 4) = "0045" THEN CONCAT("+", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN CONCAT("+45", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'ELSE "+45" || ', STRING_AGG(DISTINCT mlm.j_key_mobile, ', ') ,' END '
        )

-- For the NO market
    WHEN mlm.market_identifier = "NO" THEN 
        CONCAT(
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = "47" THEN ',
                'CASE WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 10 THEN CONCAT("+", REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) ELSE NULL END ',
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "047" AND LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 11 THEN CONCAT("+", SUBSTRING(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", ""), 2)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 4) = "0047" AND LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 12 THEN CONCAT("+", SUBSTRING(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", ""), 3)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN CASE WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 9 THEN CONCAT("+47", SUBSTRING(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", ""), 2)) ELSE NULL END '
            'WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 8 THEN "+47" || REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "") END '
        )
END,
      'ELSE CASE WHEN LENGTH(REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', " ", "")) > 6 AND LENGTH(REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', " ", "")) < 18 THEN REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', " ", "") END END AS mobile_clean, ',
                CASE WHEN mlm.table_schema IN ('sambla_group_data_stream', 'sambla_group_data_stream_fi','sambla_group_data_stream_no') THEN '_PARTITIONTIME AS date_partition ' ELSE '' END,
  -- Mobile clean CTE ends
  CONCAT(
                ' FROM', CASE WHEN mlm.table_schema in ('salus_group_integration','sambla_group_data_stream',"sambla_group_data_stream_fi","sambla_group_data_stream_no","helios_staging") THEN '`data-domain-data-warehouse.' ELSE '`sambla-data-staging-compliance.' END, mlm.table_schema, '.', mlm.table_name, '` raw ) ',

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
                'WHEN (raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' IS NOT NULL AND raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' <> "" AND raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' <> "0" AND vault_email.uuid IS NOT NULL) OR (raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ' IS NOT NULL AND raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ' <> "" AND raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ' <> "0" AND vault_mobile.uuid IS NOT NULL) OR LOWER(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ') =  "anonymized" OR LOWER(raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ') =  "anonymized" THEN TRUE ',
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
      WHEN mlm.is_table_contains_email AND mlm.table_name NOT IN (SELECT table FROM exclude_tables_list, UNNEST(exclude_tables) AS table) AND mlm.table_name != 'marketing_contact_service_contact_blocks_sgds_r' THEN 
        CONCAT(
          'WITH vault_emails_flattened_initial AS (',
            'SELECT history.email AS email,',
              'history.stack,',
              'history.created_at,',
              'vault.uuid,',
              'vault.aead_key ',
              'FROM `sambla-group-compliance-db.compilance_database.gdpr_vault_rudolf` AS vault '
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
                'WHEN (raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' IS NOT NULL AND raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' <> "" AND raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' <> "0" AND VAULT.uuid IS NOT NULL) OR LOWER(raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ') =  "anonymized" THEN TRUE ',
              'END AS is_anonymised, ',
                CASE 
                    WHEN mlm.table_schema IN ('sambla_group_data_stream', 'sambla_group_data_stream_fi','sambla_group_data_stream_no') THEN '_PARTITIONTIME AS date_partition ' ELSE ''
                END
                ,
                ' FROM', CASE WHEN mlm.table_schema in ('salus_group_integration','sambla_group_data_stream',"sambla_group_data_stream_fi","sambla_group_data_stream_no","helios_staging") THEN '`data-domain-data-warehouse.' ELSE '`sambla-data-staging-compliance.' END, mlm.table_schema, '.', mlm.table_name, '` raw ',
              'LEFT JOIN vault_emails_flattened vault ',
                      'ON raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' = vault.email'
                    )
 ------ mobile join -----------
      WHEN mlm.is_table_contains_mobile AND mlm.table_name NOT IN (SELECT table FROM exclude_tables_list, UNNEST(exclude_tables) AS table) AND mlm.table_name != 'marketing_contact_service_contact_blocks_sgds_r' THEN 
        CONCAT(
          'WITH vault_mobiles_flattened_intial AS (',
            'SELECT history.mobile_phone AS mobile,',
              'history.stack,',
              'history.created_at,',
              'vault.uuid,',
              'vault.aead_key ',
              'FROM `sambla-group-compliance-db.compilance_database.gdpr_vault_rudolf` AS vault '
              'CROSS JOIN UNNEST(vault.history) AS history '
              'WHERE history.mobile_phone IS NOT NULL AND history.mobile_phone != ""), '
          'vault_mobiles_flattened AS ( '
          'SELECT mobile, stack, uuid, aead_key FROM vault_mobiles_flattened_intial '
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
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = "46" THEN CONCAT("+", raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ')',
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "046" THEN CONCAT("+", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 4) = "0046" THEN CONCAT("+", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN CONCAT("+46", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'ELSE "+46" || ', STRING_AGG(DISTINCT mlm.j_key_mobile, ', ') ,' END '
        )

-- -- For the DK market
    WHEN mlm.market_identifier = "DK" THEN 
        CONCAT(
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = "45" THEN CONCAT("+", raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ')',
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "045" THEN CONCAT("+", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 4) = "0045" THEN CONCAT("+", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN CONCAT("+45", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'ELSE "+45" || ', STRING_AGG(DISTINCT mlm.j_key_mobile, ', ') ,' END '
        )

-- For the NO market
    WHEN mlm.market_identifier = "NO" THEN 
        CONCAT(
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = "47" THEN ',
                'CASE WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 10 THEN CONCAT("+", REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) ELSE NULL END ',
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "047" AND LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 11 THEN CONCAT("+", SUBSTRING(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", ""), 2)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 4) = "0047" AND LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 12 THEN CONCAT("+", SUBSTRING(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", ""), 3)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN CASE WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 9 THEN CONCAT("+47", SUBSTRING(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", ""), 2)) ELSE NULL END '
            'WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 8 THEN "+47" || REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "") END '
        )
END,
      'ELSE CASE WHEN LENGTH(REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', " ", "")) > 6 AND LENGTH(REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', " ", "")) < 18 THEN REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', " ", "") END END AS mobile_clean, ',
                CASE WHEN mlm.table_schema IN ('sambla_group_data_stream', 'sambla_group_data_stream_fi','sambla_group_data_stream_no') THEN '_PARTITIONTIME AS date_partition ' ELSE '' END,
  -- Mobile clean CTE ends
  CONCAT(
                ' FROM', CASE WHEN mlm.table_schema in ('salus_group_integration','sambla_group_data_stream',"sambla_group_data_stream_fi","sambla_group_data_stream_no","helios_staging") THEN '`data-domain-data-warehouse.' ELSE '`sambla-data-staging-compliance.' END, mlm.table_schema, '.', mlm.table_name, '` raw ) ',

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
                'WHEN (CAST(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ' AS STRING) IS NOT NULL AND CAST(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ' AS STRING) <> "" AND CAST(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ' AS STRING) <> "0" AND VAULT.uuid IS NOT NULL) OR LOWER(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ' AS STRING)) =  "anonymized" THEN TRUE ',
                  'ELSE FALSE ',
                'ELSE FALSE ',
              'END AS is_anonymised ',
      'FROM mobile_cleaned raw ',
              'LEFT JOIN vault_mobiles_flattened vault_mobile ',
                      'ON raw.mobile_clean = vault_mobile.mobile '
                    )
        )
------ triple join -------
      WHEN mlm.table_name = 'marketing_contact_service_contact_blocks_sgds_r' THEN CONCAT(
          'WITH vault_mobiles_flattened_intial AS (',
            'SELECT history.mobile_phone AS mobile,',
              'history.stack,',
              'history.created_at,',
              'vault.uuid,',
              'vault.aead_key ',
              'FROM `sambla-group-compliance-db.compilance_database.gdpr_vault_rudolf` AS vault '
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
              'FROM `sambla-group-compliance-db.compilance_database.gdpr_vault_rudolf` AS vault '
              'CROSS JOIN UNNEST(vault.history) AS history '
              'WHERE history.email IS NOT NULL AND history.email != ""), '
          'vault_emails_flattened AS ( '
          'SELECT email, stack, uuid, aead_key FROM vault_emails_flattened_initial '
          'QUALIFY ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at DESC) = 1 ), '
  -- Mobile clean CTE
      'mobile_and_ssn_cleaned AS ( ',
        'SELECT *, ',
        'CASE WHEN raw.payload.type != "mobile_phone" THEN NULL '
        'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = " +" THEN SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) ',
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
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = "46" THEN CONCAT("+", raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ')',
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "046" THEN CONCAT("+", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 4) = "0046" THEN CONCAT("+", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN CONCAT("+46", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'ELSE "+46" || ', STRING_AGG(DISTINCT mlm.j_key_mobile, ', ') ,' END '
        )

-- -- For the DK market
    WHEN mlm.market_identifier = "DK" THEN 
        CONCAT(
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = "45" THEN CONCAT("+", raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ')',
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "045" THEN CONCAT("+", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 4) = "0045" THEN CONCAT("+", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN CONCAT("+45", SUBSTRING(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2)) '
            'ELSE "+45" || ', STRING_AGG(DISTINCT mlm.j_key_mobile, ', ') ,' END '
        )

-- For the NO market
    WHEN mlm.market_identifier = "NO" THEN 
        CONCAT(
            'CASE WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 2) = "47" THEN ',
                'CASE WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 10 THEN CONCAT("+", REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) ELSE NULL END ',
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 3) = "047" AND LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 11 THEN CONCAT("+", SUBSTRING(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", ""), 2)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 4) = "0047" AND LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 12 THEN CONCAT("+", SUBSTRING(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", ""), 3)) '
            'WHEN LEFT(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', 1) = "0" THEN CASE WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 9 THEN CONCAT("+47", SUBSTRING(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", ""), 2)) ELSE NULL END '
            'WHEN LENGTH(REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "")) = 8 THEN "+47" || REGEXP_REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', "[^0-9\\\\s]", "") END '
        )
END,
      'ELSE CASE WHEN LENGTH(REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', " ", "")) > 6 AND LENGTH(REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', " ", "")) < 18 THEN REPLACE(raw.', STRING_AGG(DISTINCT mlm.j_key_mobile, ', '), ', " ", "") END END AS mobile_clean, ',
                CASE WHEN mlm.table_schema IN ('sambla_group_data_stream', 'sambla_group_data_stream_fi','sambla_group_data_stream_no') THEN '_PARTITIONTIME AS date_partition, ' ELSE '' END,

                ---
        CASE 
          WHEN mlm.market_identifier = 'OTHER MARKETS' THEN 
          CONCAT(
            'CASE ',
            'WHEN ', CASE WHEN mlm.table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p','customers_ppi_p','insurance_products_ppi_p') THEN 'market'  ELSE 'country_code' END ,'= "SE" THEN LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING), "[^0-9]", ""), 12) ',
            'WHEN ', CASE WHEN mlm.table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p','customers_ppi_p','insurance_products_ppi_p') THEN 'market' ELSE 'country_code' END ,'= "NO" THEN LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING), "[^0-9]", ""), 11) ',
            'WHEN ', CASE WHEN mlm.table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p','customers_ppi_p','insurance_products_ppi_p') THEN 'market' ELSE 'country_code' END ,'= "DK" THEN LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING), "[^0-9]", ""), 10) ',
            'WHEN ', CASE WHEN mlm.table_name IN ('applications_all_versions_sambq_p'
,'applications_sambq_p','customers_ppi_p','insurance_products_ppi_p') THEN 'market' ELSE 'country_code' END ,'= "FI" THEN LEFT(REGEXP_REPLACE(UPPER(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING)), "[^0-9-+A-Z]", ""), 11) ' ,
            'END AS ssn_clean'
          ) 
          WHEN mlm.market_identifier = 'SE' THEN 
            CONCAT(
              'CASE WHEN raw.payload.type != "post" THEN NULL ELSE LEFT(REGEXP_REPLACE(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING), "[^0-9]", ""), 12) END AS ssn_clean'
            )
          WHEN mlm.market_identifier = 'FI' THEN 
            CONCAT(
              'CASE WHEN raw.payload.type != "post" THEN NULL ELSE LEFT(REGEXP_REPLACE(UPPER(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING)), "[^0-9-+A-Z]", ""), 11) END AS ssn_clean'
            )
          WHEN mlm.market_identifier = 'NO' THEN 
            CONCAT(
              'CASE WHEN raw.payload.type != "post" THEN NULL ELSE LEFT(REGEXP_REPLACE(UPPER(CAST(raw.', STRING_AGG(DISTINCT mlm.j_key, ', '), ' AS STRING)), "[^0-9]", ""), 11) END AS ssn_clean'
            )
        END,
                ---
  -- Mobile clean CTE ends
  CONCAT(
                ' FROM', CASE WHEN mlm.table_schema in ('salus_group_integration','sambla_group_data_stream',"sambla_group_data_stream_fi","sambla_group_data_stream_no","helios_staging") THEN '`data-domain-data-warehouse.' ELSE '`sambla-data-staging-compliance.' END, mlm.table_schema, '.', mlm.table_name, '` raw ) ',

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
                'WHEN (raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' IS NOT NULL AND raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' <> "" AND raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' <> "0" AND COALESCE(vault.uuid, vault_email.uuid, vault_mobile.uuid) IS NOT NULL) THEN TRUE ',
                  'ELSE FALSE ',
              'END AS is_anonymised ',
      'FROM mobile_and_ssn_cleaned raw ',
              'LEFT JOIN `sambla-group-compliance-db.compilance_database.gdpr_vault_rudolf` vault ',
                      'ON CASE WHEN raw.payload.type = "post" THEN raw.ssn_clean ELSE NULL END = vault.ssn '
              'LEFT JOIN vault_emails_flattened vault_email ',
                      'ON CASE WHEN raw.payload.type = "email" THEN raw.', STRING_AGG(DISTINCT mlm.j_key_email, ', '), ' ELSE NULL END = vault_email.email '
              'LEFT JOIN vault_mobiles_flattened vault_mobile ',
                      'ON CASE WHEN raw.payload.type = "mobile_phone" THEN raw.mobile_clean ELSE NULL END = vault_mobile.mobile '
                    )
        )
------ NO join ---------
      ELSE CONCAT(
        'SELECT * , False AS is_anonymised,',
        CASE 
        WHEN mlm.table_schema IN ('sambla_group_data_stream', 'sambla_group_data_stream_fi','sambla_group_data_stream_no')
        THEN ' _PARTITIONTIME AS date_partition' ELSE ''
        END,' FROM ', CASE WHEN mlm.table_schema in ('salus_group_integration','sambla_group_data_stream',"sambla_group_data_stream_fi","sambla_group_data_stream_no","helios_staging") THEN '`data-domain-data-warehouse.' ELSE '`sambla-data-staging-compliance.' END,
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

select * from final 
WHERE final_encrypted_columns IS NOT NULL
--AND table_schema IN ("sambla_group_data_stream_no")