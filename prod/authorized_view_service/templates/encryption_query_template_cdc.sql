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
                AND r.table_name IN ('applications_all_versions_sambq_p', 'applications_sambq_p') THEN TRUE
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
      OR "nationalidsensitive" IN UNNEST(ARRAY_AGG(normalized_column) OVER (PARTITION BY table_schema, table_name)),
      TRUE, FALSE
    ) AS is_table_contains_ssn
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
    CASE 
      WHEN table_name != "people_adhis_r" 
        AND LOWER(REPLACE(
            CASE 
                WHEN join_key LIKE '%.%' THEN SUBSTR(join_key, STRPOS(join_key, '.') + 1)
                ELSE join_key
            END, "_", ""
        )) 
        IN ('ssn', 'ssnid', 'nationalid', 'customerssn', 'sotu', 'yvsotu','nationalidsensitive') 
        THEN  join_key
     -- Exceptional case
      WHEN table_name = "people_adhis_r" 
        THEN "national_id_sensitive" 
    END AS j_key
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
                          " AND VAULT.uuid IS NOT NULL THEN ",  
                          "TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, CAST(raw.", column_name, " AS STRING), VAULT.uuid)) ",  
                          "ELSE CAST(raw.", column_name, " AS STRING) END AS ", column_name
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
                              " AND VAULT.uuid IS NOT NULL THEN TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, CAST(raw.", t2.field_path, " AS STRING), VAULT.uuid)) ",
                              "ELSE CAST(raw.", t2.field_path, " AS STRING) END AS ", t2.display_name
                          )
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
                              " AND VAULT.uuid IS NOT NULL THEN TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, CAST(f_", t2.column_name, " AS STRING), VAULT.uuid)) ",
                              "ELSE CAST(f_", t2.column_name, " AS STRING) END AS ", t2.display_name
                          )
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
                              " AND VAULT.uuid IS NOT NULL THEN ",  
                              "TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, CAST(f_", t2.column_name, ".", t2.nested_field, " AS STRING), VAULT.uuid)) ",  
                              "ELSE CAST(f_", t2.column_name, ".", t2.nested_field, " AS STRING) END AS ", t2.nested_field)
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
    WHERE t2.is_nested = TRUE and valid_nested_field > 1 AND t1.table_schema in ("sambla_legacy_integration_legacy","sambla_group_data_stream",'sambla_group_data_stream_fi','sambla_group_data_stream_no')
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
        t2.sensitive_field,
        t2.j_key,
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
        'LEFT JOIN `{{compliance_project}}.compilance_database.{{gdpr_vault_table}}` VAULT ',
        'ON CAST(raw.ssn_clean AS STRING) = VAULT.ssn'
      )

      ELSE CONCAT(
        'SELECT * , False AS is_anonymised FROM ', CASE WHEN mlm.table_schema in ('salus_group_integration','sambla_group_data_stream',"sambla_group_data_stream_fi","sambla_group_data_stream_no","helios_staging") THEN '`data-domain-data-warehouse.' ELSE '`sambla-data-staging-compliance.' END,
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

select * from final 
WHERE final_encrypted_columns IS NOT NULL
--AND table_schema IN ("sambla_group_data_stream_no")

