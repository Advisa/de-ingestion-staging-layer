WITH table_columns AS (
    -- replace this with your actual query for table columns
  {{query_table_columns}} 
),

table_columns_filtered AS (
    SELECT * FROM table_columns WHERE table_name!="applications_lvs"
),


policy_tags AS (
  SELECT *
  FROM `{{raw_layer_project}}.policy_tags_metadata.policy_tags`
  -- gdpr_compliance_measures_prod_high, gdpr_compliance_measures_prod_medium, gdpr_compliance_measures_prod_low
  WHERE taxonomy_id IN ('462501529798891334', '1348545653474742340', '8452725999489655507')
),

sensitive_fields AS (
  -- Identify non-struct sensitive fields based on policy tags match
  SELECT
    r.table_schema,
    r.table_name,
    r.column_name,
    LOWER(REPLACE(r.column_name, "_", "")) AS normalized_column
  FROM table_columns_filtered r
  INNER JOIN policy_tags p 
    ON LOWER(REPLACE(r.column_name, "_", "")) = LOWER(REPLACE(p.display_name, "_", ""))
  WHERE r.data_type NOT LIKE "STRUCT%" 
    AND r.data_type NOT LIKE "ARRAY%" 
    AND r.data_type NOT LIKE "BOOL"
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
    ) AS is_table_contains_ssn,
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

final AS (
  -- Construct the final query for each table
  SELECT
    t1.table_schema,
    t1.table_name,
    CASE 
      WHEN t2.is_table_contains_ssn THEN CONCAT(
        'SELECT ',
        STRING_AGG(DISTINCT encrypted_fields, ", "),
        ', raw.* EXCEPT(',
        STRING_AGG(DISTINCT sensitive_field, ', '),
        ') FROM `{{raw_layer_project}}.',
        t1.table_schema, '.', t1.table_name,
        '` raw LEFT JOIN `{{compliance_project}}.compilance_database.gdpr_vault` VAULT ',
        'ON CAST(raw.', STRING_AGG(CASE WHEN j_key IS NOT NULL THEN j_key END, '') ,  ' AS STRING) = VAULT.ssn ') 
      ELSE CONCAT(
        'SELECT * FROM `{{raw_layer_project}}.',
        t1.table_schema,
        '.',
        t1.table_name)
  END AS final_encrypted_columns
  FROM  non_nested_field_encryption t1
  INNER JOIN unnested_join_keys t2 
    ON t1.table_name = t2.table_name 
    AND t1.table_schema = t2.table_schema
  GROUP BY t1.table_schema, t1.table_name, t2.is_table_contains_ssn
)

SELECT * FROM final WHERE final_encrypted_columns IS NOT NULL;