WITH table_columns AS (
    -- replace this with your actual query for table columns
  {{query_table_columns}} 
),

table_columns_filtered AS (
    SELECT * FROM table_columns WHERE table_name!="applications_lvs"
)


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

sensitive_fields_unnested AS (
  -- Extract nested sensitive fields from STRUCT ARRAY
  SELECT
    f.table_schema,
    f.table_name,
    f.field_name AS column_name,
    -- Normalize the UNNESTED STRUCT field
    LOWER(REPLACE(f.field_name, "_", "")) AS normalized_column,
    CONCAT(f.column_name, '.', f.field_name) AS full_column_name,
    -- Extract STRUCT field's column name , e.g. financials.amount -> financials
    f.column_name AS parent_column,
    REGEXP_EXTRACT_ALL(f.nested_fields_data_type, r'\b(\w+)\s+\w+') AS struct_fields_list
  FROM (
    SELECT
      SPLIT(field, " ")[OFFSET(0)] AS field_name,
      SPLIT(field, " ")[OFFSET(1)] AS data_type,
      table_name,
      table_schema,
      column_name,
      nested_fields_data_type
    FROM (
      SELECT
        REGEXP_EXTRACT_ALL(data_type, r"\w+ \w+") AS fields,
        table_name,
        table_schema,
        column_name,
        data_type AS nested_fields_data_type
      FROM table_columns_filtered
    ), UNNEST(fields) AS field
  ) f
  INNER JOIN policy_tags p 
    ON LOWER(REPLACE(f.field_name, "_", "")) = LOWER(REPLACE(p.display_name, "_", ""))
  WHERE 
  -- Exclude BOOLEAN fields, no need to encrypt those
    f.data_type NOT IN ("BOOL") 
    AND 
  -- Exclude the tables which has deeply nested fields
    table_schema NOT IN ('sambla_legacy_integration_legacy') 
    AND 
    table_name NOT IN ('credit_remarks_lvs_r')
),

all_sensitive_fields AS (
  -- Combine normal and nested sensitive fields
  SELECT *, column_name AS full_column_name, FALSE AS is_nested FROM sensitive_fields
  UNION DISTINCT
  SELECT * EXCEPT(parent_column, struct_fields_list), TRUE AS is_nested FROM sensitive_fields_unnested
),

join_keys AS (
  -- Identify join keys for linking tables to VAULT
  SELECT
    table_schema,
    table_name,
    -- Use Full_column_name (CONCAT(f.column_name, '.', f.field_name)) for struct fields, column_name for non-struct fields
    ARRAY_AGG(full_column_name) AS join_keys,
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
    is_nested
  FROM all_sensitive_fields
  GROUP BY table_schema, table_name, is_nested
),

unnested_join_keys AS (
  -- Extract sensitive fields and parent fields
  SELECT
    table_schema,
    table_name,
    IF(is_nested, SPLIT(join_key, '.')[OFFSET(1)], join_key) AS sensitive_field,
    IF(is_nested, SPLIT(join_key, '.')[OFFSET(0)], join_key) AS parent_field,
    is_table_contains_ssn,
    CASE 
      WHEN table_name != "people_adhis_r" 
        AND LOWER(REPLACE(IF(is_nested, SPLIT(join_key, '.')[OFFSET(1)], join_key), "_", "")) 
        IN ('ssn', 'ssnid', 'nationalid', 'customerssn', 'sotu', 'yvsotu') 
        THEN IF(is_nested, SPLIT(join_key, '.')[OFFSET(1)], join_key) -- If field belongs to a STRUCT ARRAY, extract its column_name
     -- Exceptional case
      WHEN table_name = "people_adhis_r" 
        THEN "national_id_sensitive" 
    END AS j_key,
    is_nested
  FROM join_keys, UNNEST(join_keys.join_keys) AS join_key
),

nested_field_encryption AS (
  -- Encrypt fields inside STRUCTs
  SELECT
    t1.table_schema,
    t1.table_name,
    CONCAT(
      "ARRAY(SELECT STRUCT(",
      STRING_AGG(DISTINCT CONCAT(
        "CASE WHEN VAULT.uuid IS NOT NULL THEN ",
        "TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, CAST(f.", column_name, " AS STRING), VAULT.uuid)) ",
        "ELSE CAST(f.", column_name, " AS STRING) END AS ", column_name
      ), ", "),
      ", ", 
      STRING_AGG(DISTINCT CONCAT("f.", field), ", "),
      ") FROM UNNEST(raw.", parent_column, ") AS f) AS ", parent_column
    ) AS encrypted_fields
  FROM unnested_join_keys t1
  INNER JOIN sensitive_fields_unnested t2
    ON t1.sensitive_field = t2.full_column_name
  CROSS JOIN UNNEST(t2.struct_fields_list) AS field
  WHERE is_nested = TRUE
    AND field NOT IN (SELECT column_name FROM sensitive_fields_unnested)
  GROUP BY t1.table_schema, t1.table_name, t2.parent_column
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
  WHERE is_nested = FALSE
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
        STRING_AGG(DISTINCT parent_field, ', '),
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
  FROM (SELECT * FROM non_nested_field_encryption UNION ALL SELECT * FROM nested_field_encryption) t1
  INNER JOIN unnested_join_keys t2 
    ON t1.table_name = t2.table_name 
    AND t1.table_schema = t2.table_schema
  GROUP BY t1.table_schema, t1.table_name, t2.is_table_contains_ssn
)

SELECT * FROM final WHERE final_encrypted_columns IS NOT NULL;
