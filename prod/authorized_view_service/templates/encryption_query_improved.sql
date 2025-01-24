WITH table_columns AS (
    -- replace this with your actual query for table columns
  {{query_table_columns}} 
),

policy_tags AS (
  SELECT
    *
  FROM
    `{{raw_layer_project}}.policy_tags_metadata.policy_tags`
),

sensitive_fields_raw AS (
  SELECT
    t1.table_schema,
    t1.table_name,
    t1.column_name,
    LOWER(REPLACE(t1.column_name, "_", ""))as normalized_column
  FROM
    table_columns AS t1
    INNER JOIN policy_tags AS t2 ON LOWER(REPLACE(t1.column_name, "_", "")) = LOWER(REPLACE(t2.display_name, "_", ""))
),

sensitive_fields_unnested AS (SELECT
    table_schema,
    table_name,
    field_name AS column_name,
    LOWER(REPLACE(field_name, "_", ""))as normalized_column,
    CONCAT(parent_column,".",field_name) as full_column_name
  FROM (
    SELECT
      SPLIT(field, " ")[OFFSET(0)] AS field_name,
      SPLIT(field, " ")[OFFSET  (1)] AS data_type,
      table_name,
      table_schema,
      parent_column
    FROM (
      SELECT
        REGEXP_EXTRACT_ALL(data_type, r"\w+ \w+") AS fields,
        table_name,
        table_schema,
        column_name as parent_column,
      FROM
        table_columns),
      UNNEST(fields) AS field )
  INNER JOIN policy_tags AS t2 ON LOWER(REPLACE(field_name, "_", "")) = LOWER(REPLACE(t2.display_name, "_", ""))
),

sensitive_fields_all AS (
  SELECT *, column_name AS full_column_name, FALSE AS is_nested  FROM sensitive_fields_raw
  --UNION DISTINCT 
  --SELECT *, TRUE AS is_nested FROM sensitive_fields_unnested
),

join_keys AS (
  SELECT
    table_schema,
    table_name,
    ARRAY_AGG(CASE WHEN is_nested THEN full_column_name ELSE column_name END) AS join_keys,
    IF (
    "ssn" IN UNNEST(ARRAY_AGG(normalized_column)) 
    OR "customerssn" IN UNNEST(ARRAY_AGG(normalized_column)) 
    OR "foreignerssn" IN UNNEST(ARRAY_AGG(normalized_column))
    OR "ssnid" IN UNNEST(ARRAY_AGG(normalized_column)) 
    OR "nationalid" IN UNNEST(ARRAY_AGG(normalized_column)) 
    OR "sotu" IN UNNEST(ARRAY_AGG(normalized_column)) 
    OR "yvsotu" IN UNNEST(ARRAY_AGG(normalized_column))
    ,TRUE,
    FALSE
    ) AS is_table_contains_ssn
FROM
    sensitive_fields_all
GROUP BY
    table_schema,
    table_name
)

--SELECT includes_ssn, count(distinct table_name) FROM join_keys group by 1
--176 all tables
--63 total count of tables with sensitive fields
-- 50 tables without ssn but with sensitive fields
--15  tables with ssn 

,unnested_join_keys AS (
  SELECT
    table_schema,
    table_name,
    join_key AS sensitive_field,
    is_table_contains_ssn,
    CASE 
      WHEN table_name != "people_adhis_r" AND LOWER(REPLACE(join_key, "_", "")) IN ('ssn', 'ssnid','nationalid', 'customerssn','sotu','yvsotu') THEN join_key 
      WHEN table_name = "people_adhis_r" THEN "national_id_sensitive" 
    END AS j_key
  FROM
    join_keys,
    UNNEST(join_keys.join_keys) AS join_key
)

,encryption_queries AS (
  SELECT
    raw.table_schema,
    raw.table_name,
    raw.j_key, 
    raw.sensitive_field,
    raw.is_table_contains_ssn,
    CONCAT(
      'CASE WHEN VAULT.uuid IS NOT NULL THEN ',
      'TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, CAST(raw.',
      raw.sensitive_field,
      ' AS STRING)' ', VAULT.uuid)) ',
      'ELSE CAST(raw.',
      raw.sensitive_field,
      ' AS STRING)' ' END AS ',
      raw.sensitive_field
    ) AS encrypted_columns,
    STRING_AGG(raw.sensitive_field, ', ') AS sensitive_column_names
  FROM
    unnested_join_keys raw
    LEFT JOIN `{{compliance_project}}.compilance_database.gdpr_vault` VAULT ON CAST(raw.j_key AS STRING) = VAULT.ssn
  GROUP BY
    raw.table_schema,
    raw.table_name,
    raw.j_key,
    raw.sensitive_field,
    raw.is_table_contains_ssn
)

SELECT DISTINCT
  table_schema,
  table_name,
  CASE 
    WHEN is_table_contains_ssn THEN CONCAT(
    'SELECT ',
    STRING_AGG(encrypted_columns, ', '),
    ', raw.* EXCEPT(',
    STRING_AGG(DISTINCT sensitive_field, ', '),
    ') ',
    'FROM `{{raw_layer_project}}.',
    table_schema,
    '.',
    table_name,
    '` raw ',
    'LEFT JOIN `{{compliance_project}}.compilance_database.gdpr_vault` VAULT ON CAST(raw.',
    STRING_AGG(CASE WHEN j_key IS NOT NULL THEN j_key END, '') ,  ' AS STRING)' ' = VAULT.ssn '
  ) 
    ELSE CONCAT('SELECT * FROM `{{raw_layer_project}}.',
    table_schema,
    '.',
    table_name)
  END AS encrypted_columns,
  is_table_contains_ssn
FROM
  encryption_queries
GROUP BY
  table_schema,
  table_name,
  is_table_contains_ssn

ORDER BY table_name