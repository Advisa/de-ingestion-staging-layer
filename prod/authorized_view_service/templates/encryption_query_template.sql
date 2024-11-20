WITH table_columns AS (
  {{query_table_columns}} -- Replace with your actual query for table columns
),
-- this where condition should be removed after table-policy mapping is settled
policy_tags AS (
  SELECT
    *
  FROM
    `{{raw_layer_project}}.policy_tags_metadata.policy_tags`
  WHERE display_name!="data"
),
sensitive_fields AS (
  SELECT
    t1.table_schema,
    t1.table_name,
    t1.column_name
  FROM
    table_columns AS t1
    INNER JOIN policy_tags AS t2 ON t1.column_name = t2.display_name
),
join_keys AS (
  SELECT
    table_schema,
    table_name,
    ARRAY_AGG(column_name) AS join_keys,
    IF (
    "ssn" IN UNNEST(ARRAY_AGG(column_name)) 
    OR "ssn_id" IN UNNEST(ARRAY_AGG(column_name)) 
    OR "national_id" IN UNNEST(ARRAY_AGG(column_name)) 
    OR "nationalId" IN UNNEST(ARRAY_AGG(column_name)),
    TRUE,
    FALSE
) AS includes_ssn

  FROM
    sensitive_fields
  GROUP BY
    table_schema,
    table_name
),
unnested_join_keys AS (
  SELECT
    table_schema,
    table_name,
    join_key
  FROM
    join_keys,
    UNNEST(join_keys.join_keys) AS join_key
  WHERE
    includes_ssn
),
encryption_queries AS (
  SELECT
    raw.table_schema,
    raw.table_name,
    raw.join_key,
    CONCAT(
      'CASE WHEN VAULT.uuid IS NOT NULL THEN ',
      'TO_HEX(SAFE.DETERMINISTIC_ENCRYPT(VAULT.aead_key, raw.',
      raw.join_key,
      ', VAULT.uuid)) ',
      'ELSE raw.',
      raw.join_key,
      ' END AS ',
      raw.join_key
    ) AS encrypted_columns,
    STRING_AGG(raw.join_key, ', ') AS sensitive_column_names
  FROM
    unnested_join_keys raw
    LEFT JOIN `{{compliance_project}}.compilance_database.gdpr_vault` VAULT ON raw.join_key = VAULT.ssn
  GROUP BY
    raw.table_schema,
    raw.table_name,
    raw.join_key
)
SELECT
  table_schema,
  table_name,
  STRING_AGG(CASE WHEN join_key IN ('ssn', 'ssn_id','nationalId', 'national_id') THEN join_key END, '') as j_key,
  CONCAT(
    'SELECT ',
    STRING_AGG(encrypted_columns, ', '),
    ', raw.* EXCEPT(',
    STRING_AGG(DISTINCT join_key, ', '),
    ') ',
    'FROM `{{raw_layer_project}}.',
    table_schema,
    '.',
    table_name,
    '` raw ',
    'LEFT JOIN `{{compliance_project}}.compilance_database.gdpr_vault` VAULT ON raw.',
    STRING_AGG(CASE WHEN join_key IN ('ssn', 'ssn_id','nationalId', 'national_id') THEN join_key END, '') ,  
    ' = VAULT.ssn '
  ) AS encrypted_columns
FROM
  encryption_queries
GROUP BY
  table_schema,
  table_name