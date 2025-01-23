WITH
  table_columns AS (
    {{source_table_columns}}
),
  policy_tags AS (
    SELECT
      *
    FROM
      `{{raw_layer_project}}.policy_tags_metadata.policy_tags`
    WHERE display_name not in ("name","postal_code_id","data")
  )
SELECT
  t1.*,
  t2.* ,
  CONCAT(
    "projects/sambla-data-staging-compliance",
    "/locations/europe-north1/",
    "taxonomies/",
    t2.taxonomy_id,
    "/policyTags/",
    t2.parent_policy_tag_id
  ) AS iam_policy_name
FROM
  table_columns AS t1
INNER JOIN
  policy_tags AS t2
ON
  LOWER(REPLACE(t1.column_name, '_', '')) = LOWER(REPLACE(t2.display_name, '_', ''))
-- we filter the only the gdpr_test taxonomy where all the complaint policy tags are created
WHERE
  t1.table_name not in ("rahalaitos_laina_businessinfo_raha_r","insurance_insurance_gender_raha_r","rahalaitos_laina_decision_data_raha_r")
  and
  t1.data_type NOT IN ("INT64","BOOL")