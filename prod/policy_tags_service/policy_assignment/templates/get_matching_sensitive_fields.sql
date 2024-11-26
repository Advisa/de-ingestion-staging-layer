WITH
  table_columns AS (
    {{source_table_columns}}
    UNION ALL
    SELECT * FROM `data-domain-data-warehouse.salus_group_integration`.INFORMATION_SCHEMA.COLUMNS
    UNION ALL
    SELECT * FROM `data-domain-data-warehouse.salus_group_integration`.INFORMATION_SCHEMA.COLUMNS
  ),
  policy_tags AS (
    SELECT
      *
    FROM
      `{{raw_layer_project}}.policy_tags_metadata.policy_tags`
    WHERE display_name!="name"
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
  t1.column_name = t2.display_name
-- we filter the only the gdpr_test taxonomy where all the complaint policy tags are created
WHERE
  taxonomy_id = "6126692965998272750" 
  and  
  t1.table_name not in ("rahalaitos_laina_businessinfo_raha_r","insurance_insurance_gender_raha_r","rahalaitos_laina_decision_data_raha_r")
