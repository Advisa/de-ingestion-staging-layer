WITH
  table_columns AS (
    {{source_table_columns}}
  ),
  policy_tags AS (
    SELECT
      *
    FROM
      `test_duygu.policy_tags`
    WHERE display_name!="name"
  )
SELECT
  t1.*,
  t2.* EXCEPT(description),
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
  taxonomy_id = "6126692965998272750";
