WITH
  applications_all_versions_sambq_r AS (
  SELECT
    a.*,
    CASE
      WHEN utm.domain LIKE '%advisa%' THEN 'advisa'
      ELSE 'sambla'
  END
    AS brand,
    "mongodb" AS source_database,
  FROM
    `${project_id}.${dataset_id}.applications_gcs_streaming` a
  WHERE
    operationType != 'delete')

SELECT DISTINCT
  _id AS application_id,
  amount,
  createdAt,
  updatedAt AS updated_at,
  status.sent AS statussent,
  time_archived
FROM
  applications_all_versions_sambq_r a
