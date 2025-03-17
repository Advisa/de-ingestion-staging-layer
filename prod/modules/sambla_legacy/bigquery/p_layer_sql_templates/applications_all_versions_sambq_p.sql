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
FROM
  applications_all_versions_sambq_r a
--QUALIFY ROW_NUMBER() OVER(PARTITION BY _id ORDER BY __v desc, ifnull(updatedAt,'1990-01-01') desc, ifnull(time_archived,'1990-01-01') DESC) =1
    
