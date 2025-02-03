with users_sambq_r as (
    select * from `${project_id}.${dataset_id}.users_gcs_streaming` WHERE operationType != 'delete'
)

SELECT * FROM users_sambq_r