WITH largest_loan_arrays AS (
    SELECT loans, _id 
    FROM `${project_id}.${dataset_id}.applications_gcs_streaming`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY array_length(loans) DESC) = 1
),

base_data AS (
    SELECT 
        a.*, 
        a._id AS application_id, 
        --a.time_archived, 
        a.__v AS versions, 
        a.updatedAt AS updated_at, 
        a.status.sent AS is_status_sent, 
        CASE WHEN utm.domain LIKE '%advisa%' THEN 'advisa' ELSE 'sambla' END AS brand, 
        "mongodb" AS source_database
    FROM `${project_id}.${dataset_id}.applications_gcs_streaming` a
    WHERE operationType != 'delete'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a._id 
        ORDER BY __v DESC, IFNULL(updatedAt, '1990-01-01') DESC, IFNULL(a.time_archived, '1990-01-01') DESC
    ) = 1
),

applications_sambq_r AS (
    SELECT 
        b.*, 
        CASE 
            WHEN array_length(l.loans) > array_length(b.loans) THEN l.loans 
            ELSE b.loans 
        END AS loans
    FROM base_data b
    LEFT JOIN largest_loan_arrays l ON b._id = l._id
)

select 
r.*, 
a._id as application_id,
a.time_archived,
a.__v as versions,
a.updatedAt as updated_at,
a.status.sent as is_status_sent 
from applications_sambq_r  a, 
unnest(a.loans) r

-- where  timestamp_trunc(time_archived,day) in (timestamp(current_date),timestamp(date_sub(current_date, interval 1 day)),timestamp(date_sub(current_date, interval 2 day)),timestamp(date_sub(current_date, interval 3 day)))
