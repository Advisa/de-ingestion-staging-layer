with raw_data AS (
    SELECT 
        a.*except(last_check),
        cast(last_check as timestamp) as last_check
    FROM `${project_id}.${dataset_id}.credit_remarks_lvs_r` a
)
select 
ssn_id,
name,
bisnode_id,
payment_remark_latest,
payment_remarks_count,
other_remarks_count,
is_under_guardianship,
is_banned_on_business_operations,
last_check
from raw_data c
where true