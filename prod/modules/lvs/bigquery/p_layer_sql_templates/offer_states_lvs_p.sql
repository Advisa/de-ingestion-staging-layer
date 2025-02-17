with raw_data as (
    select * except(api_approved_datetime,approved_datetime,paid_datetime),
    safe_cast(api_approved_datetime as timestamp) as api_approved_datetime,
    safe_cast(approved_datetime as timestamp) as approved_datetime,
    safe_cast(paid_datetime as timestamp) as paid_datetime, 
    FROM `${project_id}.${dataset_id}.offers_lvs_r`
),

main AS (

select *,
coalesce(paid_datetime,approved_datetime,api_approved_datetime) as incremental_datetime
from raw_data 
)

select distinct
cast(o.data_id as string) data_id,
o.incremental_datetime,
os.* except(data_id)
from 
main o
left join unnest(o.states) os 