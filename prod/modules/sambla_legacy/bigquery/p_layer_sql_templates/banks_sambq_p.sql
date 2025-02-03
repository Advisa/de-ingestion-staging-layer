with banks_sambq_r as (
    select * from `${project_id}.${dataset_id}.banks_gcs_streaming` WHERE operationType != 'delete'
)

select *
from banks_sambq_r 
--where  timestamp_trunc(time_archived,day) in (timestamp(current_date),timestamp(date_sub(current_date, interval 1 day)),timestamp(date_sub(current_date, interval 2 day)),timestamp(date_sub(current_date, interval 3 day)))
