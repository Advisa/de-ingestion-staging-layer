select
CAST(case when  JSON_VALUE(data,'$.canceled')='0' OR JSON_VALUE(data,'$.canceled') IS NULL then 0 else 1 end  as BOOLEAN) AS canceled,
CAST(JSON_VALUE(data,'$.created_at') as TIMESTAMP) AS created_at,
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
CAST(case when JSON_VALUE(data,'$.self_service')='0' then 0 else 1 end as BOOLEAN) AS self_service,
CAST(JSON_VALUE(data,'$.application_type') as INT64) AS application_type,
CAST(JSON_VALUE(data,'$.brands') as STRING) AS brands,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
ROW_NUMBER() over(partition by xid,xoffset,ts order by source desc) rn,
source,
xid,
xoffset,
from `${project_id}.${dataset_id}.event_data_sgmw_r`
where table='loan_applications'
QUALIFY rn = 1