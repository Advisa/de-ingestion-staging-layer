select
CAST(JSON_VALUE(data,'$.created_at') as TIMESTAMP) AS created_at,
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
CAST(JSON_VALUE(data,'$.internal_id') as STRING) AS internal_id,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
ROW_NUMBER() over(partition by xid,xoffset,ts order by xid desc, source desc) rn,
source,
xid,
xoffset,
from `${project_id}.${dataset_id}.event_data_sgmw_r`
where table='cookies'
QUALIFY rn = 1