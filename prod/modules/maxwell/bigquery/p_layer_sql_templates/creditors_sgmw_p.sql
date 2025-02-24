select
CAST(JSON_VALUE(data,'$.deactivated_at') as STRING) AS deactivated_at,
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
CAST(JSON_VALUE(data,'$.updated_at') as TIMESTAMP) AS updated_at,
CAST(JSON_VALUE(data,'$.name') as STRING) AS name,
CAST(JSON_VALUE(data,'$.created_at') as TIMESTAMP) AS created_at,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
ROW_NUMBER() over(partition by xid,xoffset,ts order by source desc) rn,
source,
xid,
xoffset,
from `${project_id}.${dataset_id}.event_data_sgmw_r`
where table='creditors'
QUALIFY rn = 1