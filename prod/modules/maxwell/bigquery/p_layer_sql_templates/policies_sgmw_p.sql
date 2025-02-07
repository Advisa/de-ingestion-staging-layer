select
CAST(JSON_VALUE(data,'$.deactivated_at') as TIMESTAMP) AS deactivated_at,
CAST(JSON_VALUE(data,'$.created_at') as TIMESTAMP) AS created_at,
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
CAST(JSON_VALUE(data,'$.national_id') as STRING) AS national_id,
CAST(JSON_VALUE(data,'$.updated_at') as TIMESTAMP) AS updated_at,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
ROW_NUMBER() over(partition by xid,xoffset,ts order by xid desc) rn,
source,
xid,
xoffset,
from {{ref('event_data_sgmw_r')}}
where table = 'policies'
QUALIFY rn = 1