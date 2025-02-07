select 
CAST(JSON_VALUE(data,'$.data') as STRING) AS data,
CAST(JSON_VALUE(data,'$.external_tracking_id') as STRING) AS external_tracking_id,
CAST(JSON_VALUE(data,'$.event') as STRING) AS event,
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
CAST(JSON_VALUE(data,'$.created_at') as TIMESTAMP) AS created_at,
CAST(JSON_VALUE(data,'$.affiliate') as STRING) AS affiliate,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
ROW_NUMBER() over(partition by xid,xoffset,ts order by xid desc) rn,
source,
xid,
xoffset,
from {{ref('event_data_sgmw_r')}}
where table='sent_events'
QUALIFY rn = 1