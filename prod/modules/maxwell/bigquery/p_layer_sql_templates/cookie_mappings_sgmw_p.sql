select 
CAST(JSON_VALUE(data,'$.application_id') as INT64) AS application_id,
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
CAST(JSON_VALUE(data,'$.cookie_id') as INT64) AS cookie_id,
CAST(JSON_VALUE(data,'$.created_at') as TIMESTAMP) AS created_at ,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
ROW_NUMBER() over(partition by xid,xoffset,ts order by xid desc) rn,
source,
xid,
xoffset,
from {{ref('event_data_sgmw_r')}}
where table='application_cookie_mappings'
QUALIFY rn = 1