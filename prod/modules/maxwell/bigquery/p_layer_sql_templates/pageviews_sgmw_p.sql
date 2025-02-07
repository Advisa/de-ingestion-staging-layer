select
CAST(JSON_VALUE(data,'$.ip') as STRING) AS ip,
CAST(JSON_VALUE(data,'$.edge_request_id') as STRING) AS edge_request_id,
CAST(JSON_VALUE(data,'$.request_date') as TIMESTAMP) AS request_date,
CAST(JSON_VALUE(data,'$.user_agent') as STRING) AS user_agent,
CAST(JSON_VALUE(data,'$.url') as STRING) AS url,
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
CAST(JSON_VALUE(data,'$.cookie_id') as STRING) AS cookie_id,
CAST(JSON_VALUE(data,'$.host') as STRING) AS host,
CAST(JSON_VALUE(data,'$.referer') as STRING) AS referer,
CAST(JSON_VALUE(data,'$.created_at') as TIMESTAMP) AS created_at,
CAST(case when JSON_VALUE(data,'$.dummy')='0' then 0 else 1 end as BOOLEAN) AS dummy,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
ROW_NUMBER() over(partition by xid,xoffset,ts order by xid desc) rn,
source,
xid,
xoffset,
from {{ref('event_data_sgmw_r')}}
where table = 'pageviews'
QUALIFY rn = 1