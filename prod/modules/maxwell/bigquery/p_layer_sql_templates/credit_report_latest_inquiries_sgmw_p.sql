select
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
CAST(JSON_VALUE(data,'$.name') as STRING) AS name,
CAST(JSON_VALUE(data,'$.date') as TIMESTAMP) AS date,
CAST(JSON_VALUE(data,'$.credit_report_id') as INT64) AS credit_report_id,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
source,
xid,
xoffset,
from `${project_id}.${dataset_id}.event_data_sgmw_r`
where table = 'credit_report_latest_inquiries'
QUALIFY ROW_NUMBER() over(partition by xid,xoffset,ts order by source desc) = 1

