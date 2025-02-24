with inv_logs as (select
CAST(JSON_VALUE(data,'$.updated_at') as TIMESTAMP) AS updated_at,
CAST(JSON_VALUE(data,'$.comment') as STRING) AS comment,
CAST(JSON_VALUE(data,'$.user_id') as INT64) AS user_id,
CAST(JSON_VALUE(data,'$.action') as STRING) AS action,
CAST(JSON_VALUE(data,'$.system') as STRING) AS system,
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
CAST(JSON_VALUE(data,'$.invite_id') as INT64) AS invite_id,
CAST(JSON_VALUE(data,'$.created_at') as TIMESTAMP) AS created_at,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
ROW_NUMBER() over(partition by xid,xoffset,ts order by xid desc, source desc) rn,
source,
xid,
xoffset,
from `${project_id}.${dataset_id}.event_data_sgmw_r`
where table='invite_logs'
QUALIFY rn = 1)

select * from inv_logs where date(created_at) >='2021-01-01'