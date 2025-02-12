with bid_logs as (
select
--CONCAT('CAST(JSON_VALUE(DATA,''$.' , __COLUMN , ''') AS ' , 
--IFNULL(REPLACE(REPLACE(__DATATYPE,'VARCHAR','STRING'),'INTEGER','INT64'),'STRING') , ') AS ',__COLUMN  ,',')
CAST(JSON_VALUE(data,'$.reason') as STRING) AS reason,
CAST(JSON_VALUE(data,'$.bid_id') as INT64) AS bid_id,
CAST(JSON_VALUE(data,'$.comment') as STRING) AS comment,
CAST(JSON_VALUE(data,'$.created_at') as TIMESTAMP) AS created_at,
CAST(JSON_VALUE(data,'$.system') as STRING) AS system,
CAST(JSON_VALUE(data,'$.updated_at') as TIMESTAMP) AS updated_at,
CAST(JSON_VALUE(data,'$.user_id') as INT64) AS user_id,
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
CAST(JSON_VALUE(data,'$.action') as STRING) AS action,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
source,
xid,
xoffset,
from `${project_id}.${dataset_id}.event_data_sgmw_r`
where table='bid_logs'
QUALIFY ROW_NUMBER() over(partition by xid,xoffset,ts order by xid desc) = 1
)

select * from bid_logs where date(created_at) >='2021-01-01'