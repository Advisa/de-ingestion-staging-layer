with bids as (
select
CAST(JSON_VALUE(data,'$.bank_account') as STRING) AS bank_account,
CAST(JSON_VALUE(data,'$.bid_id') as INT64) AS bid_id,
CAST(JSON_VALUE(data,'$.created_at') as TIMESTAMP) AS created_at,
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
CAST(JSON_VALUE(data,'$.updated_at') as TIMESTAMP) AS updated_at,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
source,
xid,
xoffset,
from `${project_id}.${dataset_id}.event_data_sgmw_r`
where table='bid_accepts'
QUALIFY ROW_NUMBER() over(partition by xid,xoffset,ts order by xid desc) = 1
)

select * from bids 
where date(created_at) >='2021-01-01'