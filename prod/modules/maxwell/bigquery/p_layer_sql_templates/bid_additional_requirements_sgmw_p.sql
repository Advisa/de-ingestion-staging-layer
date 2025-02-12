with bid_additional_requirements as (
select
CAST(JSON_VALUE(data,'$.applies_to') as INT64) AS applies_to,
CAST(JSON_VALUE(data,'$.bid_id') as INT64) AS bid_id,
CAST(JSON_VALUE(data,'$.count') as INT64) AS count,
CAST(JSON_VALUE(data,'$.created_at') as TIMESTAMP) AS created_at,
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
CAST(JSON_VALUE(data,'$.max_age') as INT64) AS max_age,
CAST(JSON_VALUE(data,'$.required_when') as INT64) AS required_when,
CAST(JSON_VALUE(data,'$.requirement_type') as STRING) AS requirement_type,
CAST(JSON_VALUE(data,'$.requirement_value') as  STRING) AS requirement_value,
CAST(JSON_VALUE(data,'$.updated_at') as TIMESTAMP) AS updated_at,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
ROW_NUMBER() over(partition by xid,xoffset,ts order by xid desc) rn,
source,
xid,
xoffset,
from `${project_id}.${dataset_id}.event_data_sgmw_r`
where table='bid_additional_requirements'
QUALIFY rn = 1
)

select * from bid_additional_requirements where date(created_at) >='2021-01-01'