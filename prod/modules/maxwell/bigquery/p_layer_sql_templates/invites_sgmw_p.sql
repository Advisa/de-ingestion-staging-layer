with invites as 
(select
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
CAST(JSON_VALUE(data,'$.updated_at') as TIMESTAMP) AS updated_at,
CAST(JSON_VALUE(data,'$.creditor_product_id') as INT64) AS creditor_product_id,
CAST(JSON_VALUE(data,'$.application_id') as INT64) AS application_id,
CAST(JSON_VALUE(data,'$.creditor_application_id') as STRING) AS creditor_application_id,
CAST(JSON_VALUE(data,'$.deadline') as STRING) AS deadline,
CAST(JSON_VALUE(data,'$.created_at') as TIMESTAMP) AS created_at,
CAST(JSON_VALUE(data,'$.creditor_data') as STRING) AS creditor_data,
CAST(CASE WHEN JSON_VALUE(data,'$.rejected') = '1' THEN true ELSE false END as BOOLEAN) AS rejected,
CAST(CASE WHEN JSON_VALUE(data,'$.declined') = '1' THEN true ELSE false END as BOOLEAN) AS declined,
CAST(JSON_VALUE(data,'$.application_version_id') as INT64) AS application_version_id,
CAST(JSON_VALUE(data,'$.creditor_id') as INT64) AS creditor_id,
CAST(JSON_VALUE(data,'$.co_applicant_removed') as INT64) AS co_applicant_removed,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
ROW_NUMBER() over(partition by xid,xoffset,ts order by xid desc) rn,
source,
xid,
xoffset,
from {{ref('event_data_sgmw_r')}}
where table = 'invites'
QUALIFY rn = 1)

select * from invites where date(created_at) >='2021-01-01'