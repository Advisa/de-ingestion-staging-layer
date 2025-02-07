select
CAST(JSON_VALUE(data,'$.updated_at') as TIMESTAMP) AS updated_at,
CAST(JSON_VALUE(data,'$.created_at') as TIMESTAMP) AS created_at,
CAST(JSON_VALUE(data,'$.type') as INT64) AS type,
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
CAST(JSON_VALUE(data,'$.applicant_draft_id') as INT64) AS applicant_draft_id,
CAST(JSON_VALUE(data,'$.amount') as INT64) AS amount,
CAST(JSON_VALUE(data,'$.interest') as NUMERIC) AS interest,
CAST(JSON_VALUE(data,'$.administration_fee') as NUMERIC) AS administration_fee,
CAST(JSON_VALUE(data,'$.insurance_cost') as NUMERIC) AS insurance_cost,
CAST(JSON_VALUE(data,'$.monthly_payment') as NUMERIC) AS monthly_payment,
CAST(case when JSON_VALUE(data,'$.transfer')='0' then 0 else 1 end as BOOLEAN) AS transfer,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
ROW_NUMBER() over(partition by xid,xoffset,ts order by xid desc) rn,
source,
xid,
xoffset,
from {{ref('event_data_sgmw_r')}}
where table='current_loan_drafts'
QUALIFY rn = 1