with bids as (
select
CONCAT(xid,xoffset,ts) natural_key,
CAST(JSON_VALUE(data,'$.interest') as DECIMAL) AS interest,
CAST(JSON_VALUE(data,'$.arrangement_fee') as DECIMAL) AS arrangement_fee,
CAST(JSON_VALUE(data,'$.creditor_signing_url') as STRING) AS creditor_signing_url,
CAST(JSON_VALUE(data,'$.expires_at') as TIMESTAMP) AS expires_at,
CAST(JSON_VALUE(data,'$.amount') as DECIMAL) AS amount,
CAST(JSON_VALUE(data,'$.debt_letter_delivery_method') as INT64) AS debt_letter_delivery_method,
CAST(JSON_VALUE(data,'$.accepted_amount') as DECIMAL) AS accepted_amount,
CAST(JSON_VALUE(data,'$.repayment_type') as STRING) AS repayment_type,
CAST(CASE WHEN JSON_VALUE(data,'$.debt_letter_received') = '1' THEN true ELSE false END as BOOLEAN) AS debt_letter_received,
CAST(JSON_VALUE(data,'$.invite_id') as INT64) AS invite_id,
CAST(JSON_VALUE(data,'$.created_at') as TIMESTAMP) AS created_at,
CAST(case when JSON_VALUE(data,'$.require_consolidation_specification')='1' then true else false end as BOOLEAN) AS require_consolidation_specification,
CAST(case when JSON_VALUE(data,'$.debt_letter_sent')='1' THEN true else false end as BOOLEAN) AS debt_letter_sent,
CAST(JSON_VALUE(data,'$.updated_at') as TIMESTAMP) AS updated_at,
CAST(JSON_VALUE(data,'$.repayment_time') as DECIMAL) AS repayment_time,
CAST(JSON_VALUE(data,'$.administration_fee') as DECIMAL) AS administration_fee,
CAST(JSON_VALUE(data,'$.apr') as DECIMAL) AS apr,
CAST(JSON_VALUE(data,'$.monthly_payment') as DECIMAL) AS monthly_payment,
CAST(JSON_VALUE(data,'$.paid_at') as TIMESTAMP) AS paid_at,
CAST(JSON_VALUE(data,'$.minimum_amount_to_resolve') as DECIMAL) AS minimum_amount_to_resolve,
CAST(CASE WHEN JSON_VALUE(data,'$.accepted') = '1' THEN true ELSE false END as BOOLEAN) AS accepted,
CAST(JSON_VALUE(data,'$.paid_amount') as DECIMAL) AS paid_amount,
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
ROW_NUMBER() over(partition by xid,xoffset,ts order by xid desc) rn,
source,
xid,
xoffset,
from {{ref('event_data_sgmw_r')}}
where table='bids'
QUALIFY rn = 1
)

select * from bids where date(created_at) >='2021-01-01'