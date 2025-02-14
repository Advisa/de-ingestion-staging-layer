with raw_data AS (
select a.* except(timestamp),
a.timestamp as application_created_at
FROM `${project_id}.${dataset_id}.applications_lvs_r` a
)

select 
cast(a.data_id as string) data_id,
timestamp(a.application_created_at) as application_created_at,
date(c.effective_date) as effective_date,
c.value as commission_value,
c.bank_id as commission_bank_id,
c.type as commission_type
from 
raw_data a
,unnest(a.commissions) c
where true
