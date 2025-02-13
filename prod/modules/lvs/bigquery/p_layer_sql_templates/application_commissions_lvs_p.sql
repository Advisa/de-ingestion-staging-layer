select 
cast(a.data_id as string) data_id,
date(c.effective_date) as effective_date,
c.value as commission_value,
c.bank_id as commission_bank_id,
c.type as commission_type
from 
`${project_id}.${dataset_id}.applications_lvs_r` a
,unnest(a.commissions) c
where true
