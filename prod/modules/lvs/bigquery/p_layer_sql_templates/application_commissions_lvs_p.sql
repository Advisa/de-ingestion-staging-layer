select 
cast(a.data_id as string) data_id,
timestamp(a.application_created_at) as application_created_at,
date(c.effective_date) as effective_date,
c.value as commission_value,
c.bank_id as commission_bank_id,
c.type as commission_type
from 
`sambla-data-staging-compliance.lvs_integration_legacy.applications_lvs_r` a
,unnest(a.commissions) c
where true
