select distinct
cast(o.data_id as string) data_id,
o.incremental_datetime,
os.* except(data_id)
from 
`sambla-data-staging-compliance.lvs_integration_legacy.offers_lvs_r` o
left join unnest(o.states) os 