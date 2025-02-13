select 
cast(c.data_id as string) data_id,
timestamp(c.created_at) as applicant_created_at,
c.id as applicant_id,
cco.id as consent_id,
timestamp(cco.created_at) as consent_created_at,
cco.accepted_datetime as consent_accepted_at,
cco.consent_type as consent_type
from 
`sambla-data-staging-compliance.lvs_integration_legacy.applicants_lvs_r` c
left join unnest(c.consents) cco on c.id = cco.applicant_id 
where true