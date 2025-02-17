select 
cast(c.data_id as string) data_id,
timestamp(c.created_at) as applicant_created_at,
c.id as applicant_id,
cc.id as card_id,
timestamp(cc.created_at) as card_created_at,
cc.issuer as card_issuer,
cc.card_type as card_type,
cc.debt as card_debt,
cc.limit as card_limit,
from 
`${project_id}.${dataset_id}.applicants_lvs_r` c
left join unnest(c.cards) cc on c.id = cc.applicant_id
