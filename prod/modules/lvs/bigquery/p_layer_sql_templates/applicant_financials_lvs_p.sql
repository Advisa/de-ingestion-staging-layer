select 
cast(c.data_id as string) data_id,
timestamp(c.created_at) as applicant_created_at,
c.id as applicant_id,
cf.id as financial_id,
timestamp(cf.created_at) as financial_created_at,
cf.description as  financial_description,
cf.issuer as  financial_issuer,
cf.amount_monthly as  financial_amount_monthly,
cf.money_type as  financial_money_type,
cf.consolidation as  financial_consolidation,
cf.amount as  financial_amount,
cf.finance_source as  financial_finance_source,
cf.finance_type as  financial_finance_type
from 
`sambla-data-staging-compliance.lvs_integration_legacy.applicants_lvs_r` c
left join unnest(c.financials) cf on c.id = cf.applicant_id
where true
