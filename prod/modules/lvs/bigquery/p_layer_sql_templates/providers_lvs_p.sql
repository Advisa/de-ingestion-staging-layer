select 
'approved' in unnest(cpl_rules) as cpl_approved,
'APIapproved' in unnest(cpl_rules) as cpl_api_approved,
'paid' in unnest(cpl_rules) as cpl_paid,
* except(cpl_rules,commissions) 
FROM `sambla-data-staging-compliance.lvs_integration_legacy.providers_lvs_r`