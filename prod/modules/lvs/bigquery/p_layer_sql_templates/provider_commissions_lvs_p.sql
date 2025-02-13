select 
c.*
FROM `sambla-data-staging-compliance.lvs_integration_legacy.providers_lvs_r` p
left join unnest(p.commissions) c