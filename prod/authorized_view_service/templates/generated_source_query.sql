SELECT * FROM `sambla-data-staging-compliance.maxwell_integration_legacy`.INFORMATION_SCHEMA.COLUMNS
UNION ALL 
SELECT * FROM `sambla-data-staging-compliance.salus_integration_legacy`.INFORMATION_SCHEMA.COLUMNS
UNION ALL 
SELECT * FROM `sambla-data-staging-compliance.lvs_integration_legacy`.INFORMATION_SCHEMA.COLUMNS
UNION ALL 
SELECT * FROM `sambla-data-staging-compliance.rahalaitos_integration_legacy`.INFORMATION_SCHEMA.COLUMNS
UNION ALL 
SELECT * FROM `sambla-data-staging-compliance.sambla_legacy_integration_legacy`.INFORMATION_SCHEMA.COLUMNS
UNION ALL 
SELECT * FROM `sambla-data-staging-compliance.advisa_history_integration_legacy`.INFORMATION_SCHEMA.COLUMNS