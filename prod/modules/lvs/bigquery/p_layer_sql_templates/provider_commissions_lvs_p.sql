select 
c.*
FROM `${project_id}.${dataset_id}.providers_lvs_r` p
left join unnest(p.commissions) c