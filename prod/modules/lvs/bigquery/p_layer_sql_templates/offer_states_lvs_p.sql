select distinct
cast(o.data_id as string) data_id,
os.* except(data_id)
from 
`${project_id}.${dataset_id}.offers_lvs_r` o
left join unnest(o.states) os 