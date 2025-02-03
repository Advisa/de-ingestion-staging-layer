with unsubscriptions_sambq_r as (
    select * from `${project_id}.${dataset_id}.unsubscriptions_gcs_streaming` 
WHERE operationType != 'delete' and date(createdAt)< '2022-01-01'
QUALIFY ROW_NUMBER() OVER(PARTITION BY _id ORDER BY __v desc, time_archived DESC)=1
)

select *
 from unsubscriptions_sambq_r a
--where  timestamp_trunc(time_archived,day) in (timestamp(current_date),timestamp(date_sub(current_date, interval 1 day)),timestamp(date_sub(current_date, interval 2 day)),timestamp(date_sub(current_date, interval 3 day)))
