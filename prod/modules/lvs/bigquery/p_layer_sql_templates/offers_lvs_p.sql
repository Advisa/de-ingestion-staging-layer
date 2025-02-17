with raw_data as (
    select * except(api_approved_datetime,approved_datetime,paid_datetime),
    safe_cast(api_approved_datetime as timestamp) as api_approved_datetime,
    safe_cast(approved_datetime as timestamp) as approved_datetime,
    safe_cast(paid_datetime as timestamp) as paid_datetime, 
    FROM `${project_id}.${dataset_id}.offers_lvs_r`
),

main AS (

select *,
coalesce(paid_datetime,approved_datetime,api_approved_datetime) as incremental_datetime
from raw_data 
)

select distinct
cast(o.data_id as string) data_id,
o.incremental_datetime,
o.api_approved_datetime,
o.application,
o.user_selected_datetime,
o.approved,
o.api_approved,
o.badges,
o.esign_link,
cast(o.bank_id as string) as bank_id,
o.is_new_for_provider,
o.paid,
o.apr,
o.approved_amount,
o.approved_datetime,
o.interest_rate,
o.administration_fee,
o.opening_fee,
o.status,
o.monthly_payment,
o.repayment_amount,
o.repayment_time,
o.esign_info,
o.consolidation_amount,
o.topup_amount,
o.esign_txt,
o.paid_datetime
from 
main o
where true