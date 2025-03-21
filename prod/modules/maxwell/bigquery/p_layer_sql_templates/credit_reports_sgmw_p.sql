select
CAST(JSON_VALUE(data,'$.template_code') as STRING) AS template_code,
CAST(JSON_VALUE(data,'$.previous_taxed_annual_income') as INT64) AS previous_taxed_annual_income,
CAST(JSON_VALUE(data,'$.debt_balance') as STRING) AS debt_balance,
CAST(JSON_VALUE(data,'$.country') as STRING) AS country,
CAST(JSON_VALUE(data,'$.national_id') as STRING) AS national_id,
CAST(JSON_VALUE(data,'$.applicant_draft_id') as INT64) AS applicant_draft_id,
CAST(JSON_VALUE(data,'$.has_debt_reconstruction') as STRING) AS has_debt_reconstruction,
CAST(JSON_VALUE(data,'$.postal_code') as STRING) AS postal_code,
CAST(JSON_VALUE(data,'$.minor') as STRING) AS minor,
CAST(JSON_VALUE(data,'$.previous_capital_deficit') as INT64) AS previous_capital_deficit,
CAST(JSON_VALUE(data,'$.taxed_annual_income') as INT64) AS taxed_annual_income,
CAST(JSON_VALUE(data,'$.payment_complaint') as STRING) AS payment_complaint,
CAST(JSON_VALUE(data,'$.uc_registration_date') as TIMESTAMP) AS uc_registration_date,
CAST(JSON_VALUE(data,'$.debt_reconciliation_date') as TIMESTAMP) AS debt_reconciliation_date,
CAST(JSON_VALUE(data,'$.secret_address') as STRING) AS secret_address,
CAST(JSON_VALUE(data,'$.template_passed') as STRING) AS template_passed,
CAST(case when JSON_VALUE(data,'$.temporary_address')='0' then 0 else 1 end as STRING) AS temporary_address,
CAST(JSON_VALUE(data,'$.capital_deficit') as INT64) AS capital_deficit,
CAST(JSON_VALUE(data,'$.customer_id') as INT64) AS customer_id,
CAST(JSON_VALUE(data,'$.street_address') as STRING) AS street_address,
CAST(JSON_VALUE(data,'$.number_of_payment_complaints') as INT64) AS number_of_payment_complaints,
CAST(JSON_VALUE(data,'$.city') as STRING) AS city,
CAST(JSON_VALUE(data,'$.product_type') as INT64) AS product_type,
CAST(JSON_VALUE(data,'$.xml') as STRING) AS xml,
CAST(JSON_VALUE(data,'$.first_name') as STRING) AS first_name,
CAST(JSON_VALUE(data,'$.fetched_at') as TIMESTAMP) AS fetched_at,
CAST(JSON_VALUE(data,'$.id') as INT64) AS id,
CAST(JSON_VALUE(data,'$.latest_payment_complaint') as TIMESTAMP) AS latest_payment_complaint,
CAST(JSON_VALUE(data,'$.major_city_inhabitant') as STRING) AS major_city_inhabitant,
CAST(JSON_VALUE(data,'$.last_name') as STRING) AS last_name,
CAST(JSON_VALUE(data,'$.no_visit_address') as STRING) AS no_visit_address,
CAST(JSON_VALUE(data,'$.blocked_code') as STRING) AS blocked_code,
CAST(JSON_VALUE(data,'$.unregistered') as STRING) AS unregistered,
CAST(JSON_VALUE(data,'$.blocked_description') as STRING) AS blocked_description,
CAST(JSON_VALUE(data,'$.number_of_reports_12_months') as INT64) AS number_of_reports_12_months,
CAST(JSON_VALUE(data,'$.taxed_year') as TIMESTAMP) AS taxed_year,
CAST(JSON_VALUE(data,'$.previous_taxed_year') as TIMESTAMP) AS previous_taxed_year,
CAST(JSON_VALUE(data,'$.score') as decimal) AS score,
ts,
DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts,
ROW_NUMBER() over(partition by xid,xoffset,ts order by xid desc, source desc) rn,
source,
xid,
xoffset,
-- CAST(JSON_VALUE(data,'$.applicant_draft_id') as INT64) || '|' || CAST(JSON_VALUE(data,'$.id') as INT64) as credit_report_id,
from `${project_id}.${dataset_id}.event_data_sgmw_r`
where table = 'credit_reports'
QUALIFY rn = 1