select 
cast(c.data_id as string) data_id,
safe_cast(c.created_at as timestamp) as applicant_created_at,
c.id as applicant_id,
c.is_main_applicant,
c.first_name,
c.last_name,
c.gender,
c.dob,
c.age,
c.phone,
c.email,
c.ssn,
c.marital_status,
c.dependents,
c.city,
c.post_code,
c.address,
c.education,
c.citizenship,
c.has_summer_house,
c.is_pep,
c.military,
c.adults,
c.bank_bic,
c.bank_iban,
c.has_credit_remarks,
c.is_anonymized,
c.spouse.id as spouse_id,
timestamp(c.spouse.created_at) as spouse_created_at,
c.spouse.monthly_expenses as spouse_monthly_expenses,
c.spouse.salary_monthly_net as spouse_salary_monthly_net,
c.spouse.salary_monthly_gross as spouse_salary_monthly_gross,
ch.id as housing_id,
timestamp(ch.created_at) as housing_created_at,
ch.moving_date as housing_moving_date,
ch.property_type as housing_property_type,
ch.property_area as housing_property_area,
ch.residency_type as housing_residency_type,
ce.id as employment_id,
timestamp(ce.created_at) as employment_created_at,
ce.profession_area as employment_area,
ce.profession as employment_profession,
ce.profession_class employment_profession_class,
ce.is_current as is_current_employment,
ce.start_date as employment_start_date,
ce.business_id as employment_business_id,
ce.occupation as employment_occupation,
ce.employer as employer,
ce.end_date as employment_end_date,
from 
`${project_id}.${dataset_id}.applicants_lvs_r` c
left join unnest(c.housings) ch on c.id = ch.applicant_id
left join unnest(c.employments) ce on c.id = ce.applicant_id
where true
