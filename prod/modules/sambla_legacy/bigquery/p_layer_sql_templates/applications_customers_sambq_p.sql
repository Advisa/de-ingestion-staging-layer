WITH largest_loan_arrays AS (
    SELECT loans, _id 
    FROM `${project_id}.${dataset_id}.applications_gcs_streaming`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY array_length(loans) DESC) = 1
),

base_data AS (
    SELECT 
        a.*, 
        a._id AS application_id, 
        --a.time_archived, 
        a.__v AS versions, 
        a.updatedAt AS updated_at, 
        a.status.sent AS is_status_sent, 
        CASE WHEN utm.domain LIKE '%advisa%' THEN 'advisa' ELSE 'sambla' END AS brand, 
        "mongodb" AS source_database
    FROM `${project_id}.${dataset_id}.applications_gcs_streaming` a
    WHERE operationType != 'delete'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a._id 
        ORDER BY __v DESC, IFNULL(updatedAt, '1990-01-01') DESC, IFNULL(a.time_archived, '1990-01-01') DESC
    ) = 1
),

applications_sambq_r AS (
    SELECT 
        b.*, 
        CASE 
            WHEN array_length(l.loans) > array_length(b.loans) THEN l.loans 
            ELSE b.loans 
        END AS loans
    FROM base_data b
    LEFT JOIN largest_loan_arrays l ON b._id = l._id),
    
    
source as (
SELECT
    status.sent as is_status_sent,
    time_archived,
    __v as versions,
    updatedAt as updated_at,
    _id AS application_id,
    -- idnumber is national_id for person
    _id || '|' || ifnull(person._id, person.idnumber) as application_applicant_id,
    createdAt,
    createdAt as valid_from,
    person._id AS applicant_id,
    person.first_name,
    person.last_name,
    person.age,                      
    person.citizenship,
    person.postalArea,
    person.address,
    person.occupation,
    person.civilStatus,
    person.education,
    person.email,       
    person.idNumber,
    person.employerPhone,
    person.employer,
    company.organizationNumber AS business_organization_number,
    company.industryType AS employment_industry_text,
    company.foundationDate AS business_registration_date,
    person.phoneNumber,
    person.militaryService,
    person.employedSince,
    person.employedUntil,
    person.workStatus,
    person.workStatusSince,
    person.livingCost,
    person.rent,
    person.homeStatus,
    person.homeSince,
    person.monthlyIncome,
    person.monthlyNetIncome,
    person.spouseIncome,
    person.politicallyExposedPerson,
    person.postalCode,
    person.gender,
    person.children,
    'person' as applicant_type,
    person.hasacceptedcreditreport ,
    person.jojkaBlocklist,
    islead,
    market,
    brand,
    bankAccount.accountNumber,
    bankAccount.yearsWithBank,
    bankAccount.sinceYear,
    bankAccount.clearingNumber,
    bankAccount.bankName, 
    person.extraIncome
from applications_sambq_r 
where True
--and timestamp_trunc(time_archived,day) in (timestamp(current_date),timestamp(date_sub(current_date, interval 1 day)),timestamp(date_sub(current_date, interval 2 day)),timestamp(date_sub(current_date, interval 3 day)))

    UNION ALL 

SELECT
    status.sent as is_status_sent,
    time_archived,
    __v as versions,
    updatedAt as updated_at,
    _id AS application_id,
    -- idnumber is national_id for person
    _id || '|' || ifnull(partner._id, partner.idnumber) as application_applicant_id,
    createdAt,
    createdAt as valid_from,
    partner._id AS applicant_id,
    partner.first_name,
    partner.last_name,
    partner.age,                      
    partner.citizenship,
    partner.postalArea,
    partner.address,
    partner.occupation,
    partner.civilStatus,
    partner.education,
    partner.email,       
    partner.idNumber,
    partner.employerPhone,
    partner.employer,
    company.organizationNumber AS business_organization_number,
    company.industryType AS employment_industry_text,
    company.foundationDate AS business_registration_date,
    partner.phoneNumber,
    partner.militaryService,
    partner.employedSince,
    partner.employedUntil,
    partner.workStatus,
    partner.workStatusSince,
    partner.livingCost,
    partner.rent,
    partner.homeStatus,
    partner.homeSince,
    partner.monthlyIncome,
    partner.monthlyNetIncome,
    partner.spouseIncome,
    partner.politicallyExposedPerson,
    partner.postalCode,
    partner.gender,
    partner.children,
    'partner' as applicant_type,
    partner.hasacceptedcreditreport ,
    partner.jojkaBlocklist,
    islead,
    market,
    brand,
    bankAccount.accountNumber,
    bankAccount.yearsWithBank,
    bankAccount.sinceYear,
    bankAccount.clearingNumber,
    bankAccount.bankName,
    partner.extraIncome
from applications_sambq_r
where True
--and timestamp_trunc(time_archived,day) in (timestamp(current_date),timestamp(date_sub(current_date, interval 1 day)),timestamp(date_sub(current_date, interval 2 day)),timestamp(date_sub(current_date, interval 3 day)))
and has_co_applicant 
)

select * EXCEPT(application_id)
from source