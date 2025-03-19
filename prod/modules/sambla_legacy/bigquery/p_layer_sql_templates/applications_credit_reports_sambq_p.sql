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

creditReports AS (
    SELECT 
      alv.status.sent as is_status_sent,
      alv._id || '|' || alv.person._id as customer_id,
      alv.person.idNumber,
      alv.market,
      alv._id || '|' || cr._id as credit_report_id,
      cr._id,
      cr.type,
      cr.createdAt,
      cr.taxableIncomeYear,
      cr.taxableIncome,
      cr.creditScore,
      cr.registeredAt,
      cr.ucRegisteredAt,
      cr.numberOfRecords,
      cr.capitalDeficit,
      cr.ucBlockCode,
      cr.lastYearTaxInfo.taxableIncome as lastYeartaxableIncome,
      cr.lastYearTaxInfo.capitalDeficit as lastYearcapitalDeficit,
      cr.lastYearTaxInfo.taxableIncomeYear as lastYeartaxableIncomeYear,
      cr.latestRecordDetails.type as latest_record_type,
      cr.latestRecordDetails.claimant,
      cr.latestRecordDetails.date as latestRecordDetails_date,
      cr.latestRecordDetails.amount,
      alv.time_archived,
      alv.__v as versions,
      alv.updatedAt as updated_at,
      alv._id as application_id,
      alv.createdat as application_created_at,
      cr.currentdebt.hasdebtreconstruction,
      cr.currentdebt.hasDebtBalance
    FROM
     applications_sambq_r alv,
     UNNEST(alv.person.creditReports) AS cr
    
      --where  timestamp_trunc(time_archived,day) in (timestamp(current_date),timestamp(date_sub(current_date, interval 1 day)),timestamp(date_sub(current_date, interval 2 day)),timestamp(date_sub(current_date, interval 3 day)))
     
    UNION ALL 
    
     

    SELECT 
      alv.status.sent as is_status_sent,
      alv._id || '|' || alv.partner._id as customer_id,
      alv.partner.idNumber,
      alv.market,
      alv._id || '|' || cr._id as credit_report_id,
      cr._id,
      cr.type,
      cr.createdAt,
      cr.taxableIncomeYear,
      cr.taxableIncome,
      cr.creditScore,
      cr.registeredAt,
      cr.ucRegisteredAt,
      cr.numberOfRecords,
      cr.capitalDeficit,
      cr.ucBlockCode,
      cr.lastYearTaxInfo.taxableIncome as lastYeartaxableIncome,
      cr.lastYearTaxInfo.capitalDeficit as lastYearcapitalDeficit,
      cr.lastYearTaxInfo.taxableIncomeYear as lastYeartaxableIncomeYear,
      cr.latestRecordDetails.type as latest_record_type,
      cr.latestRecordDetails.claimant,
      cr.latestRecordDetails.date as latestRecordDetails_date,
      cr.latestRecordDetails.amount,
      alv.time_archived,
      alv.__v as versions,
      alv.updatedAt as updated_at,
      alv._id as application_id,
      alv.createdat as application_created_at,
      cr.currentdebt.hasdebtreconstruction,
      cr.currentdebt.hasDebtBalance
    
    FROM
     applications_sambq_r alv,
     UNNEST(alv.partner.creditReports) AS cr
      --where  timestamp_trunc(time_archived,day) in (timestamp(current_date),timestamp(date_sub(current_date, interval 1 day)),timestamp(date_sub(current_date, interval 2 day)),timestamp(date_sub(current_date, interval 3 day)))  
      
  )

SELECT
  *
FROM
  creditReports