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
    LEFT JOIN largest_loan_arrays l ON b._id = l._id
),

rectified_accepted as (
SELECT  _id as app_id,acceptedBid.acceptedAt as rectified_accepted_at,acceptedBid.date as rectified_accepted_bid_created_at,paymentStatus.date as rectified_payment_status_date
    FROM `${project_id}.${dataset_id}.applications_gcs_streaming`
    WHERE operationType != 'delete'
    and acceptedBid.acceptedAt <= paymentStatus.date and paymentStatus.date is not null
    QUALIFY ROW_NUMBER() OVER(PARTITION BY _id ORDER BY __v desc, ifnull(updatedAt,'1990-01-01') desc, ifnull(time_archived,'1990-01-01') DESC) =1
),

rectified_debt_letter_date as (
SELECT  _id as app_id,debtLetter.date as rectified_debt_letter_date,
    FROM `${project_id}.${dataset_id}.applications_gcs_streaming`
    WHERE operationType != 'delete'
    and debtLetter.date is not null
    QUALIFY ROW_NUMBER() OVER(PARTITION BY _id ORDER BY __v desc, ifnull(updatedAt,'1990-01-01') desc, ifnull(time_archived,'1990-01-01') DESC) =1
),


application as (
SELECT
  a.* EXCEPT( _id,scheduledcall,person,partner,
     
     allPaidOutBySambla ,

     bids ,

     estates ,

     excludeBanks ,

     internalComments ,

     loans ,

     scheduledCalls ,

     version 
,
 
     acceptedBid ,

     adservice ,

     adtraction ,

     agentFeedback ,

     bankAccount ,

     company ,

     debtLetter ,

     lastPaidOutBySambla ,

     lifeInsurance ,

     loanProtectionForPartner ,

     loanProtectionForPerson ,

     loxyStatus ,

     mortgageInfo ,

     partnerTracking ,

     paymentProtection ,

     paymentStatus ,

     random ,

     status ,

     teamAttributes ,

     utm,application_id,versions,updated_at

 ),
 _id as application_id,a.__v as versions,a.updatedAt as updated_at,
  acceptedBid.bankEffectiveInterest,
  acceptedBid.requires_extra_doc,
  acceptedBid.effectiveInterest,
  acceptedBid.placedBy.user acceptedBiduser,
  acceptedBid.placedby.name acceptedBidname,
  acceptedBid.integrationAcceptSent acceptedBidintegrationAcceptSent,
  acceptedBid.signLink,
  acceptedBid.bankMonthlyCost,
  acceptedBid.mustRefinanceAmount,
  acceptedBid.startupFee,
  acceptedBid.accepted,
  acceptedBid.comments acceptedBidcomments,
  acceptedBid.amount acceptedBidamount,
  acceptedBid.bankNotInterested acceptedBidbankNotInterested,
  acceptedBid.invitation,
  acceptedBid.fromInbox,
  acceptedBid.amortizationType,
  acceptedBid._id acceptedBidId,
  acceptedBid.integrationAccentSent,
  acceptedBid.requiredDocs.companyBalanceSheet,
  acceptedBid.requiredDocs.ownCompanyBalance,
  acceptedBid.requiredDocs.rentalIncome,
  acceptedBid.requiredDocs.employerDoc,
  acceptedBid.requiredDocs.retiredPaymentNotice,
  acceptedBid.requiredDocs.companyMoreRecentProfitAndLossStatement,
  acceptedBid.requiredDocs.taxReturn,
  acceptedBid.requiredDocs.companyProfitAndLossStatement,
  acceptedBid.requiredDocs.payslip,
  acceptedBid.requiredDocs.companyBankStatements,
  acceptedBid.requiredDocs.companyAdditionalGuarantor,
  acceptedBid.bank acceptedBidBank,
  acceptedBid.maximumAmount,
  acceptedBid.repaymentTime acceptedBidrepaymentTime,
  acceptedBid.externalApplicationId acceptedBidexternalApplicationId,
  acceptedBid.interest acceptedBidinterest,
  acceptedBid.adminFee,
  
  agentFeedback.firstCallAnswered,
  agentFeedback. partnerWhenPossible,
  agentFeedback.notPrepared,
  agentFeedback.noAnswer,
  agentFeedback.correctExpectations,
  agentFeedback.noPartnerWhenPossible,
  agentFeedback.customerSatisfied,
  agentFeedback.firstCallNotAnswered,
  agentFeedback.noInfoAboutLoan,
  agentFeedback.infoAboutLoan,
  agentFeedback.wrongExpectations,
  
  company.ngrams ,
  company.yearlyRevenue,
  company.address,
  company.monthlyRevenue,
  company.entityType,
  company.postalCode,
  company.industryTypeOther,
  company.market companymarket,
  company.organizationNumber,
  company._id companyId,
  company.name companyName,
  company.postalArea,
  company.foundationDate,
  company.industryType,

  debtLetter.signed debtLettersigned,
  debtLetter.placedBy.name,
  debtLetter.amount debtLetteramount,
  debtLetter.accepted debtLetteraccepted,
  debtLetter.integrationAcceptSent debtLetterintegrationAcceptSent,
  debtLetter.repaymentTime debtLetterrepaymentTime,
  debtLetter.sent debtLettersent,
  debtLetter.externalApplicationId debtLetterexternalApplicationId,
  debtLetter.acceptedAt debtLetteracceptedAt,
  debtLetter. _id debtLetterId,
  debtLetter. interest debtLetterinterest,
  debtLetter. bank debtLetterBank,
  debtLetter.bankNotInterested debtLetterbankNotInterested,
  
  lastPaidOutBySambla.bank lastPaidOutBySamblabank,
  lastPaidOutBySambla.date lastPaidOutBySambladate,
  lastPaidOutBySambla.amount lastPaidOutBySamblaamount,
  
  lifeInsurance.signed lifeInsurancesigned,
  lifeInsurance.notInterested lifeInsurancenotInterested,
  lifeInsurance.signedAt lifeInsurancesignedAt,
  lifeInsurance.insuranceType,
  lifeInsurance.premium,
  lifeInsurance.notInterestedReason,
  lifeInsurance. errorMessage,
  lifeInsurance. compensation,
  loanProtectionForPartner.notInterested loanProtectionForPartnernotInterested,
  loanProtectionForPartner.accepted loanProtectionForPartneraccepted,
  loanProtectionForPartner.acceptedAt loanProtectionForPartneracceptedAt,
  loanProtectionForPartner.sentToBank loanProtectionForPartnersentToBank,
  loanProtectionForPartner.person loanProtectionForPartnerperson,
  loanProtectionForPerson.notInterested loanProtectionForPersonnotInterested,
  loanProtectionForPerson.accepted loanProtectionForPersonaccepted,
  loanProtectionForPerson.acceptedAt loanProtectionForPersonacceptedAt,
  loanProtectionForPerson.notApplicable,
  loanProtectionForPerson.sentToBank loanProtectionForPersonsentToBank,
  loanProtectionForPerson.person,
  
  loxyStatus.agentId,
  loxyStatus.status,
  loxyStatus.date loxyStatusdate,
 
  mortgageInfo.refinanceMortgage mortgageInforefinanceMortgage,
  mortgageInfo.propertyType,
  mortgageInfo.daycare,
  mortgageInfo.propertyValue,
  mortgageInfo.monthlyCondoRent,
  mortgageInfo.cars,
  mortgageInfo.otherIncomes,
  mortgageInfo.localTaxes,
  mortgageInfo.condoRent,
 
  partnerTracking.adservice.fp partnerTrackingfp,
  partnerTracking.hypetraffic.cid hypetrafficcid,
  partnerTracking.responsefinance.transaction_id,
  partnerTracking.swegaming.sgclid,
  partnerTracking.smartresponse.affid,
  partnerTracking.smartresponse. reqid,
  partnerTracking.smartresponse.cid smartresponsecid,
  partnerTracking.trygglan.cid trygglancid,
  partnerTracking.unifinance .uuid,
  partnerTracking.adtraction.at_gd,
  partnerTracking.adtraction.cn,
  partnerTracking.adtraction.cv,
  paymentProtection.signed,
  paymentProtection.notInterested,
  paymentProtection.qualified.CTF,
  paymentProtection.qualified.CFTF,
  paymentProtection.signedAt paymentProtectionsignedAt,
  paymentProtection.insuranceType paymentProtectioninsuranceType,
  paymentProtection.premium paymentProtectionpremium,
  paymentProtection.notInterestedReason paymentProtectionnotInterestedReason,
  paymentProtection.errorMessage paymentProtectionerrorMessage,
  paymentProtection.compensation paymentProtectioncompensation,
  paymentStatus.paidAccordingToBank paymentStatuspaidAccordingToBank,
  paymentStatus.bank paymentStatusBank,
  paymentStatus.paidAccordingToCustomer,
  
  random.type randomtype,
  random.coordinates ,
  
  scheduledCall.date scheduled_call_date,
  status.rejectionReasonForInsurance,
  status.active,
  status.text,
  status.supplement,
  status.sent statussent,
  status.deactivationReason,
  status.date statusdate,
  teamAttributes.teamLeader,
  teamAttributes.team,
  
  utm.term,
  utm.cookieId,
  utm.adposition,
  utm.medium,
  utm.partner,
  utm.content,
  utm.campaign,
  utm.device,
  utm.gaCookie,
  utm.campaignId,
  utm.source,
  utm.domain,
  utm.fbcCookie,
  utm.fbpCookie,
  person._id applicant_id,
  partner._id as partner_id,
  person.idnumber as national_id,
  --person.spouseIncome AS spouse_income,

  -- picking a acceptedAt date from version where accpetedBid.acceptedAt < paymentStatus.date,because we have paymentStatus.date that are less than acceptedAt.
  case when ra.app_id is null then (case when a.paymentStatus.date is not null and a.acceptedBid.acceptedAt > paymentStatus.date then a.paymentStatus.date else a.acceptedBid.acceptedAt end) else ra.rectified_accepted_at end as acceptedAt, 
  case when ra.app_id is null then a.acceptedBid.date else ra.rectified_accepted_bid_created_at end  as acceptedBidDate, -- picking corresponding created_date
  case when ra.app_id is null then a.paymentStatus.date else ra.rectified_payment_status_date end paymentStatusDate, -- picking corresponding paymentStatusDate

  REGEXP_EXTRACT(originReference, r"gclid=([^&]*)") as gclid,
  REGEXP_EXTRACT(originReference, r"fbclid=([^&]*)") as fbclid,
  REGEXP_EXTRACT(originReference, r"scid=([^&]*)") as snapchat_id,
  REGEXP_EXTRACT(originReference, r"msclkid=([^&]*)") as microsoft_click_id,
  case when debtLetter.date is null then rdd.rectified_debt_letter_date else debtLetter.date end debtLetterDate,

FROM
   applications_sambq_r a
   left join rectified_accepted ra on (a._id = ra.app_id)
   left join rectified_debt_letter_date rdd on a._id = rdd.app_id 


--where  timestamp_trunc(time_archived,day) in (timestamp(current_date),timestamp(date_sub(current_date, interval 1 day)),timestamp(date_sub(current_date, interval 2 day)),timestamp(date_sub(current_date, interval 3 day)))


)
select a.* from application a