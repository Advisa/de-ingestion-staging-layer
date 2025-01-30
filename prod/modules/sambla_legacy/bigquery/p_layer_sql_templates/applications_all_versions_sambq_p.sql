with applications_all_versions_sambq_r as (SELECT a.*, 
    case when utm.domain like '%advisa%' then 'advisa' else 'sambla' end as brand, 
    "mongodb" as source_database,
    FROM `${project_id}.${dataset_id}.applications_gcs_streaming` a
    WHERE operationType != 'delete')


, application as (

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

     utm 

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
  
  bankAccount.idNumber,
  bankAccount.accountNumber,
  bankAccount.yearsWithBank,
  bankAccount.sinceYear,
  bankAccount.clearingNumber,
  bankAccount.bankName,
  
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
  company. foundationDate,
  company. industryType,
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
  utm. campaign,
  utm.device,
  utm. gaCookie,
  utm.campaignId,
  utm.source,
  utm.domain,
  person._id applicant_id,
  partner._id as partner_id,
  person.idnumber as national_id,

  REGEXP_EXTRACT(originReference, r"gclid=([^&]*)") as gclid,
  REGEXP_EXTRACT(originReference, r"fbclid=([^&]*)") as fbclid,

FROM
applications_all_versions_sambq_r a


--where  timestamp_trunc(time_archived,day) in (timestamp(current_date),timestamp(date_sub(current_date, interval 1 day)),timestamp(date_sub(current_date, interval 2 day)),timestamp(date_sub(current_date, interval 3 day)))


)
select a.* from application a