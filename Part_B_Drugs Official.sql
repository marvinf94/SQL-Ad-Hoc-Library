USE WAREHOUSE LOCAL_marvinfoster ;
USE DATABASE  ;

Select 
act.FK_provider_primary_ID,
act.procedure_hcpcs_cd,
pxc.claim_paid_amt,
pxc.month_cd 
from INSIGHTS.METRIC_VALUE_PATIENT_X_CLAIM pxc
left join INSIGHTS.ACTIVITY act 
on pxc.claim_id = act.pk_activity_id
where pxc.qexpu_subheader_cd in('Part B Physician/Supplier (Carrier)-part_b_drugs')                              
and pxc.claim_paid_amt > 0
and pxc.attribution_curr_period_flag = true
and pxc.attribution_type = 'as_was'
and month_cd like '%2021%'
