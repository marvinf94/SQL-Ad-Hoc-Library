Select 
act.FK_provider_primary_ID AS PROVIDER_ID,
prov.name AS PROVIDER_NAME,
PROV.PRACTICE_GROUP,
act.procedure_hcpcs_cd AS HCPCS_CODE,
HCP.short_label AS HCPCS_DESC,
pxc.claim_paid_amt AS PAID_AMOUNT,
pxc.month_cd 
from "PROD_A3327_FE"."INSIGHTS"."METRIC_VALUE_PATIENT_X_CLAIM" pxc
left join "PROD_A3327_FE"."INSIGHTS"."ACTIVITY" act 
on pxc.claim_id = act.pk_activity_id
left join "PROD_COMMON_FE"."REF"."CODE_HCPCS" HCP
on act.procedure_hcpcs_cd = HCP.value
left join "PROD_A3327_FE"."INSIGHTS"."PROVIDER" prov
on act.FK_provider_primary_ID = prov.PK_PROVIDER_ID
where pxc.qexpu_subheader_cd in('Part B Physician/Supplier (Carrier)-part_b_drugs')                              
and pxc.claim_paid_amt > 0
and pxc.attribution_curr_period_flag = true
and pxc.attribution_type = 'as_was'
and pxc.month_cd in ('m-2021-12',
        'm-2022-01',
        'm-2022-02',
        'm-2022-03',
        'm-2022-04',
        'm-2022-05',
        'm-2022-06',
        'm-2022-07',
        'm-2022-06',
        'm-2022-07',
        'm-2022-08',
        'm-2022-09')