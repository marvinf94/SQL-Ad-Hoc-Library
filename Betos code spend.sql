Select 
act.org_id,
act.fk_facility_id,
Fac.NAME,
act.procedure_hcpcs_cd,
hcp.SHORT_LABEL,
xref.TARGET_1_VALUE AS BETOS_CODE,
cod.LABEL,
count(*) as claims, 
sum (CLAIM_LINE_PAID_AMT) as TOTAL_SPEND
from "PROD_A2024_FE"."INSIGHTS"."ACTIVITY" act
left join "PROD_COMMON_FE"."REF"."CODE_HCPCS" hcp
on act.procedure_hcpcs_cd = hcp.VALUE
left join "PROD_A2024_FE"."INSIGHTS"."FACILITY" fac
on act.fk_facility_id = 'ccn_num|'|| fac.OSCAR_CCN_NUM
left join "PROD_A2024_FE"."INSIGHTS"."PATIENT_X_MONTH" pxm
on act.activity_thru_month_cd = pxm.month_cd and act.fk_patient_id = pxm.fk_patient_id
left join "PROD_COMMON_FE"."REF"."CODE_XREF_MAP" xref
on xref.SOURCE_1_VALUE = act.procedure_hcpcs_cd
left join "PROD_COMMON_FE"."REF"."CODE" cod
on xref.TARGET_1_VALUE = cod.VALUE 

where 
act.claim_type_cd = '40' and
activity_from_dt <= '2022-09-30' and 
activity_from_dt >= '2021-10-01' and
pxm.ATTRIBUTION_TYPE = 'as_was' and
pxm.ATTRIBUTION_CURR_PERIOD_FLAG = 'TRUE' and
xref.xref_id = 'hcpcs_x_betos' and 
cod.TYPE_CD = 'hcpcs_betos_cd'


group by 1,2,3,4,5,6,7





