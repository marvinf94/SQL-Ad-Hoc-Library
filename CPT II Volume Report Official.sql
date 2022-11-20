use warehouse ;
use database ;

Select
ods.ORG_ID,
ods.SRC_RNDRG_PRVDR_NPI_NUM,
ods.SRC_CLM_LINE_HCPCS_CD,
pl.NUM_ATTRIBUTED_PATIENTS,
ods.fk_bene_id
from ODS.CCLF_5_PT_B_PHYS ods
LEFT JOIN INSIGHTS.PROFILE_LIST_PHYSICIAN_LAYUP pl
on pl.physician_npi = 'npi_num|'|| ods.SRC_CLM_LINE_HCPCS_CD
Where fk_bene_id in (select fk_patient_id from INSIGHTS.PATIENT_X_MONTH where attribution_type = 'as_is' and 
                     attribution_curr_period_flag = true) and SRC_CLM_THRU_DT between '2020-08-01' 
                     and'2021-09-30'
and src_clm_line_HCPCS_CD in (
'99497',
'99498',
'1123F',
'1124F')
group by 1,2,3,4,5




