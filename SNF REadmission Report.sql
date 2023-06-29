Select
ip.org_id,
snf.AT_TIME_PRIMARY_PROV_NH as Primary_Care_Provider,
snf.FK_PATIENT_ID as Patient_ID,
snf.INPATIENT_SRC_FACILITY as Referring_hosptial,
ip.CLAIM_DRG_CD_LIST [0] as DRG_Code_of_Initial_Stay,
drg.SHORT_LABEL as DRG_Label,
ip.DIAGNOSIS_FACILITY_PRINCIPAL_ICD_10_CD_LIST [0] as Principle_Diagnosis_Of_Initial_Stay,
ip.STAY_FROM_DT as Initial_Stay_From_Dt,
ip.STAY_THRU_DT as Initial_Stay_Thru_Dt,
hos.READMISSION_FLAG as Readmit_Initial_Stay,
snf.FK_PROVIDER_PRIMARY_ID_LIST [0] as SNF_Provider,
snf.FK_FACILITY_ID as SNF_Facility,
--Fac.NAME as Facility_Name,
snf.STAY_FROM_DT as SNF_Stay_From_Date,
snf.STAY_THRU_DT SNF_Stay_Thru_Date,
snf.IP_ADMIT30_FLAG as Readmit_SNF_Stay,
ip2.FK_FACILITY_ID as Readmitting_Hospital,
ip2.CLAIM_DRG_CD_LIST [0] as Readmitting_DRG_Code,
ip2.PK_IP_STAY_ID as unique_identifier,
drg2.SHORT_LABEL as Readmitting_DRG_Label,
ip2.DIAGNOSIS_FACILITY_PRINCIPAL_ICD_10_CD_LIST [0] as Diagnosis_Of_Readmitting_Stay,
ip2.STAY_FROM_DT as Readmitting_Stay_From_Dt,
ip2.STAY_THRU_DT as Readmitting_Stay_Thru_Dt,
pat.DATE_OF_DEATH as Date_of_Death
from "PROD_A2024_FE"."INSIGHTS"."METRIC_VALUE_GROUPED_SNF" snf
left join "PROD_A2024_FE"."INSIGHTS"."INPATIENT_STAY" ip
on snf.IP_LOOKUP = ip.PK_IP_STAY_ID
Left join "PROD_COMMON_FE"."REF"."CODE_DRG" drg
on drg.value = ip.CLAIM_DRG_CD_LIST [0] 
left join (select * from "PROD_A2024_FE"."INSIGHTS"."INPATIENT_STAY" where CLAIM_TYPE_CD_LIST [0] = '60' ) ip2 
on ip2.FK_PATIENT_ID = snf.FK_PATIENT_ID and ip2.STAY_FROM_DT between snf.STAY_THRU_DT and snf.STAY_THRU_DT + 30 
left join "PROD_A2024_FE"."INSIGHTS"."PATIENT" pat
on 'cms_mssp|'||PAT.BENE_MBI_ID = snf.FK_PATIENT_ID 
Left join "PROD_A2024_FE"."INSIGHTS"."METRIC_VALUE_HOSP_IP" hos
on hos.PK_IP_STAY_ID = ip.PK_IP_STAY_ID


Left join (select * from "PROD_COMMON_FE"."REF"."CODE_DRG") drg2
on drg2.value = ip2.CLAIM_DRG_CD_LIST [0] 

where snf.IP_BEFORE_FLAG = 'TRUE'

and snf.MONTH_CD >= 'm-2022-01'
and snf.month_cd <= 'm-2022-12'
and snf.ATTRIBUTION_TYPE = 'as_was'
--and ip.FACILITY_TYPE_CD ='3'
--and ip2.FACILITY_TYPE_CD ='3'
and hos.ATTRIBUTION_TYPE = 'as_was'
and ip2.STAY_FROM_DT >= ip.STAY_FROM_DT 