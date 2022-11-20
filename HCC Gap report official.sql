use warehouse local_marvinfoster;
use database prod_cityblockdce_fe;

Drop table if exists local_marvinfoster.public.HCC_temp;

CREATE TEMPORARY TABLE local_marvinfoster.public.HCC_temp as
select target_1_value as HCC,
e.value::string as ICD_CD 
from prod_common_fe.ref.code_xref_map, 
    table(flatten(input => split(array_to_string(source_2_value_list, ','), ','))) e
where xref_id in ('year_x_icd9dx_x_hcc','year_x_icd10dx_x_hcc')
    and source_1_value = 'y-2020'
and version = '23';

drop table if exists local_marvinfoster.PUBLIC.HCC_MAP;

CREATE TEMPORARY TABLE local_marvinfoster.PUBLIC.HCC_MAP as 
Select distinct HCC, HCC_DESC
from prod_cityblockdce_fe.insights.hcc_x_patient_year
where VERSION = '23'
and HCC <> 0; 

drop table if exists local_marvinfoster.public.ICD_Last_Year; 

CREATE TEMPORARY TABLE local_marvinfoster.public.ICD_Last_Year as 
Select distinct dx.org_id, 
    dx.fk_patient_id as Patient_ID, 
    truncate((SUBSTR(dx.month_cd, 3, 4) + 1)) as Year_of_Interest,
    LTRIM(dx.fk_diagnosis_id, 'icd_10_cm_cd|') as ICD_10_Code, 
    icd.label as ICD_10_Description,
    '23' as HCC_Version, 
    hcc.HCC, 
    map.hcc_desc,
    Sum(dx.TOT_VISIT_CNT) as Visit_Count, 
    LTRIM(Max(dx.month_cd), 'm-') as Last_Billed
from prod_cityblockdce_fe.insights.diagnosis_x_patient_month dx 
join prod_common_fe.REF.CODE_ICD_10_CM icd 
    on LTRIM(dx.fk_diagnosis_id, 'icd_10_cm_cd|') = icd.icd_10_cm_cd
join local_marvinfoster.public.HCC_temp hcc
    on LTRIM(dx.fk_diagnosis_id, 'icd_10_cm_cd|') = hcc.icd_cd
left join local_marvinfoster.public.HCC_map map 
    on hcc.hcc = map.hcc
where SUBSTR(dx.month_cd, 3, 4) = '2021'c
Group by 1,2,3,4,5,6,7,8;

drop table if exists local_marvinfoster.public.ICD_Current_Year; 

CREATE TEMPORARY TABLE local_marvinfoster.public.ICD_Current_Year as 
Select distinct dx.fk_patient_id as Patient_ID, 
    SUBSTR(dx.month_cd, 3, 4) as Year,
    LTRIM(dx.fk_diagnosis_id, 'icd_10_cm_cd|') as ICD_10_Code, 
    hcc.HCC
from prod_cityblockdce_fe.insights.diagnosis_x_patient_month dx 
join prod_common_fe.REF.CODE_ICD_10_CM icd 
    on LTRIM(dx.fk_diagnosis_id, 'icd_10_cm_cd|') = icd.icd_10_cm_cd
join local_marvinfoster.public.HCC_temp hcc
    on LTRIM(dx.fk_diagnosis_id, 'icd_10_cm_cd|') = hcc.icd_cd
where SUBSTR(dx.month_cd, 3, 4) = '2022';

Select distinct past.*, 
'Y' as Had_ICD_10_Prior_Year, 
Case when now.ICD_10_Code is null then 'N'
    when now.ICD_10_Code is not null then 'Y' end as Had_ICD_10_Current_Year,
Case when now2.HCC is null then 'N'
    when now2.HCC is not null then 'Y' end as Had_HCC_Current_Year
from local_marvinfoster.public.ICD_Last_Year past 
left join local_marvinfoster.public.ICD_Current_Year now
on past.Patient_ID = now.Patient_ID 
    and past.ICD_10_Code = now.ICD_10_Code
left join local_marvinfoster.public.ICD_Current_Year now2
on past.Patient_ID = now2.Patient_ID 
    and past.HCC = now2.HCC
order by past.Patient_ID, past.HCC, past.ICD_10_Code;