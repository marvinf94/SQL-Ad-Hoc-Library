use warehouse local_marvinfoster;
use database ;

CREATE or replace TEMPORARY TABLE local_marvinfoster.public.HCC_temp as
select target_1_value as HCC,
e.value::string as ICD_CD 
from prod_common_fe.ref.code_xref_map, 
    table(flatten(input => split(array_to_string(source_2_value_list, ','), ','))) e
where xref_id in ('year_x_icd9dx_x_hcc','year_x_icd10dx_x_hcc')
    and source_1_value = 'y-2020'
and version = '23';

CREATE or replace TEMPORARY TABLE local_marvinfoster.PUBLIC.HCC_MAP as 
Select distinct HCC, HCC_DESC
from prod_cityblockdce_fe.insights.hcc_x_patient_year
where VERSION = '23'
and HCC <> 0;

CREATE or replace TEMPORARY TABLE local_marvinfoster.public.ICD_Temp as 
Select distinct SUBSTR(dx.month_cd, 3, 4) as Year, 
    dx.org_id,
    Count(distinct dx.fk_patient_id) Count_of_Patients, 
    LTRIM(dx.fk_diagnosis_id, 'icd_10_cm_cd|') as ICD_10_Code, 
    icd.label as ICD_10_Description
from prod_cityblockdce_fe.insights.diagnosis_x_patient_month dx 
join prod_common_fe.REF.CODE_ICD_10_CM icd 
    on LTRIM(dx.fk_diagnosis_id, 'icd_10_cm_cd|') = icd.icd_10_cm_cd
where (dx.month_cd like 'm-2019%' or dx.month_cd like 'm-2020%' or dx.month_cd like 'm-2021%' or dx.month_cd like 'm-2022%')
Group by 1,2,4,5;

Select distinct icd.*, 
    hcc.hcc, 
    map.hcc_desc,
    Case when hcc.hcc is null then 'N'
        when hcc.hcc is not null then 'Y' end as In_HCC
from local_marvinfoster.public.ICD_Temp icd 
left join local_marvinfoster.public.HCC_temp hcc
    on icd.ICD_10_Code = hcc.ICD_CD
left join local_marvinfoster.public.hcc_map map
    on hcc.hcc = map.hcc
Order by icd.Count_of_Patients desc;
