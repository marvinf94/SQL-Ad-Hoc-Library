USE DATABASE ;
USE WAREHOUSE LOCAL_marvinfoster;
USE ROLE CJ_MS;


with hosp as ( 
with all_stays as (
select 
    hospice.org_id,
    split_part(hospice.fk_patient_id,'|',2) as patient_id,
    split_part(hospice.fk_facility_id,'|',2) as hospice_fac,
    gen.facility_name as hospice_name,
    hospice.stay_from_dt as hospice_stay_from_dt,
    hospice.stay_thru_dt as hospice_stay_thru_dt,
    hospice.total_paid_amt as hospice_paid_amt,
    hospice.length_of_stay as hospice_length_of_stay,
    hospice.deceased_at_hospice as hospice_deceased_flag,
    case 
        when get(before_ip.claim_type_cd_list,0) = '10' then 'HHA'
        when get(before_ip.claim_type_cd_list,0) = '20' or get(before_ip.claim_type_cd_list,0) = '30'then 'SNF'
        when get(before_ip.claim_type_cd_list,0) = '60' then 'Inpatient'
        else 'Unknown' end as before_ip_facility_type,
    before_ip.facility_ccn_num as before_inpatient_facility_id,
    fac1.fac_name as before_inpatient_facility_name,
    before_ip.stay_from_dt as before_inpatient_stay_from_dt,
    before_ip.stay_thru_dt as before_inpatient_stay_thru_dt,
    before_ip.stay_length_of_stay as before_inpatient_length_of_stay,
    before_ip.claim_paid_amt as before_inpatient_paid_amt,
    row_number() OVER (PARTITION BY hospice.grouped_stay_id ORDER BY inpatient_discharge.inpatient_discharge_date DESC) as most_recent
from insights.metric_value_grouped_hospice hospice 
LEFT JOIN (
                SELECT DISTINCT
                    ip.fk_patient_id
                    ,ip.pk_ip_stay_id  as  inpatient_visit_id
                    ,ip.stay_thru_dt   as  inpatient_discharge_date
                    ,ip.fk_facility_id as  inpatient_facility
                    ,ip.claim_type_cd_list as inpatient_clm_type
                FROM insights.inpatient_stay ip -- org parameter
                WHERE get(ip.claim_type_cd_list,0) != '50'
            ) inpatient_discharge 
                ON hospice.fk_patient_id = inpatient_discharge.fk_patient_id
                    AND inpatient_discharge.inpatient_discharge_date  BETWEEN hospice.stay_from_dt  - 30
                    AND hospice.stay_from_dt 
left join insights.inpatient_stay before_ip 
    on inpatient_discharge.inpatient_visit_id = before_ip.pk_ip_stay_id
left join insights.inpatient_stay during_ip 
    on inpatient_discharge.inpatient_visit_id = during_ip.pk_ip_stay_id
left join prod_common_fe.od.od_hospice_compare_gen_info_2015 gen
    on gen.ccn = split_part(hospice.fk_facility_id, '|', 2)
left join prod_common_fe.od.od_ccn_prvdr_service_reg_201609 fac1
    on fac1.prvdr_num = before_ip.facility_ccn_num
left join prod_common_fe.od.od_ccn_prvdr_service_reg_201609 fac2
    on fac2.prvdr_num = during_ip.facility_ccn_num
where 
    attribution_type = 'as_is' and 
    attribution_curr_period_flag = TRUE 
order by hospice.fk_patient_id, hospice.stay_thru_dt
)
select * from all_stays 
where most_recent = 1 
)




select * from hosp


--select distinct org_id from comb