USE DATABASE PROD_A2024_FE;
USE WAREHOUSE LOCAL_marvinfoster;
USE ROLE CJ_MS;

with discharges as (

with a2024_hosp as ( 
 
with initial_hosp as ( 
    select * 
    from prod_a2024_fe.insights.metric_value_hosp_ip  
    where attribution_type = 'as_is'), 
     
all_post_ip as ( 
select  
    initial_hosp.org_id as ip_org_id, 
    post_ip.org_id as post_ip_org_id, 
    initial_hosp.fk_patient_id as patient_id, 
    initial_hosp.stay_from_dt as ip_stay_from_dt, 
    initial_hosp.stay_thru_dt as ip_stay_thru_dt, 
    initial_hosp.month_cd as ip_stay_month_cd, 
    initial_hosp.fk_facility_id as ip_facility_id, 
    post_ip.stay_from_dt as post_ip_stay_from_dt, 
    post_ip.stay_thru_dt as post_ip_stay_thru_dt, 
    post_ip.fk_facility_id as post_ip_facility_id, 
    post_ip.claim_type_cd_list as post_ip_claim_type_cd, 
    get(post_ip.diagnosis_facility_principal_icd_10_cd_list,0) as post_ip_icd10, 
    rank() 
          OVER ( 
            PARTITION BY initial_hosp.pk_ip_stay_id  
            ORDER BY post_ip.stay_from_dt) AS first_ip_flag 
from initial_hosp 
left join prod_a2024_fe.insights.inpatient_stay post_ip  
    on initial_hosp.fk_patient_id = post_ip.fk_patient_id  
    and post_ip.stay_from_dt between initial_hosp.stay_thru_dt and initial_hosp.stay_thru_dt + 4  
where  
    initial_hosp.attribution_curr_period_flag = true  
    and ip_type = 'short_term' 
    and planned_admission_flag = False 
) 
 
select  
    all_post_ip.ip_org_id, 
    ip_facility_id, 
    fac.name, 
    count(*) as total_stays, 
    sum(case when get(post_ip_claim_type_cd,0)  in ('20','30') then 1 else 0 end) as discharge_snf, 
    sum(case when get(post_ip_claim_type_cd,0) = '10' then 1 else 0 end) as discharge_home_health, 
    sum(case when get(post_ip_claim_type_cd,0) = '50' then 1 else 0 end) as discharge_hospice, 
    sum(case when get(post_ip_claim_type_cd,0) in ('60','61') then 1 else 0 end) as discharge_other_ip 
from all_post_ip 
left join prod_a2024_fe.insights.facility fac  
    on all_post_ip.ip_facility_id = fac.pk_facility_id 
where  
    first_ip_flag = 1 
    and ip_stay_month_cd in ( 'm-2021-07',
        'm-2021-08',
        'm-2021-09',
        'm-2021-10',
        'm-2021-11',
        'm-2021-12',
        'm-2022-01',
        'm-2022-02',
        'm-2022-03',
        'm-2022-04',
        'm-2022-05',
        'm-2022-06')
     
group by 1,2,3  
order by 4 desc 
), 
 

 
combo as ( 
select * from a2024_hosp 
) 


 
select  
    ip_facility_id, 
    name, 
    sum(total_stays) as total_stays, 
    sum(discharge_snf) as discharge_snf, 
    sum(discharge_home_health) as discharge_home_health, 
    sum(discharge_hospice) as discharge_hospice, 
    sum(discharge_other_ip) as discharge_other_ip 
from combo  
group by 1,2  
order by 3 desc

),


a2024_fac as (
select *
from prod_a2024_fe.insights.profile_list_facility_hosp_ip
where 
    attribution_type = 'as_is'
    and month_cd = 'm-2022-06'
),

combo as (
select * from a2024_fac
)

--select distinct org_id from combo

select 
    facility_id as facility_ccn,
    facility_name as facility_name,
    sum(num_stays_by_aco_patients) as num_stays_by_aco_patients,
    sum(num_aco_beneficiaries_seen) as num_aco_beneficiaries_seen,
    sum(total_aco_spend) as total_aco_spend,
    sum(avg_aco_spend_per_stay*num_stays_by_aco_patients)*1.00/sum(num_stays_by_aco_patients) as avg_aco_spend_per_stay,
    sum(avg_aco_risk_score*num_stays_by_aco_patients)*1.00/sum(num_stays_by_aco_patients) as avg_aco_risk_score,
    sum(total_eradmits) as total_eradmits,
    sum(pct_stays_from_ed*num_stays_by_aco_patients)*1.00/sum(num_stays_by_aco_patients) as pct_stays_from_ed,
    sum(avg_aco_cost_30_day_post_dschrg*num_stays_by_aco_patients)*1.00/sum(num_stays_by_aco_patients) as avg_aco_cost_30_day_post_dschrg,
    sum(ip_readmit_30d_rate*num_stays_by_aco_patients)*1.00/sum(num_stays_by_aco_patients) as ip_readmit_30d_rate,
    discharges.discharge_snf as discharge_snf,
    discharges.discharge_home_health as discharge_home_health,
    discharges.discharge_hospice as discharge_hospice,
    discharges.discharge_other_ip as discharge_other_ip
from combo
left join discharges 
    on discharges.ip_facility_id = combo.facility_id
group by 1,2,12,13,14,15
order by 3 desc 