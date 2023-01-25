USE DATABASE prod_a3327_fe; 
USE WAREHOUSE Local_marvinfoster;
USE ROLE CJ_MS;

with
patient_list as (
          (select
             bene_id
              , prev5_rank as rank
              , 'PREV5' as metric
          from pantry.sndbx.a3327_breadcrumbs_members_2023
          where prev5_rank > 0)
    union
          (select
             bene_id
              , prev6_rank as rank
              , 'PREV6' as metric
          from pantry.sndbx.a3327_breadcrumbs_members_2023
          where prev6_rank > 0)
     union
          (select
             bene_id
              , prev7_rank as rank
              , 'PREV7' as metric
          from pantry.sndbx.a3327_breadcrumbs_members_2023
          where prev7_rank > 0)
     union
         (select
             bene_id
              , prev10_rank as rank
              , 'PREV10' as metric
          from pantry.sndbx.a3327_breadcrumbs_members_2023
          where prev10_rank > 0)
     union
         (select
             bene_id
              , prev12_rank as rank
              , 'PREV12' as metric
          from pantry.sndbx.a3327_breadcrumbs_members_2023
          where prev12_rank > 0)
     union
         (select
             bene_id
              , mh1_rank as rank
              , 'MH1' as metric
          from pantry.sndbx.a3327_breadcrumbs_members_2023
          where mh1_rank > 0)
     union
         (select
             bene_id
              , dm_rank as rank
              , 'DM1' as metric
          from pantry.sndbx.a3327_breadcrumbs_members_2023
          where dm_rank > 0)
),

Code_list as (
select * from pantry.sndbx.breadcrumbs_prev_num_2022
)

select
    act.org_id as aco_id,
    bc.measure_type||bc.measure as measure,
    patient_list.rank,
    bc.code_type,
    act.fk_patient_id as patient_id,
    pat.full_name as patient_name,
    act.fk_provider_primary_id as rendering_npi,
    prov1.name as rendering_prov_name,
    act.fk_facility_id as facility_or_tin_id,
    'hcpcs' as code_system_type,
    act.procedure_hcpcs_cd as code,
    code_hcpcs.short_label as code_label,
    act.activity_from_dt as activity_date,
    pxm.at_time_primary_prov_nh as attributed_npi,
    prov2.name as attributed_prov_name
from "PROD_A3327_FE"."INSIGHTS"."ACTIVITY" act
inner join code_list bc
    on act.procedure_hcpcs_cd = bc.code
inner join patient_list
    on 'cms_mssp|'||patient_list.bene_id = act.fk_patient_id and patient_list.metric = bc.measure_type||bc.measure
left join (select * from "PROD_A3327_FE"."INSIGHTS"."PATIENT_X_MONTH" where attribution_type = 'as_is') pxm 
    on act.fk_patient_id = pxm.fk_patient_id and act.activity_thru_month_cd = pxm.month_cd
left join ref.code_hcpcs
    on act.procedure_hcpcs_cd = code_hcpcs.value
left join "PROD_A3327_FE"."INSIGHTS"."PATIENT"pat 
    on pat.pk_patient_id = act.fk_patient_id
left join "PROD_A3327_FE"."INSIGHTS"."PROVIDER" prov1
    on prov1.pk_provider_id = act.fk_provider_primary_id
left join "PROD_A3327_FE"."INSIGHTS"."PROVIDER" prov2
    on prov2.pk_provider_id = pxm.at_time_primary_prov_nh
where
    bc.code_type = 'num'
    and pxm.attribution_curr_period_flag =true
    and act.activity_from_dt >
        (case 
            when bc.measure = '5' then timestamp '2020-10-01'
            when bc.measure = '6' then timestamp '2013-01-01'
            when bc.measure = '7' then timestamp '2021-08-01'
            when bc.measure = '10' then timestamp '2021-01-01'
            when bc.measure = '1' and bc.measure_type = 'MH' then timestamp '2019-01-01'
            when bc.measure = '1' and bc.measure_type = 'DM' then timestamp '2021-01-01'
            when bc.measure = '12' then timestamp '2020-01-01'
         end)
 order by 5,2 
 

