USE DATABASE ;
USE WAREHOUSE LOCAL_marvinfoster;
USE ROLE CJ_MS;
--op therapy
with

therapy as (
select 
  a.org_id,
  facility_ccn_num, 
  count(distinct a.fk_patient_id) as patients,
  count(*) as claims,
  sum(claim_line_paid_amt) as spend 
from insights.activity  a
left join (select * from insights.patient_x_month where attribution_type = 'as_is') pxm
  on pxm.fk_patient_id = a.fk_patient_id
  and a.activity_from_month_cd = pxm.month_cd
where 
  claim_type_cd = '40' and 
  attribution_curr_period_flag = TRUE and 
  activity_from_dt <= '2022-02-28' and 
  activity_from_dt >= '2021-03-01'
  and get(a.procedure_hcpcs_mod_cd_list,0) in ('GP','GO','GN')
  and a.procedure_hcpcs_cd in 
('92506',
    '92507',
    '92508',
    '92521',
    '92522',
    '92523',
    '92524',
    '92526',
    '92597',
    '92605',
    '92606',
    '92607',
    '92608',
    '92609',
    '92618',
    '96125',
    '97001',
    '97002',
    '97003',
    '97004',
    '97010',
    '97012',
    '97016',
    '97018',
    '97022',
    '97024',
    '97026',
    '97028',
    '97032',
    '97033',
    '97034',
    '97035',
    '97036',
    '97039',
    '97110',
    '97112',
    '97113',
    '97116',
    '97124',
    '97139',
    '97140',
    '97150',
    '97161',
    '97162',
    '97163',
    '97164',
    '97165',
    '97166',
    '97167',
    '97168',
    '97504',
    '97520',
    '97530',
    '97533',
    '97535',
    '97537',
    '97542',
    '97703',
    '97750',
    '97755',
    '97760',
    '97761',
    '97762',
    '97763',
    '97799',
    'G0281',
    'G0283',
    'G0329',
    '92520',
    '97597',
    '97598',
    '97602',
    '97605',
    '97606',
    '97607',
    '97608',
    '97610',
    'G0456',
    'G0457',
    '0183T',
    '64550',
    '90901',
    '90911',
    '92610',
    '92611',
    '92612',
    '92614',
    '92616',
    '95831',
    '95832',
    '95833',
    '95834',
    '95851',
    '95852',
    '95992',
    '96105',
    '96111',
    '97532',
    'G0451',
    'G0515',
    '0019T',
    '90912',
    '90913',
    '97129',
    '97130',
    '98966',
    '98967',
    '98968',
    '98970',
    '98971',
    '98972',
    'G2010',
    'G2012',
    'G2061',
    'G2062',
    'G2063',
    'G2250',
    'G2251'
    )
group by 1,2)
,



op as (
select * from therapy)


--select distinct org_id from combo
--select org_id, count(*) from combo group by 1 

select 
    facility_ccn_num,
    fac_name,
    sum(claims),
    sum(patients),
    sum(spend)
from op
left join prod_common_fe.od.od_ccn_prvdr_service_reg_201609 ccn
  on ccn.prvdr_num = op.facility_ccn_num
group by 1,2
order by 5 desc 
