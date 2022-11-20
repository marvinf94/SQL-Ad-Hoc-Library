use warehouse local_marvinfoster;
use database prod_cityblockdce_fe;

Create or replace temporary table local_marvinfoster.public.cityblock_claims as 
((Select c.org_id as org_id, c.fk_bene_id, c.src_cur_clm_uniq_id,
TO_CHAR(c.src_clm_thru_dt, 'YYYY-MM') as date_of_srv,
TO_CHAR(c.src_clm_efctv_dt, 'YYYY-MM') as date_of_load,
c.src_clm_type_cd as claim_type,
Case when c.src_clm_type_cd = '10' then 'Home Health'
    when c.src_clm_type_cd = '20' then 'SNF'
    when c.src_clm_type_cd = '40' then 'Outpatient'
    when c.src_clm_type_cd = '50' then 'Hospice'
    when c.src_clm_type_cd = '60' then 'Inpatient'
    when c.src_clm_type_cd = '71' then 'Professional'
    when c.src_clm_type_cd = '72' then 'Local Carrier DMEPOS'
    when c.src_clm_type_cd = '81' then 'Durable Medical Equipment Regional Carrier, non-DMEPOS'
    when c.src_clm_type_cd = '82' then 'Durable Medical Equipment Regional Carrier, DMEPOS' 
    else 'Drug' end as claim_type_desc, 
'0.0' as allowed_amt,
c.src_clm_pmt_amt as paid_amt
FROM ods.cclf_1_pt_a_clm_hdr c
WHERE COALESCE(c.src_clm_mdcr_npmt_rsn_cd,'#NA') = '#NA'
AND c.src_clm_type_cd IN ('10','20','30','40','50','60','61')
AND c.record_status_cd = 'a'
AND c.effective_flag = true)
UNION All
    (Select c.org_id as org_id, c.fk_bene_id, c.src_cur_clm_uniq_id,
     (CASE WHEN c.src_clm_line_thru_dt < CAST('1900-01-01' AS DATE)
        THEN TO_CHAR(CAST('1900-01-01' AS DATE), 'YYYY-MM')
        ELSE TO_CHAR(c.src_clm_line_thru_dt,'YYYY-MM') END) as date_of_srv,
    TO_CHAR(c.src_clm_efctv_dt, 'YYYY-MM') as date_of_load,
    c.src_clm_type_cd as claim_type,
    Case when c.src_clm_type_cd = '10' then 'Home Health'
    when c.src_clm_type_cd = '20' then 'SNF'
    when c.src_clm_type_cd = '40' then 'Outpatient'
    when c.src_clm_type_cd = '50' then 'Hospice'
    when c.src_clm_type_cd = '60' then 'Inpatient'
    when c.src_clm_type_cd = '71' then 'Professional'
    when c.src_clm_type_cd = '72' then 'Local Carrier DMEPOS'
    when c.src_clm_type_cd = '81' then 'Durable Medical Equipment Regional Carrier, non-DMEPOS'
    when c.src_clm_type_cd = '82' then 'Durable Medical Equipment Regional Carrier, DMEPOS' 
    else 'Drug' end as claim_type_desc,
    (CASE WHEN c.src_clm_adjsmt_type_cd = '1' THEN -1
        ELSE 1 END * c.src_clm_line_alowd_chrg_amt) as allowed_amt,
    (CASE WHEN c.src_clm_adjsmt_type_cd = '1' THEN -1
    ELSE 1 END * c.src_clm_line_cvrd_pd_amt) as paid_amt
    FROM ods.cclf_5_pt_b_phys c
//    join insights.patient_x_month pat 
//        on SUBSTR(c.src_clm_thru_dt,3,7) = SUBSTR(pat.month_cd,3,7)
//        and c.fk_bene_id = pat.fk_patient_id                
    where c.src_clm_carr_pmt_dnl_cd != '0'
        AND c.src_clm_prcsg_ind_cd = 'A'
        AND c.record_status_cd = 'a'
        AND c.effective_flag = true)
UNION ALL
    (Select c.org_id as org_id, c.fk_bene_id, c.src_cur_clm_uniq_id, 
    (CASE WHEN c.src_clm_line_thru_dt < CAST('1900-01-01' AS DATE) 
        THEN TO_CHAR(CAST('1900-01-01' AS DATE), 'YYYY-MM')
        ELSE TO_CHAR(c.src_clm_line_thru_dt,'YYYY-MM') END) as date_of_srv,
    TO_CHAR(c.src_clm_efctv_dt, 'YYYY-MM') as date_of_load,
    c.src_clm_type_cd as claim_type,
    Case when c.src_clm_type_cd = '10' then 'Home Health'
    when c.src_clm_type_cd = '20' then 'SNF'
    when c.src_clm_type_cd = '40' then 'Outpatient'
    when c.src_clm_type_cd = '50' then 'Hospice'
    when c.src_clm_type_cd = '60' then 'Inpatient'
    when c.src_clm_type_cd = '71' then 'Professional'
    when c.src_clm_type_cd = '72' then 'Local Carrier DMEPOS'
    when c.src_clm_type_cd = '81' then 'Durable Medical Equipment Regional Carrier, non-DMEPOS'
    when c.src_clm_type_cd = '82' then 'Durable Medical Equipment Regional Carrier, DMEPOS' 
    else 'Drug' end as claim_type_desc,
    '0.0' as allowed_amt,
    (CASE WHEN c.src_clm_adjsmt_type_cd = '1' THEN -1
        ELSE 1 END * c.src_clm_line_cvrd_pd_amt) as paid_amt
    FROM ods.cclf_6_pt_b_dme c
//    join insights.patient_x_month pat 
//        on SUBSTR(c.src_clm_thru_dt,3,7) = SUBSTR(pat.month_cd,3,7)
//        and c.fk_bene_id = pat.fk_patient_id 
    where c.src_clm_carr_pmt_dnl_cd != '0'
        AND c.src_clm_prcsg_ind_cd = 'A'
        AND c.record_status_cd = 'a'
        AND c.effective_flag = true));
        
Create or replace temporary table local_marvinfoster.public.cityblock_patient as
Select distinct pat.fk_patient_id, SUBSTR(pat.month_cd,3,7) as mnth
from prod_cityblockdce_fe.insights.patient_x_month pat;
        
Select clm.* --clm.date_of_srv, sum(clm.paid_amt) as Amt 
from local_marvinfoster.public.cityblock_claims clm 
join local_marvinfoster.public.cityblock_patient pat
on clm.fk_bene_id=pat.fk_patient_id
and clm.date_of_srv=pat.mnth
--group by 1
order by 1;