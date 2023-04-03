USE WAREHOUSE LOCAL_marvinfoster;
USE DATABASE PROD_CITYBLOCKDCE_FE;

SELECT 
    org_id,
    fk_patient_id as patient_id,
    at_time_primary_prov_nh as pcp_npi,
    thru_dt as date_of_service,
    qexpu_header_cd as claim_category_code,
    claim_paid_amt,
    primary_icd_code,
    icd_desc
    from prod_cityblockdce_fe.insights.metric_value_patient_x_claim
where 
    attribution_type = 'as_was'
    and attribution_curr_period_flag = true
    and claim_paid_amt > 0
    and month_cd like '%2022%'