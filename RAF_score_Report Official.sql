USE WAREHOUSE local_marvinfoster;
USE DATABASE prod_cityblockdce;

------------------------------------------------------
-- step 1
-- diagnosis_x_patient_month (modified)
------------------------------------------------------

CREATE OR REPLACE TABLE local_marvinfoster.public.diagnosis_x_patient_month_CB as
select * from insights.diagnosis_x_patient_month 
where SUBSTR(MONTH_CD,3,4) in ('2019','2020','2021','2022')
--and SUBSTR(MONTH_CD,8,2) in ('01','02','03','04','05')
;

------------------------------------------------------
-- step 2
-- hcc_x_patient_year (modified)
-- set to version 22
------------------------------------------------------

CREATE OR REPLACE TABLE local_marvinfoster.public.hcc_x_patient_year_CB as
WITH hcc_x_patient_year_tmp AS (
    SELECT
          fk_patient_id || '|' || CAST(hcc AS VARCHAR) || '|' ||  CAST(version AS VARCHAR) || '|' || CAST(dos_year AS VARCHAR) AS pk_hcc_patient_year_id
        , fk_patient_id
        , dos_year
        , version
        , hcc
        , hcc_desc
        , hcc_coeff
        , hcc_hierarchy_list
        , min(CASE WHEN hcc = 0 THEN NULL ELSE first_visit_dt END) AS hcc_first_visit_dt
        , max(CASE WHEN hcc = 0 THEN NULL ELSE last_visit_dt END) AS hcc_last_visit_dt
        , max(hcpcs_ra_flag) AS hcpcs_ra_flag
        , sum(CASE WHEN hcc = 0 THEN 0 ELSE facility_ip_stay_cnt END) AS hcc_facility_ip_stay_cnt
        , sum(CASE WHEN hcc = 0 THEN 0 ELSE tot_visit_cnt END) AS hcc_tot_visit_cnt
        , sum(CASE WHEN hcc = 0 THEN 0 ELSE facility_op_visit_cnt END) AS hcc_facility_op_visit_cnt
        , sum(CASE WHEN hcc = 0 THEN 0 ELSE physician_visit_cnt END) AS hcc_physician_visit_cnt
        , sum(CASE WHEN hcc = 0 THEN 0 ELSE tot_provider_cnt END) AS hcc_tot_provider_cnt
        , sum(CASE WHEN hcc = 0 THEN 0 ELSE facility_ip_provider_cnt END) AS hcc_facility_ip_provider_cnt
        , sum(CASE WHEN hcc = 0 THEN 0 ELSE facility_op_provider_cnt END) AS hcc_facility_op_provider_cnt
        , sum(CASE WHEN hcc = 0 THEN 0 ELSE physician_provider_cnt END) AS hcc_physician_provider_cnt
    FROM (
      
        -- pulls patients with HCCs based on icd billing
        with people_WITH_hccs_this_year AS (
            SELECT
                  CAST(ref.version AS int) AS version
                , CAST(ref.target_1_value AS int) AS hcc
                , info.target_1_value AS hcc_desc
                , CAST(info.target_2_value AS DECIMAL(10,2)) AS hcc_coeff
                , info.target_3_value_list AS hcc_hierarchy_list
                , diag.diag_type
                , diag.diag
                , diag.dos_year
                , diag.code_xref_JOIN_year
                , diag.org_id
                , diag.fk_patient_id
                , diag.fk_diagnosis_id
                , diag.month_cd
                , diag.first_visit_dt
                , diag.last_visit_dt
                , diag.tot_visit_cnt
                , diag.physician_visit_cnt
                , diag.facility_ip_stay_cnt
                , diag.facility_op_visit_cnt
                , diag.tot_provider_cnt
                , diag.facility_op_provider_cnt
                , diag.facility_ip_provider_cnt
                , diag.physician_provider_cnt
                , diag.last_12_first_visit_dt
                , diag.last_12_last_visit_dt
                , diag.last_12_tot_visit_cnt
                , diag.last_12_physician_visit_cnt
                , diag.last_12_facility_ip_stay_cnt
                , diag.last_12_facility_op_visit_cnt
                , diag.last_12_tot_provider_cnt
                , diag.last_12_facility_op_provider_cnt
                , diag.last_12_facility_ip_provider_cnt
                , diag.last_12_physician_provider_cnt
                , diag.hcpcs_ra_flag
            FROM (
                SELECT
                      split_part(fk_diagnosis_id,'|',1) AS diag_type
                    , split_part(fk_diagnosis_id,'|',2) AS diag
                    , CAST(split_part(month_cd,'-',2) AS int) AS dos_year
                    , CAST(split_part(month_cd,'-',2) AS int) AS code_xref_JOIN_year -- modification to pull 2022 hccs (not 2023, unavail)
                    , org_id
                    , fk_patient_id
                    , fk_diagnosis_id
                    , month_cd
                    , first_visit_dt
                    , last_visit_dt
                    , tot_visit_cnt
                    , physician_visit_cnt
                    , facility_ip_stay_cnt
                    , facility_op_visit_cnt
                    , tot_provider_cnt
                    , facility_op_provider_cnt
                    , facility_ip_provider_cnt
                    , physician_provider_cnt
                    , last_12_first_visit_dt
                    , last_12_last_visit_dt
                    , last_12_tot_visit_cnt
                    , last_12_physician_visit_cnt
                    , last_12_facility_ip_stay_cnt
                    , last_12_facility_op_visit_cnt
                    , last_12_tot_provider_cnt
                    , last_12_facility_op_provider_cnt
                    , last_12_facility_ip_provider_cnt
                    , last_12_physician_provider_cnt
                    , hcpcs_ra_flag
                FROM local_marvinfoster.public.diagnosis_x_patient_month_CB
            ) diag
          
            INNER JOIN (
                SELECT
                      source_1_value
                    , source_2_value_list
                    , version
                    , target_1_value
                 FROM prod_common.ref.code_xref_map
                 WHERE xref_id in ('year_x_icd9dx_x_hcc','year_x_icd10dx_x_hcc')
                    AND VERSION = 22
            ) ref
            ON diag.code_xref_JOIN_year = CAST(split_part(ref.source_1_value,'-',2) AS int)
            AND array_contains(diag.diag::variant, ref.source_2_value_list)
          
            INNER JOIN (
                SELECT
                    source_1_value
                    , version
                    , target_1_value
                    , target_2_value
                    , target_3_value_list
                    , target_4_value_list
                 FROM prod_common.ref.code_xref_map
                 WHERE xref_id = 'hcc_x_info'
            ) info
            ON ref.version = info.version
            AND ref.target_1_value = info.source_1_value
        )
      
        -- pulls patients with no HCCs / no icd billing
        , people_w_no_hccs_that_year AS (
            SELECT
                  22 AS version
                , 0 AS hcc
                , 'N/A' AS hcc_desc
                , 0.00  AS hcc_coeff
                , NULL  AS hcc_hierarchy_list
                , diag.diag_type
                , diag.diag
                , diag.dos_year
                , diag.code_xref_JOIN_year
                , diag.org_id
                , diag.fk_patient_id
                , diag.fk_diagnosis_id
                , diag.month_cd
                , diag.first_visit_dt
                , diag.last_visit_dt
                , diag.tot_visit_cnt
                , diag.physician_visit_cnt
                , diag.facility_ip_stay_cnt
                , diag.facility_op_visit_cnt
                , diag.tot_provider_cnt
                , diag.facility_op_provider_cnt
                , diag.facility_ip_provider_cnt
                , diag.physician_provider_cnt
                , diag.last_12_first_visit_dt
                , diag.last_12_last_visit_dt
                , diag.last_12_tot_visit_cnt
                , diag.last_12_physician_visit_cnt
                , diag.last_12_facility_ip_stay_cnt
                , diag.last_12_facility_op_visit_cnt
                , diag.last_12_tot_provider_cnt
                , diag.last_12_facility_op_provider_cnt
                , diag.last_12_facility_ip_provider_cnt
                , diag.last_12_physician_provider_cnt
                , diag.hcpcs_ra_flag
            FROM (
                SELECT
                      split_part(fk_diagnosis_id, '|', 1) AS diag_type
                    , split_part(fk_diagnosis_id, '|', 2) AS diag
                    , CAST(split_part(month_cd,'-',2) AS int) AS dos_year
                    , CAST(split_part(month_cd,'-', 2) AS int) AS code_xref_JOIN_year -- modification to pull 2022 hccs (not 2023, unavail)
                    , org_id
                    , fk_patient_id
                    , fk_diagnosis_id
                    , month_cd
                    , first_visit_dt
                    , last_visit_dt
                    , tot_visit_cnt
                    , physician_visit_cnt
                    , facility_ip_stay_cnt
                    , facility_op_visit_cnt
                    , tot_provider_cnt
                    , facility_op_provider_cnt
                    , facility_ip_provider_cnt
                    , physician_provider_cnt
                    , last_12_first_visit_dt
                    , last_12_last_visit_dt
                    , last_12_tot_visit_cnt
                    , last_12_physician_visit_cnt
                    , last_12_facility_ip_stay_cnt
                    , last_12_facility_op_visit_cnt
                    , last_12_tot_provider_cnt
                    , last_12_facility_op_provider_cnt
                    , last_12_facility_ip_provider_cnt
                    , last_12_physician_provider_cnt
                    , hcpcs_ra_flag
                FROM local_marvinfoster.diagnosis_x_patient_month_CB
            ) diag
          
            LEFT JOIN (
                SELECT
                      fk_patient_id
                    , code_xref_JOIN_year
                FROM people_WITH_hccs_this_year WHERE version = 22
            ) curr_diag
            ON diag.fk_patient_id = curr_diag.fk_patient_id
            AND diag.code_xref_JOIN_year = curr_diag.code_xref_JOIN_year
          
            WHERE curr_diag.fk_patient_id is null
        )
      
        -- stack patients with and without HCCs
        SELECT
              version
            , hcc
            , hcc_desc
            , hcc_coeff
            , hcc_hierarchy_list
            , diag_type
            , diag
            , dos_year
            , code_xref_JOIN_year
            , org_id
            , fk_patient_id
            , fk_diagnosis_id
            , month_cd
            , first_visit_dt
            , last_visit_dt
            , tot_visit_cnt
            , physician_visit_cnt
            , facility_ip_stay_cnt
            , facility_op_visit_cnt
            , tot_provider_cnt
            , facility_op_provider_cnt
            , facility_ip_provider_cnt
            , physician_provider_cnt
            , last_12_first_visit_dt
            , last_12_last_visit_dt
            , last_12_tot_visit_cnt
            , last_12_physician_visit_cnt
            , last_12_facility_ip_stay_cnt
            , last_12_facility_op_visit_cnt
            , last_12_tot_provider_cnt
            , last_12_facility_op_provider_cnt
            , last_12_facility_ip_provider_cnt
            , last_12_physician_provider_cnt
            , hcpcs_ra_flag
        FROM people_WITH_hccs_this_year

        UNION ALL

        SELECT
              version
            , hcc
            , hcc_desc
            , hcc_coeff
            , hcc_hierarchy_list
            , diag_type
            , diag
            , dos_year
            , code_xref_JOIN_year
            , org_id
            , fk_patient_id
            , fk_diagnosis_id
            , month_cd
            , first_visit_dt
            , last_visit_dt
            , tot_visit_cnt
            , physician_visit_cnt
            , facility_ip_stay_cnt
            , facility_op_visit_cnt
            , tot_provider_cnt
            , facility_op_provider_cnt
            , facility_ip_provider_cnt
            , physician_provider_cnt
            , last_12_first_visit_dt
            , last_12_last_visit_dt
            , last_12_tot_visit_cnt
            , last_12_physician_visit_cnt
            , last_12_facility_ip_stay_cnt
            , last_12_facility_op_visit_cnt
            , last_12_tot_provider_cnt
            , last_12_facility_op_provider_cnt
            , last_12_facility_ip_provider_cnt
            , last_12_physician_provider_cnt
            , hcpcs_ra_flag
            FROM people_w_no_hccs_that_year
    )
    GROUP BY
          fk_patient_id
        , dos_year
        , version
        , hcc
        , hcc_desc
        , hcc_coeff
        , hcc_hierarchy_list
)

, hcc_arrays AS (
    SELECT
          fk_patient_id
        , version
        , dos_year
        , ARRAY_AGG(CAST(hcc AS VARCHAR(5))) AS all_hccs
        , ARRAY_AGG(CAST(CASE WHEN hcpcs_ra_flag = 1 THEN hcc  ELSE NULL END AS VARCHAR(5))) AS ra_hccs
    FROM hcc_x_patient_year_tmp
    GROUP BY
          fk_patient_id
        , version
        , dos_year
)

SELECT DISTINCT
      a.pk_hcc_patient_year_id
    , a.fk_patient_id
    , a.dos_year
    , a.version
    , a.hcc
    , a.hcc_desc
    , a.hcc_coeff
    , a.hcc_hierarchy_list
    , a.hcc_first_visit_dt
    , a.hcc_last_visit_dt
    , a.hcc_tot_visit_cnt
    , a.hcc_physician_visit_cnt
    , a.hcc_facility_ip_stay_cnt
    , a.hcc_facility_op_visit_cnt
    , a.hcc_tot_provider_cnt
    , a.hcc_facility_op_provider_cnt
    , a.hcc_facility_ip_provider_cnt
    , a.hcc_physician_provider_cnt
    , CASE WHEN b.fk_patient_id IS NOT NULL THEN 1 ELSE 0 END AS hcc_hierarchy_canceled_flag
    , CASE WHEN c.fk_patient_id IS NOT NULL AND a.hcc <> 0 THEN 1
           WHEN g.fk_patient_id IS NOT NULL AND a.hcc <> 0 THEN 0
           ELSE NULL END AS patient_had_hcc_prior_year
    , CASE WHEN d.fk_patient_id IS NOT NULL AND a.hcc <> 0 THEN 1
           WHEN f.fk_patient_id IS NOT NULL AND a.hcc <> 0 THEN 0
           ELSE NULL END AS patient_has_hcc_next_year
    , CASE WHEN e.fk_patient_id IS NOT NULL THEN 1
           WHEN f.fk_patient_id IS NOT NULL AND a.hcc <> 0 THEN 0
           ELSE NULL END AS patient_has_higher_hierarchy_hcc_next_year
    , a.hcpcs_ra_flag
    , CASE WHEN b_ra.fk_patient_id IS NOT NULL THEN 1 ELSE 0 END AS hcc_hierarchy_canceled_hcpcs_ra_flag
    , CASE WHEN c_ra.fk_patient_id IS NOT NULL AND a.hcc <> 0 THEN 1
           WHEN g.fk_patient_id IS NOT NULL AND a.hcc <> 0 THEN 0
           ELSE NULL END AS patient_had_hcc_prior_year_hcpcs_ra_flag
    , CASE WHEN d_ra.fk_patient_id IS NOT NULL AND a.hcc <> 0 THEN 1
           WHEN f.fk_patient_id IS NOT NULL AND a.hcc <> 0 THEN 0
           ELSE NULL END AS patient_has_hcc_next_year_hcpcs_ra_flag
    , CASE WHEN e_ra.fk_patient_id IS NOT NULL THEN 1
           WHEN f.fk_patient_id IS NOT NULL AND a.hcc <> 0 THEN 0
           ELSE NULL END AS patient_has_higher_hierarchy_hcc_next_year_hcpcs_ra_flag
    , NULL AS patient_name
    , NULL AS risk_score
    , NULL AS patient_frailty_group
    , NULL AS medicare_cohort
FROM hcc_x_patient_year_tmp AS a
LEFT JOIN hcc_arrays AS b ON a.fk_patient_id = b.fk_patient_id AND a.version = b.version AND a.dos_year = b.dos_year AND ARRAYS_OVERLAP(array_except(a.hcc_hierarchy_list, ARRAY_CONSTRUCT('#NA')), b.all_hccs)
LEFT JOIN hcc_arrays AS c ON a.fk_patient_id = c.fk_patient_id AND a.dos_year - 1 = c.dos_year AND a.version = c.version AND ARRAY_CONTAINS(cast(a.hcc as varchar(5))::variant, c.all_hccs)
LEFT JOIN hcc_arrays AS d ON a.fk_patient_id = d.fk_patient_id AND a.dos_year + 1 = d.dos_year AND a.version = d.version AND ARRAY_CONTAINS(cast(a.hcc as varchar(5))::variant, d.all_hccs)
LEFT JOIN hcc_arrays AS e ON a.fk_patient_id = e.fk_patient_id AND a.dos_year + 1 = e.dos_year AND a.version = e.version AND ARRAYS_OVERLAP(array_except(a.hcc_hierarchy_list, ARRAY_CONSTRUCT('#NA')), e.all_hccs)
LEFT JOIN (SELECT fk_patient_id, dos_year FROM hcc_arrays GROUP BY fk_patient_id, dos_year) AS f ON a.fk_patient_id = f.fk_patient_id AND a.dos_year + 1 = f.dos_year
LEFT JOIN (SELECT fk_patient_id, dos_year FROM hcc_arrays GROUP BY fk_patient_id, dos_year) AS g ON a.fk_patient_id = g.fk_patient_id AND a.dos_year - 1 = g.dos_year
LEFT JOIN hcc_arrays AS b_ra ON a.fk_patient_id = b_ra.fk_patient_id AND a.version = b_ra.version AND a.dos_year = b_ra.dos_year AND ARRAYS_OVERLAP(array_except(a.hcc_hierarchy_list, ARRAY_CONSTRUCT('#NA')), b_ra.ra_hccs)
LEFT JOIN hcc_arrays AS c_ra ON a.fk_patient_id = c_ra.fk_patient_id AND a.dos_year - 1 = c_ra.dos_year AND a.version = c_ra.version AND ARRAY_CONTAINS(cast(a.hcc as varchar(5))::variant, c_ra.ra_hccs)
LEFT JOIN hcc_arrays AS d_ra ON a.fk_patient_id = d_ra.fk_patient_id AND a.dos_year + 1 = d_ra.dos_year AND a.version = d_ra.version AND ARRAY_CONTAINS(cast(a.hcc as varchar(5))::variant, d_ra.ra_hccs)
LEFT JOIN hcc_arrays AS e_ra ON a.fk_patient_id = e_ra.fk_patient_id AND a.dos_year + 1 = e_ra.dos_year AND a.version = e_ra.version AND ARRAYS_OVERLAP(array_except(a.hcc_hierarchy_list, ARRAY_CONSTRUCT('#NA')), e_ra.ra_hccs)
;


------------------------------------------------------
-- step 3
-- patient_x_month (modified)
-- QA updates: row_num/ranking logic, atribution set to as_was, select distinct (to ensure no dupes)
------------------------------------------------------

CREATE OR REPLACE TABLE local_marvinfoster.public.patient_x_month_cb as
with rank as (
    select distinct
        m.fk_patient_id, 
        SUBSTR(m.month_cd,3,4) as year,
        row_number() OVER (partition by m.fk_patient_id, year, m.ATTRIBUTION_TYPE order by m.month_cd desc) as row_num, 
        case when m.ASSGN_MEDICARE_STATUS_CD = 4 then 'CNA'
          when m.ASSGN_MEDICARE_STATUS_CD = 1 then 'INS'
          when (m.ASSGN_MEDICARE_STATUS_CD = 3 and med.BENE_ENTITLEMENT_BUYIN_CD in ('3','C')) then 'CFA'
          when (m.ASSGN_MEDICARE_STATUS_CD = 3 and med.BENE_ENTITLEMENT_BUYIN_CD not in ('3','C')) then 'CPA'
          when (m.ASSGN_MEDICARE_STATUS_CD = 2 and m.BENE_DUAL_STATUS_CD in ('01','02','08') and med.BENE_ENTITLEMENT_BUYIN_CD in ('3','C')) then 'CFD'
          when (m.ASSGN_MEDICARE_STATUS_CD = 2 and m.BENE_DUAL_STATUS_CD in ('01','02','08') and med.BENE_ENTITLEMENT_BUYIN_CD not in ('3','C')) then 'CPD'
          when (m.ASSGN_MEDICARE_STATUS_CD = 2 and m.BENE_DUAL_STATUS_CD in ('03','06','NA')) then 'CND'
        else 'CNA' end as cohort,  
        case when m.ASSGN_MEDICARE_STATUS_CD = 4 then 'CNA_interaction'
          when m.ASSGN_MEDICARE_STATUS_CD = 1 then 'INS_interaction'
          when (m.ASSGN_MEDICARE_STATUS_CD = 3 and med.BENE_ENTITLEMENT_BUYIN_CD in ('3','C')) then 'CFA_interaction'
          when (m.ASSGN_MEDICARE_STATUS_CD = 3 and med.BENE_ENTITLEMENT_BUYIN_CD not in ('3','C')) then 'CPA_interaction'
          when (m.ASSGN_MEDICARE_STATUS_CD = 2 and m.BENE_DUAL_STATUS_CD in ('01','02','08') and med.BENE_ENTITLEMENT_BUYIN_CD in ('3','C')) then 'CFD_interaction'
          when (m.ASSGN_MEDICARE_STATUS_CD = 2 and m.BENE_DUAL_STATUS_CD in ('01','02','08') and med.BENE_ENTITLEMENT_BUYIN_CD not in ('3','C')) then 'CPD_interaction'
          when (m.ASSGN_MEDICARE_STATUS_CD = 2 and m.BENE_DUAL_STATUS_CD in ('03','06','NA')) then 'CND_interaction'
        else 'CNA_interaction' end as interaction,   
        case when p.gender_cd = 2 then 'F' else 'M' end as gender_cd,
        p.date_of_birth
    from insights.patient_x_month m
    join insights.patient p 
        on m.fk_patient_id = p.pk_patient_id 
    left join insights.patient_x_medicare_month med
        on m.fk_patient_id = med.fk_patient_id 
        and m.month_cd = med.month_cd
    where attribution_type = 'as_was' -- to account for benes who get bucketed into diff cohorts b/c on the last month of as_is is diff than last month of as_was 
) 
select * from rank where row_num = 1 -- pulls most current month for that member+attribution type combo and keeps 
;
    

------------------------------------------------------
-- step 4
-- patient_x_risk_score_month (modified)
-- QA updates: using patient_x_month as the base (instead of hcc_x_patient_year/hcc_ref) to ensure non-utilizers are captured
------------------------------------------------------

create or replace table local_marvinfoster.public.patient_x_risk_score_month_cb as
WITH hcc_ref AS (
    SELECT 
          FK_PATIENT_ID
        , DOS_YEAR
        , ARRAY_DISTINCT(ARRAY_AGG(HCC)) AS HCC_LIST
        , SUM(DISTINCT HCC_COEFF) AS hcc_ref_coeff
    FROM (
        SELECT DISTINCT 
              FK_PATIENT_ID
            , DOS_YEAR
            , HCC
            , HCC_COEFF
        FROM local_davidesarey.public.hcc_x_patient_year_cb
        WHERE VERSION = 22
    )
    WHERE FK_PATIENT_ID IS NOT NULL
    GROUP BY 
         FK_PATIENT_ID
       , DOS_YEAR 
)
 
, hcc_demograph_ref AS (
    SELECT DISTINCT 
          p.FK_PATIENT_ID
        , p.YEAR as DOS_YEAR
        , CASE WHEN
            ref.gender = p.gender_cd
            AND ref.cohort = p.cohort 
            AND DATEDIFF(years, p.date_of_birth, DATE_FROM_PARTS(p.YEAR,12,31)) BETWEEN ref.age1 AND ref.age2 
            THEN ref.coeff  
          ELSE 0.0 END AS DEMOG_COEFF
    FROM local_marvinfoster.public.patient_x_month_cb p 
    LEFT JOIN (
        SELECT 
              target_3_value AS gender
            , target_1_value AS age1
            , target_2_value AS age2
            , target_4_value AS coeff
            , source_2_value AS cohort
        FROM prod_common.ref.code_xref_map 
        WHERE xref_id = 'year_x_type_x_hcc_demographic' AND version = 22
    ) ref
        ON ref.gender = p.gender_cd
        AND ref.cohort = p.cohort
        AND DATEDIFF(years, p.date_of_birth, DATE_FROM_PARTS(p.YEAR,12,31)) BETWEEN ref.age1 AND ref.age2    
)

-- pull interactions specific to version 22 (would need to revisit logic for other hcc versions)
, hcc_interaction_ref AS (
    SELECT DISTINCT 
          c.FK_PATIENT_ID
        , c.DOS_YEAR
        , ref2.hcc as hcc_match
        , CASE WHEN lower(ref2.HCC2) like '%cancer%' THEN ARRAY_CONSTRUCT(8,9,10,11,12) 
            WHEN lower(ref2.HCC2) like '%diabetes%' THEN ARRAY_CONSTRUCT(17,18,19)
            WHEN lower(ref2.HCC2) like '%copdcf%' THEN ARRAY_CONSTRUCT(110,111,112)
            WHEN lower(ref2.HCC2) like '%renal%' THEN ARRAY_CONSTRUCT(134,135,136,137)
            WHEN TRY_CAST(ref2.HCC2 AS INTEGER) IS NULL THEN ARRAY_CONSTRUCT('#NA')
            ELSE ARRAY_CONSTRUCT(TRY_CAST(ref2.HCC2 AS INTEGER))
          END AS HCC2_list
        , COALESCE(ref2.HCC2_COEFF,0.0) AS HCC2_COEFF
        , ref2.interaction
    FROM local_marvinfoster.public.hcc_x_patient_year_cb c 
    LEFT JOIN local_marvinfoster.public.patient_x_month_cb m 
        on c.fk_patient_id = m.fk_patient_id 
        and c.DOS_YEAR = m.year
    LEFT JOIN (
        SELECT
              replace(a.value,'"','') AS HCC
            , HCC2
            , hcc_desc
            , hcc2_coeff 
            , interaction
        FROM (
              SELECT
                   CASE WHEN lower(source_3_value) LIKE '%resp%' THEN ARRAY_CONSTRUCT(82,83,84)
                              ELSE ARRAY_CONSTRUCT(source_3_value) END AS hcc_list
                  , source_4_value AS hcc2
                  , target_3_value AS hcc_desc
                  , target_1_value AS hcc2_coeff   
                  , source_2_value as interaction
              FROM prod_common.ref.code_xref_map 
              WHERE xref_id = 'year_x_type_x_hcc' AND SOURCE_2_VALUE like '%_interaction' AND version = 22
        ), TABLE(flatten(hcc_list)) a 
    ) ref2
        ON TRY_CAST(ref2.hcc as INTEGER) = c.hcc
        and ref2.interaction = m.interaction
    WHERE c.fk_patient_id IS NOT NULL and c.VERSION = 22
)
  
, hcc_all AS (
    SELECT 
          FK_PATIENT_ID
        , DOS_YEAR
        , HCC_LIST
        , COALESCE(hcc_ref_coeff,0.0) as hcc_factor
        , COALESCE(demog_coeff,0.0) as demog_factor
        , sum(distinct interaction_coeff) as int_factor
    FROM (
        SELECT 
              base.FK_PATIENT_ID
            , base.year as dos_year
            , hcc_ref.HCC_LIST
            , hcc_ref.hcc_ref_coeff
            , hcc_demograph_ref.DEMOG_COEFF
            , hcc_interaction_ref.HCC_MATCH
            , hcc_interaction_ref.HCC2_LIST
            , CASE WHEN ARRAYS_OVERLAP(hcc_interaction_ref.HCC2_LIST,hcc_ref.HCC_LIST) 
                THEN hcc_interaction_ref.HCC2_COEFF 
                ELSE 0.0 
              END AS interaction_coeff
        FROM local_marvinfoster.public.patient_x_month_cb base
        LEFT JOIN hcc_ref
            ON hcc_ref.FK_PATIENT_ID = base.FK_PATIENT_ID
            AND hcc_ref.DOS_YEAR = base.year
        LEFT JOIN hcc_demograph_ref 
            ON base.FK_PATIENT_ID = hcc_demograph_ref.FK_PATIENT_ID
            AND base.YEAR = hcc_demograph_ref.DOS_YEAR
        LEFT JOIN hcc_interaction_ref
            ON base.FK_PATIENT_ID = hcc_interaction_ref.FK_PATIENT_ID
            AND base.YEAR = hcc_interaction_ref.DOS_YEAR 
            AND (ARRAYS_OVERLAP(hcc_ref.HCC_LIST,ARRAY_CONSTRUCT(TRY_CAST(hcc_interaction_ref.HCC_MATCH AS INTEGER))) AND HCC2_LIST[0] != '#NA')
        WHERE base.year BETWEEN 2014 and 2050
    )
    GROUP BY 
          FK_PATIENT_ID
        , DOS_YEAR 
        , HCC_LIST
        , hcc_factor
        , demog_factor 
)

, hcc_calc AS (
    SELECT
          FK_PATIENT_ID
        , DOS_YEAR
        , max(hcc_factor) as hcc_factor
        , max(demog_factor) as demog_factor
        , max(int_factor) as int_factor
        , SUM(COALESCE(hcc_factor,0.00)+COALESCE(demog_factor,0.00)+COALESCE(int_factor,0.00)) as risk_score
    FROM hcc_all  
    GROUP BY
          FK_PATIENT_ID
        , DOS_YEAR
     
)

SELECT 
     'CITYBLOCKDCE' AS org_id
    , fk_patient_id
    , 'hcc' AS risk_score_cd
    , 'hcc_22' AS risk_score_source_cd
    , 'y-' || DOS_YEAR AS YEAR_CD
    ,   risk_score
    , hcc_factor
    , demog_factor
    , int_factor
FROM hcc_calc
where DOS_YEAR > 2018
;


--Queries for the final views


-- --Report 1 - Patient Level YTD
-- select pxr.org_id,
-- '2022-04' as month_of_report,
-- pxr.fk_patient_id as patient_id,
-- pat.full_name as patient_name, 
-- mnt.at_time_primary_prov_nh as provider_npi, 
-- prov.name as provider_name,
-- pxr.risk_score as raf_score
-- from local_marvinfoster.public.patient_x_risk_score_month_cb pxr
-- left join insights.patient pat 
-- on pxr.fk_patient_id = pat.pk_patient_id
-- left join insights.patient_x_month mnt
-- on pxr.fk_patient_id = mnt.fk_patient_id 
--     and mnt.month_cd = 'm-2022-04'
-- left join insights.provider prov
-- on mnt.at_time_primary_prov_nh = prov.pk_provider_id
-- where pxr.year_cd = 'y-2022';


-- --Report 2 - Historical Patient RAF
-- select pxr.org_id,
-- SUBSTR(pxr.year_cd,3,4) as year,
-- pxr.fk_patient_id as patient_id,
-- pat.full_name as patient_name, 
-- pxr.risk_score as raf_score
-- from local_marvinfoster.public.patient_x_risk_score_month_cb pxr
-- left join insights.patient pat 
-- on pxr.fk_patient_id = pat.pk_patient_id
-- where pxr.year_cd <> 'y-2022'
-- order by pxr.year_cd;


-- --Report 3 - Overall DCE
-- select pxr.org_id,
-- '2022-04' as month_of_report,
-- Avg(pxr.risk_score) as raf_score
-- from local_marvinfoster.public.patient_x_risk_score_month_cb pxr
-- where pxr.year_cd = 'y-2022'
-- group by 1,2;


-- --Report 4 - Historcal DCE
-- select pxr.org_id,
-- pxr.year_cd as year,
-- Avg(pxr.risk_score) as raf_score
-- from local_marvinfoster.public.patient_x_risk_score_month_cb pxr
-- where pxr.year_cd <> 'y-2022'
-- group by 1,2;
 