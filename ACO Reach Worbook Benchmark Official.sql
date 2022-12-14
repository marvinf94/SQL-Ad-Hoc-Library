select PROVIDER_TYPE
,PROVIDER_NPI
,PROVIDER_NAME
,ORGANIZATION_NPI
,ORGANIZATION_NAME
,BENE_COUNTY_CD
,BENE_COUNTY_NAME
,BENE_STATE
,BENE_COHORT
,SSP_TYPE
,BY2017_BENE_CNT_ALIGNED
,BY2018_BENE_CNT_ALIGNED
,BY2019_BENE_CNT_ALIGNED
,CY2017_BENE_CNT_ALIGNED_ELIGIBLE
,CY2018_BENE_CNT_ALIGNED_ELIGIBLE
,CY2019_BENE_CNT_ALIGNED_ELIGIBLE
,CY2017_BENE_TOTAL_MEMBER_MONTHS
,CY2018_BENE_TOTAL_MEMBER_MONTHS
,CY2019_BENE_TOTAL_MEMBER_MONTHS
,CY2017_PARTB_SPEND
,CY2017_INPATIENT_SPEND
,CY2017_OUTPATIENT_SPEND
,CY2017_HHA_SPEND
,CY2017_SNF_SPEND
,CY2017_HOSPICE_SPEND
,CY2017_DME_SPEND
,CY2017_TOTAL_SPEND
,CY2018_PARTB_SPEND
,CY2018_INPATIENT_SPEND
,CY2018_OUTPATIENT_SPEND
,CY2018_HHA_SPEND
,CY2018_SNF_SPEND
,CY2018_HOSPICE_SPEND
,CY2018_DME_SPEND
,CY2018_TOTAL_SPEND
,CY2019_PARTB_SPEND
,CY2019_INPATIENT_SPEND
,CY2019_OUTPATIENT_SPEND
,CY2019_HHA_SPEND
,CY2019_SNF_SPEND
,CY2019_HOSPICE_SPEND
,CY2019_DME_SPEND
,CY2019_TOTAL_SPEND
,CY2017_SUM_HCC
,CY2018_SUM_HCC
,CY2019_SUM_HCC
,CY2017_SUM_ADJ_HCC
,CY2018_SUM_ADJ_HCC
,CY2019_SUM_ADJ_HCC
,GAF_TREND_2017
,GAF_TREND_2018
,GAF_TREND_2019
,COUNTY_RATE
,STATE_RATE
,GROUP_LEVEL_1_ID
,GROUP_LEVEL_1_NAME
,GROUP_LEVEL_2_ID
,GROUP_LEVEL_2_NAME
,GROUP_LEVEL_3_ID
,GROUP_LEVEL_3_NAME
,NETWORK_1_ID
,NETWORK_1_NAME
from "PROD_NETADVDEMO_FE"."VRDC"."DC_PROVIDER_BENCHMARK_NETWORK_LAYUP" 
where PROVIDER_NPI in
()