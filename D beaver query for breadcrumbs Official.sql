USE WAREHOUSE pantry;
USE DATABASE pantry;

CREATE OR REPLACE TABLE pantry.sndbx.a2024_breadcrumbs_members_2021 (
Bene_ID STRING,
Patient_ID_type STRING,
First_Name STRING,
Last_Name STRING,
Gender STRING,
Date_of_Birth DATE,
Clinic_ID STRING,
Provider_1_NPI STRING,
Provider_2_NPI STRING,
Provider_3_NPI STRING,
RankCARE_2 NUMBER,
DM_Rank NUMBER,
RankHTN_2 NUMBER,
MH1_Rank NUMBER,
Prev5_rank NUMBER,
Prev6_rank NUMBER,
Prev7_rank NUMBER,
Prev10_rank NUMBER,
Prev12_rank NUMBER,
Prev13_rank NUMBER
)

STAGE_FILE_FORMAT = (TYPE = 'csv'
					 RECORD_DELIMITER='\\n'
					 FIELD_DELIMITER= ','
					 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
					 skip_header = 1);
					
put file://D:\Users\marvin.foster\Documents\Breadcrumbs_a2024_DVACO.csv @sndbx.%a2024_breadcrumbs_members_2021 ;
COPY INTO pantry.sndbx.a2024_breadcrumbs_members_2021;



--------- Paste into Dbeaver to ensure data has been successfully copied into the table

--SELECT * from pantry.sndbx.a2024_breadcrumbs_members_2021;

