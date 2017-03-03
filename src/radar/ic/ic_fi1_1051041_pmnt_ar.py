
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 26 15:30:33 2016

@author: tanjianm
"""
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL for FI1 PMNT") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
query='''
SELECT 
'1051041' AS P_SRC_SYS_CD,
CONCAT(BSAD.BUKRS,BSAD.BELNR,BSAD.GJAHR,BSAD.BUZEI) AS PMNT_ID,
'AR' AS P_SB_LDGR_ACCT_TYP_CD,
BSAD.BLART AS acct_trsn_typ_cd,
BSAD.SHKZG as acct_trsn_itm_etry_typ_cd,
CONCAT(BSAD.BUKRS,BSAD.BELNR,BSAD.GJAHR) as pmnt_trsn_btch_id,
concat(KNB1.BUKRS,'---',KNB1.AKONT,'---') as pmnt_gnrl_ldgr_acct_cd,
BSAD.BLDAT AS pmnt_vl_eff_ts,
BSAD.SGTXT AS pmnt_cmt,
T001.LAND1 AS ctry_cd,
BSAD.BUKRS AS entrs_lgl_ent_nr,
LEL.LGL_ENT_LDGR_ID AS lgl_ent_ldgr_id,
BSAD.KUNNR AS sb_ldgr_acct_id,
FP.FSCL_PRD_ID AS fscl_prd_id,
'PAYMENT RECEIPT' as pmnt_drtn,
CASE WHEN TK.KOKRS IS NULL OR TRIM(TK.KOKRS)='' OR BSAD.PRCTR IS NULL OR TRIM(BSAD.PRCTR)='' THEN NULL ELSE CONCAT(TK.KOKRS,BSAD.PRCTR) END as pft_cntr_cd,
CASE WHEN TK.KOKRS IS NULL OR TRIM(TK.KOKRS)='' OR BSAD.KOSTL IS NULL OR TRIM(BSAD.KOSTL)='' THEN NULL ELSE CONCAT(TK.KOKRS,BSAD.KOSTL) END as pstg_obj_nn_cntrct_id,
NULL AS pstg_obj_cntrct_id,
NULL AS pmnt_mtd_cgy_lg_cd,
BSAD.SGTXT AS pmnt_apls_to_cmt,
NULL AS pmnt_apls_to_inrn_dcmt_id,
BSAD.WAERS AS pmnt_trsn_curr_cd,
T001.WAERS AS pmnt_lcl_curr_cd,
'M' AS curr_exch_rate_typ_cd,
CASE WHEN TRSN_TO_LCL.curr_drvd_exch_rate<0 THEN 1/TRSN_TO_LCL.curr_drvd_exch_rate ELSE TRSN_TO_LCL.curr_drvd_exch_rate end as pmnt_trsn_to_lcl_curr_exch_rate,
CASE WHEN BSAD.WAERS='USD' THEN 1.0 WHEN TRSN_TO_GRP.curr_drvd_exch_rate<0 then 1/TRSN_TO_GRP.curr_drvd_exch_rate else TRSN_TO_GRP.curr_drvd_exch_rate end as pmnt_trsn_to_grp_curr_exch_rate,
null as pmnt_lcl_to_grp_curr_exch_rate,
BSAD.DMBTR as pmnt_lcl_curr_amt,
BSAD.WRBTR as pmnt_trsn_curr_amt,
BSAD.DMBE2 as pmnt_grp_curr_amt,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS radar_UPD_TS,
'ic_fi1_1050141_pmnt' as radar_UPD_BY_PRS_ID,
'N' as radar_DLT_IND,
null as pmnt_gnrl_ldgr_acct_typ_dn,
'COMPANY' as entrs_lgl_ent_bsn_rol_cd,
row_number() over (partition by CONCAT(BSAD.BUKRS,BSAD.BELNR,BSAD.GJAHR,BSAD.BUZEI) order by TRSN_TO_GRP.curr_from_dt,TRSN_TO_LCL.curr_from_dt) rnk
FROM 
(SELECT * FROM 200836_az_fi1_1051041.BSAD WHERE BLART IN ('DZ', 'ZP', 'CT', 'CZ', 'DZ', 'FZ', 'PA', 'ZP', 'ZU')
UNION ALL
SELECT * FROM 200836_az_fi1_1051041.BSID WHERE BLART IN ('DZ', 'ZP', 'CT', 'CZ', 'DZ', 'FZ', 'PA', 'ZP', 'ZU')
) BSAD 
INNER JOIN 200836_az_fi1_1051041.T001 ON T001.BUKRS = BSAD.BUKRS
INNER JOIN 200836_az_fi1_1051041.KNB1 ON KNB1.BUKRS = BSAD.BUKRS AND KNB1.KUNNR = BSAD.KUNNR
AND (KNB1.AKONT IS NOT NULL AND KNB1.AKONT <> '')
LEFT JOIN radar.LGL_ENT_LDGR LEL ON LEL.LGL_ENT_LDGR_CD = T001.KTOPL and LEL.SRC_SYS_CD = '1051041'
LEFT JOIN radar.fscl_prd fp on  BSAD.BUDAT between fp.fscl_prd_strt_ts and fp.fscl_prd_end_ts and fp.fscl_prd_sqn_nr <= 12
and fp.SRC_SYS_CD = '1051041' and fp.FSCL_PRD_YR_VRNT_TXT = T001.PERIV
and fp.FSCL_PRD_TYP_CD = 'Month'
LEFT JOIN (SELECT * FROM 200836_az_fi1_1051041.TKA02 WHERE GSBER IS null or trim(GSBER)='') TK ON TK.BUKRS = BSAD.BUKRS 
LEFT JOIN 200836_az_fi1_1051041.stg_tcurf_tcurr TRSN_TO_GRP ON TRSN_TO_GRP.FROM_CURR=BSAD.WAERS AND TRSN_TO_GRP.TO_CURR='USD' and TRSN_TO_GRP.curr_from_dt<=BSAD.BUDAT
LEFT JOIN 200836_az_fi1_1051041.stg_tcurf_tcurr TRSN_TO_LCL ON TRSN_TO_LCL.FROM_CURR=BSAD.WAERS AND TRSN_TO_LCL.TO_CURR=T001.WAERS and TRSN_TO_LCL.curr_from_dt<=BSAD.BUDAT
'''
temp=spark.sql(query)
temp.filter(temp['rnk']==1).createOrReplaceTempView('temp')
spark.sql('''
select 
p_src_sys_cd,
pmnt_id,
p_sb_ldgr_acct_typ_cd,
acct_trsn_typ_cd,
acct_trsn_itm_etry_typ_cd,
pmnt_trsn_btch_id,
pmnt_gnrl_ldgr_acct_cd,
pmnt_vl_eff_ts,
pmnt_cmt,
ctry_cd,
entrs_lgl_ent_nr,
lgl_ent_ldgr_id,
sb_ldgr_acct_id,
fscl_prd_id,
pmnt_drtn,
pft_cntr_cd,
pstg_obj_nn_cntrct_id,
pstg_obj_cntrct_id,
pmnt_mtd_cgy_lg_cd,
pmnt_apls_to_cmt,
pmnt_apls_to_inrn_dcmt_id,
pmnt_trsn_curr_cd,
pmnt_lcl_curr_cd,
curr_exch_rate_typ_cd,
pmnt_trsn_to_lcl_curr_exch_rate,
pmnt_trsn_to_grp_curr_exch_rate,
pmnt_lcl_to_grp_curr_exch_rate,
pmnt_lcl_curr_amt,
pmnt_trsn_curr_amt,
pmnt_grp_curr_amt,
radar_upd_ts,
radar_upd_by_prs_id,
radar_dlt_ind,
pmnt_gnrl_ldgr_acct_typ_dn,
entrs_lgl_ent_bsn_rol_cd,
'1051041' as src_sys_cd,
'AR' as sb_ldgr_acct_typ_cd
from temp
''').write.insertInto('radar.pmnt',overwrite=True)

