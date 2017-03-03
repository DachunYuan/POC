# -*- coding: utf-8 -*-
"""
Created on Thu Dec 29 17:13:58 2016

@author: tanjianm
"""

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL for FI1 PMNT AP") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
query='''
SELECT 
'1051041' AS P_SRC_SYS_CD,
CONCAT(BSAK.BUKRS,BSAK.BELNR,BSAK.GJAHR,BSAK.BUZEI) AS PMNT_ID,
'AP' AS P_SB_LDGR_ACCT_TYP_CD,
BSAK.BLART AS ACCT_TRSN_TYP_CD,
BSAK.SHKZG AS ACCT_TRSN_ITM_ETRY_TYP_CD,
CONCAT(BSAK.BUKRS,BSAK.BELNR,BSAK.GJAHR) AS PMNT_TRSN_BTCH_ID,
CONCAT(LFB1.BUKRS,'---',LFB1.AKONT,'---') AS PMNT_GNRL_LDGR_ACCT_CD,
BSAK.BUDAT AS PMNT_VL_EFF_TS,
BSAK.SGTXT AS  PMNT_CMT,
T001.LAND1 AS CTRY_CD,
BSAK.BUKRS AS ENTRS_LGL_ENT_NR,
LEL.LGL_ENT_LDGR_ID AS LGL_ENT_LDGR_ID,
CONCAT(BSAK.LIFNR,'||VN') AS SB_LDGR_ACCT_ID,
fp.FSCL_PRD_ID AS FSCL_PRD_ID,
'PAYMENT DISBURSEMENT' AS PMNT_DRTN,
CASE WHEN  (TKA02.KOKRS IS NULL OR TRIM(TKA02.KOKRS) = '')  OR  (BSAK.PRCTR IS NULL OR TRIM(BSAK.PRCTR) = '')THEN  NULL ELSE CONCAT(TKA02.KOKRS, BSAK.PRCTR) END AS PFT_CNTR_CD,
CASE WHEN  (TKA02.KOKRS IS NULL OR TRIM(TKA02.KOKRS) = '')  OR  (BSAK.KOSTL IS NULL OR TRIM(BSAK.KOSTL) = '')THEN  NULL ELSE CONCAT(TKA02.KOKRS,BSAK.KOSTL) END AS PSTG_OBJ_NN_CNTRCT_ID,
NULL AS PSTG_OBJ_CNTRCT_ID,
CASE WHEN TRIM(T001.LAND1)='' OR TRIM(BSAK.ZLSCH)='' OR T001.LAND1 IS NULL OR  BSAK.ZLSCH IS NULL THEN NULL ELSE CONCAT(T001.LAND1,BSAK.ZLSCH) END AS PMNT_MTD_CGY_LG_CD,
BSAK.SGTXT AS PMNT_APLS_TO_CMT,
NULL AS PMNT_APLS_TO_INRN_DCMT_ID,
BSAK.WAERS AS PMNT_TRSN_CURR_CD,
T001.WAERS AS PMNT_LCL_CURR_CD,
'M' AS CURR_EXCH_RATE_TYP_CD,
case when BSAk.waers=t001.waers then 1.0 else TRSN_TO_LCL.CURR_DRVD_EXCH_RATE end AS PMNT_TRSN_TO_LCL_CURR_EXCH_RATE,
CASE WHEN BSAk.WAERS='USD' THEN 1.0 else TRSN_TO_GRP.CURR_DRVD_EXCH_RATE END AS PMNT_TRSN_TO_GRP_CURR_EXCH_RATE,
CASE WHEN  T001.WAERS = 'USD' then 1.0  ELSE  LCL_TO_GRP.CURR_DRVD_EXCH_RATE  END AS PMNT_LCL_TO_GRP_CURR_EXCH_RATE,
BSAK.DMBTR AS PMNT_LCL_CURR_AMT,
BSAK.WRBTR AS PMNT_TRSN_CURR_AMT,
BSAK.DMBE2 AS PMNT_GRP_CURR_AMT,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS RADAR_UPD_TS,
'ic_fi1_1050141_pmnt_ap' as RADAR_UPD_BY_PRS_ID,
'N' as RADAR_DLT_IND,
NULL AS PMNT_GNRL_LDGR_ACCT_TYP_DN,
'COMPANY' AS ENTRS_LGL_ENT_BSN_ROL_CD,
row_number() over (partition by CONCAT(BSAK.BUKRS,BSAK.BELNR,BSAK.GJAHR,BSAK.BUZEI) order by TRSN_TO_GRP.curr_from_dt ,TRSN_TO_LCL.curr_from_dt,LCL_TO_GRP.curr_from_dt) as rnk
from (SELECT * FROM 200836_az_fi1_1051041.BSAK  WHERE BSAK.BLART IN ('KD','KZ','ZP')
UNION ALL
SELECT * FROM 200836_az_fi1_1051041.BSIK  WHERE BSIK.BLART IN ('KD','KZ','ZP')
) BSAK 
JOIN 200836_az_fi1_1051041.T001	ON T001.BUKRS = BSAK.BUKRS 	
LEFT JOIN 200836_az_fi1_1051041.BKPF BKPF ON BKPF.BUKRS = BSAK.BUKRS AND BKPF.BELNR = BSAK.BELNR AND BKPF.GJAHR = BSAK.GJAHR	
LEFT JOIN (select * from 200836_az_fi1_1051041.TKA02	where  TKA02.GSBER = NULL OR TRIM(TKA02.GSBER) = '') as TKA02	ON  TKA02.BUKRS = BSAK.BUKRS 
LEFT JOIN 200836_az_fi1_1051041.LFB1	 ON LFB1.BUKRS = BSAK.BUKRS AND LFB1.LIFNR = BSAK.LIFNR	
LEFT JOIN  radar.LGL_ENT_LDGR  LEL ON T001.KTOPL =  LEL.LGL_ENT_LDGR_CD AND LEL.SRC_SYS_CD = '1051041'
LEFT JOIN radar.fscl_prd fp on BSAK.BUDAT between fp.fscl_prd_strt_ts and fp.fscl_prd_end_ts and fp.fscl_prd_sqn_nr <= 12
and fp.SRC_SYS_CD = '1051041' and fp.FSCL_PRD_YR_VRNT_TXT = T001.PERIV
and fp.FSCL_PRD_TYP_CD = 'Month'
LEFT JOIN 200836_az_fi1_1051041.stg_tcurf_tcurr TRSN_TO_GRP ON TRSN_TO_GRP.FROM_CURR=BSAK.WAERS AND TRSN_TO_GRP.TO_CURR='USD' and TRSN_TO_GRP.curr_from_dt<=BSAK.BUDAT
LEFT JOIN 200836_az_fi1_1051041.stg_tcurf_tcurr TRSN_TO_LCL ON TRSN_TO_LCL.FROM_CURR=BSAK.WAERS AND TRSN_TO_LCL.TO_CURR=T001.WAERS and TRSN_TO_LCL.curr_from_dt<=BSAK.BUDAT
LEFT JOIN 200836_az_fi1_1051041.stg_tcurf_tcurr LCL_TO_GRP ON LCL_TO_GRP.FROM_CURR=T001.WAERS AND LCL_TO_GRP.TO_CURR='USD' and LCL_TO_GRP.curr_from_dt<=BSAK.BUDAT
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
'AP' as sb_ldgr_acct_typ_cd
from temp
''').write.insertInto('radar.pmnt',overwrite=True)
