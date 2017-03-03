# -*- coding: UTF-8 -*-
# @author: Qian,Hong (Anho)

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql import types

################################################################
# Constraints
################################################################
APP_NAME = "gnrl_ldgr_acct_trsn_atr_dmnsn"
az_schema_name = "200836_az_fi1_1051041"
ic_schema_name = "radar"
rz_schema_name = "radar_rz"
table_name = "gnrl_ldgr_acct_trsn_atr_dmnsn"
source_system_code = "1051041"
stg_gnrl_ldgr_acct_trsn_atr_dmnsn_query='''SELECT 
SRC.CURR_EXCH_RATE_TYP_CD,
SRC.CURR_EXCH_RATE_TYP_NM,
SRC.CURR_EXCH_RATE_TYP_DN,
SRC.ACCT_TRSN_TYP_CD,
SRC.ACCT_TRSN_TYP_NM,
SRC.ACCT_TRSN_TYP_DN,
SRC.ACCT_TRSN_ITM_ETRY_TYP_CD,
SRC.ACCT_TRSN_ITM_ETRY_TYP_NM,
SRC.ACCT_TRSN_BCKPST_IND,
SRC.RVRSL_RSN_CD,
SRC.RVRSL_RSN_NM,
SRC.RVRSL_RSN_DN,
SRC.SRC_SYS_CD AS P_SRC_SYS_CD,
SRC.SRC_SYS_NM,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS RADAR_UPD_TS,
'rz_gnrl_ldgr_acct_trsn_atr_dmnsn.hql' AS RADAR_UPD_BY_PRS_ID,
'N' AS RADAR_DLT_IND,
SRC.SRC_SYS_CD
FROM (SELECT DISTINCT
NVL(ATTRS.CURR_EXCH_RATE_TYP_CD,'?') AS CURR_EXCH_RATE_TYP_CD,
COALESCE(CERT.CURR_EXCH_RATE_TYP_NM,CERT.CURR_EXCH_RATE_TYP_DN,'NO VALUE') AS CURR_EXCH_RATE_TYP_NM,
COALESCE(CERT.CURR_EXCH_RATE_TYP_DN,CERT.CURR_EXCH_RATE_TYP_NM,'NO VALUE') AS CURR_EXCH_RATE_TYP_DN,
NVL(ATTRS.ACCT_TRSN_TYP_CD,'?') AS ACCT_TRSN_TYP_CD,
COALESCE(ATT.ACCT_TRSN_TYP_NM, ATT.ACCT_TRSN_TYP_DN,'NO VALUE') AS ACCT_TRSN_TYP_NM,
COALESCE(ATT.ACCT_TRSN_TYP_DN,ATT.ACCT_TRSN_TYP_NM,'NO VALUE') AS ACCT_TRSN_TYP_DN,
NVL((CASE WHEN ATI.ACCT_TRSN_ITM_ETRY_TYP_CD='S' THEN 'DR'
          WHEN ATI.ACCT_TRSN_ITM_ETRY_TYP_CD='H' THEN 'CR'
          ELSE ATI.ACCT_TRSN_ITM_ETRY_TYP_CD END),'NO VALUE') AS ACCT_TRSN_ITM_ETRY_TYP_CD,
COALESCE(ATIET.ACCT_TRSN_ITM_ETRY_TYP_NM,ATIET.ACCT_TRSN_ITM_ETRY_TYP_DN,'NO VALUE') AS ACCT_TRSN_ITM_ETRY_TYP_NM,
NVL((CASE WHEN ATTRS.ACCT_TRSN_BCKPST_IND='X' THEN  'Y' ELSE 'N' END),'?') AS  ACCT_TRSN_BCKPST_IND,
(CASE WHEN NVL(ATTRS.RVRSL_RSN_CD,'')='' THEN '?' ELSE ATTRS.RVRSL_RSN_CD END) AS RVRSL_RSN_CD,
COALESCE(RR.RVRSL_RSN_NM, RR.RVRSL_RSN_DN,'NO VALUE') AS RVRSL_RSN_NM,
COALESCE(RR.RVRSL_RSN_DN, RR.RVRSL_RSN_NM,'NO VALUE') AS RVRSL_RSN_DN,
NVL(SS.SRC_SYS_NM,'NO VALUE') AS SRC_SYS_NM,
ATTRS.SRC_SYS_CD
FROM {ic_schema_name}.ACCT_TRSN ATTRS
INNER JOIN {ic_schema_name}.ACCT_TRSN_ITM ATI ON ATTRS.ACCT_TRSN_ID=ATI.ACCT_TRSN_ID AND ATTRS.SRC_SYS_CD=ATI.SRC_SYS_CD
LEFT JOIN {ic_schema_name}.RVRSL_RSN RR ON ATTRS.RVRSL_RSN_CD=RR.RVRSL_RSN_CD AND ATTRS.SRC_SYS_CD=RR.SRC_SYS_CD
LEFT JOIN {ic_schema_name}.CURR_EXCH_RATE_TYP CERT ON ATTRS.CURR_EXCH_RATE_TYP_CD =CERT.CURR_EXCH_RATE_TYP_CD AND CERT.SRC_SYS_CD = ATTRS.SRC_SYS_CD
LEFT JOIN {ic_schema_name}.ACCT_TRSN_TYP ATT ON ATTRS.ACCT_TRSN_TYP_CD =ATT.ACCT_TRSN_TYP_CD AND ATT.SRC_SYS_CD = ATTRS.SRC_SYS_CD
LEFT JOIN {ic_schema_name}.ACCT_TRSN_ITM_ETRY_TYP ATIET ON ATI.ACCT_TRSN_ITM_ETRY_TYP_CD=ATIET.ACCT_TRSN_ITM_ETRY_TYP_CD AND ATIET.SRC_SYS_CD = ATI.SRC_SYS_CD
LEFT JOIN {ic_schema_name}.SRC_SYS SS ON SS.SRC_SYS_CD = ATTRS.SRC_SYS_CD 
WHERE ATTRS.SRC_SYS_CD = '{source_system_code}')SRC '''
hardcode_query = '''SELECT 
-{source_system_code} AS ACCT_TRSN_ATR_KY,
'?' AS CURR_EXCH_RATE_TYP_CD,
'NO VALUE' AS CURR_EXCH_RATE_TYP_NM,
'NO VALUE' AS CURR_EXCH_RATE_TYP_DN,
'?' AS ACCT_TRSN_TYP_CD,
'NO VALUE' AS ACCT_TRSN_TYP_NM,
'NO VALUE' AS ACCT_TRSN_TYP_DN,
'?' AS ACCT_TRSN_ITM_ETRY_TYP_CD,
'NO VALUE' AS ACCT_TRSN_ITM_ETRY_TYP_NM,
'N' AS ACCT_TRSN_BCKPST_IND,
'?' AS RVRSL_RSN_CD,
'NO VALUE' AS RVRSL_RSN_NM,
'NO VALUE' AS RVRSL_RSN_DN,
'{source_system_code}' P_SRC_SYS_CD,
SS.SRC_SYS_NM,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS RADAR_UPD_TS,
'rz_gnrl_ldgr_acct_trsn_atr_dmnsn.hql' AS RADAR_UPD_BY_PRS_ID,
'N' AS RADAR_DLT_IND,
'{source_system_code}' AS SRC_SYS_CD
FROM {ic_schema_name}.SRC_SYS SS WHERE SS.SRC_SYS_CD='{source_system_code}' '''


def main(): 
    
   spark = SparkSession \
        .builder \
        .appName(APP_NAME) \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
		.config("hive.exec.reducers.max", "1") \
		.config("hive.support.concurrency", "false") \
        .enableHiveSupport() \
        .getOrCreate()

    # Define data frame based on one Hive SQL
   spark.sql("create temporary function gnrl_ldgr_acct_trsn_atr_dmnsn_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence'")
   df_hardcode=spark.sql(hardcode_query.format(source_system_code=source_system_code,ic_schema_name=ic_schema_name))
   df_stg=spark.sql(stg_gnrl_ldgr_acct_trsn_atr_dmnsn_query.format(source_system_code=source_system_code,ic_schema_name=ic_schema_name))
   df_stg.createOrReplaceTempView("SRC")
   df_src='''SELECT 
			cast( rpad(SRC.SRC_SYS_CD,18,'0') as bigint )+gnrl_ldgr_acct_trsn_atr_dmnsn_sequence() AS ACCT_TRSN_ATR_KY,
			SRC.CURR_EXCH_RATE_TYP_CD,
			SRC.CURR_EXCH_RATE_TYP_NM,
			SRC.CURR_EXCH_RATE_TYP_DN,
			SRC.ACCT_TRSN_TYP_CD,
			SRC.ACCT_TRSN_TYP_NM,
			SRC.ACCT_TRSN_TYP_DN,
			SRC.ACCT_TRSN_ITM_ETRY_TYP_CD,
			SRC.ACCT_TRSN_ITM_ETRY_TYP_NM,
			SRC.ACCT_TRSN_BCKPST_IND,
			SRC.RVRSL_RSN_CD,
			SRC.RVRSL_RSN_NM,
			SRC.RVRSL_RSN_DN,
			SRC.P_SRC_SYS_CD,
			SRC.SRC_SYS_NM,
			from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS RADAR_UPD_TS,
			'rz_gnrl_ldgr_acct_trsn_atr_dmnsn.hql' AS RADAR_UPD_BY_PRS_ID,
			'N' AS RADAR_DLT_IND,
			SRC.SRC_SYS_CD
			FROM SRC WHERE SRC.SRC_SYS_CD='{source_system_code}' '''
   sql_df=spark.sql(df_src.format(source_system_code=source_system_code)).unionAll(df_hardcode).drop_duplicates()	

    # Overwrite partition dynamically  based on resulting data frame	
   sql_df.write.insertInto("{}.{}".format(rz_schema_name, table_name), overwrite=True)
   spark.sql("drop temporary function gnrl_ldgr_acct_trsn_atr_dmnsn_sequence")

   
if __name__ == '__main__':
    main()
