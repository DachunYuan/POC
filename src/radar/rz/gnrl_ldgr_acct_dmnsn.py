# -*- coding: UTF-8 -*-

# Script Name: gnrl_ldgr_acct_dmnsn.py
# Description: Used to load IC table data to RZ table gnrl_ldgr_acct_dmnsn
# Created By: Fred
# Created Date: 12/30/2016
# Changed Date:
# Changed By:
# Comments:

from pyspark.sql.types import *
from pyspark.sql import SparkSession

APP_NAME = "gnrl_ldgr_acct_trsn_atr_dmnsn"

spark = SparkSession \
    .builder \
    .appName(APP_NAME) \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("hive.exec.reducers.max", "1") \
    .config("spark.sql.crossJoin.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()
	
#spark.sql("drop temporary function gnrl_ldgr_acct_dmnsn_row_sequence")
spark.sql("create temporary function gnrl_ldgr_acct_dmnsn_row_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence'")
	
df_hardcode = spark.sql("SELECT '-1051041' AS GNRL_LDGR_ACCT_KY,'?' AS GNRL_LDGR_ACCT_CD,'?' AS GNRL_LDGR_ACCT_ID,'?' AS GNRL_LDGR_ACCT_TYP_CD," \
                       "'?' AS GNRL_LDGR_NTRL_ACCT_NR,'NO VALUE' AS GNRL_LDGR_ACCT_TYP_NM,'NO VALUE' AS GNRL_LDGR_ACCT_TYP_DN,'NO VALUE' AS GNRL_LDGR_ACCT_NM," \
                       "'1051041' AS P_SRC_SYS_CD,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS RADAR_UPD_TS,'ETL_Initial' AS RADAR_UPD_BY_PRS_ID,'N' AS RADAR_DLT_IND,'1051041' AS SRC_SYS_CD")

df_src = spark.sql("""SELECT 
                   cast( rpad(ACCT.SRC_SYS_CD,18,'0') as bigint )+gnrl_ldgr_acct_dmnsn_row_sequence() AS GNRL_LDGR_ACCT_KY,
                   NVL(ACCT.GNRL_LDGR_ACCT_CD,'?') AS GNRL_LDGR_ACCT_CD,
                   NVL(ACCT.GNRL_LDGR_ACCT_ID,'?') AS GNRL_LDGR_ACCT_ID,
                   NVL(ACCT.GNRL_LDGR_ACCT_TYP_CD,'?') AS GNRL_LDGR_ACCT_TYP_CD,
                    CASE WHEN ACCT.GNRL_LDGR_ACCT_CD<>'?'
                    THEN split(gnrl_ldgr_acct_cd,'---')[1] 
                    END
                   AS GNRL_LDGR_NTRL_ACCT_NR,
                    CASE WHEN ACCT_TYP.SRC_SYS_CD='200115' or ACCT_TYP.SRC_SYS_CD='200164' THEN ACCT_TYP.GNRL_LDGR_ACCT_TYP_DN 
                    ELSE COALESCE(ACCT_TYP.GNRL_LDGR_ACCT_TYP_NM,ACCT_TYP.GNRL_LDGR_ACCT_TYP_DN,'NO VALUE')
                    END
                   AS GNRL_LDGR_ACCT_TYP_NM,
                   COALESCE(ACCT_TYP.GNRL_LDGR_ACCT_TYP_DN,ACCT_TYP.GNRL_LDGR_ACCT_TYP_NM,'NO VALUE') AS GNRL_LDGR_ACCT_TYP_DN,
                   COALESCE(ACCT.GNRL_LDGR_ACCT_NM, ACCT.GNRL_LDGR_ACCT_DN,'NO VALUE') AS GNRL_LDGR_ACCT_NM,
                   '1051041' AS P_SRC_SYS_CD,
                   from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS RADAR_UPD_TS,
                   'gnrl_ldgr_acct_dmnsn' AS RADAR_UPD_BY_PRS_ID,
                   'N' AS RADAR_DLT_IND,
				   '1051041' AS SRC_SYS_CD
                   FROM radar.GNRL_LDGR_ACCT ACCT left join radar.GNRL_LDGR_ACCT_TYP ACCT_TYP
                   on ACCT_TYP.GNRL_LDGR_ACCT_TYP_CD=ACCT.GNRL_LDGR_ACCT_TYP_CD and ACCT_TYP.SRC_SYS_CD = ACCT.SRC_SYS_CD
                   where acct.src_sys_cd='1051041'""")

df_tar = df_src.union(df_hardcode).drop_duplicates()

df_tar.write.mode('overwrite').insertInto("radar_rz.gnrl_ldgr_acct_dmnsn",overwrite=True)

