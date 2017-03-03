# -*- coding: UTF-8 -*-

# Script Name: ic_fi1_1051041_gnrl_ldgr_acct_typ.hql
# Description: Used to load data into IC table gnrl_ldgr_acct_typ.
# Created By: Fred
# Created Date: 12/29/2016
# Comments:
# Changed Date:
# Changed By:
# Changed Date:

from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL for FI1 GNRL_LDGR_ACCT_TYP") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

df_src1 = spark.sql("""select distinct
concat(BUKRS,FSTAG) as GNRL_LDGR_ACCT_TYP_CD,
FSTTX as GNRL_LDGR_ACCT_TYP_NM,
NULL as GNRL_LDGR_ACCT_TYP_DN
from 200836_az_fi1_1051041.T004G
where SPRAS = 'E'""")

df_src2 = spark.sql("""select distinct 
concat(SKB1.BUKRS,SKB1.FSTAG) AS GNRL_LDGR_ACCT_TYP_CD,
'MANUALY INSERTED' as GNRL_LDGR_ACCT_TYP_NM,
'DELETED MASTER DATA RECORD - MANUALLY INSERTED BY RADAR' as GNRL_LDGR_ACCT_TYP_DN
from 200836_az_fi1_1051041.SKB1 
left join 200836_az_fi1_1051041.T004G on concat(T004G.BUKRS,T004G.FSTAG)=concat(SKB1.BUKRS,SKB1.FSTAG) and T004G.SPRAS='E'
where T004G.MANDT is null
""")


df_tmp = df_src1.union(df_src2)

df_tmp.createOrReplaceTempView("tmp_gnrl_ldgr_acct_typ")

df_tar = spark.sql("""select
'1051041' as p_src_sys_cd,
GNRL_LDGR_ACCT_TYP_CD,
GNRL_LDGR_ACCT_TYP_NM,
GNRL_LDGR_ACCT_TYP_DN,
'2' as GNRL_LDGR_ACCT_TYP_LVL_NR,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as RADAR_UPD_TS,
'ic_fi1_1051041_gnrl_ldgr_acct_typ' as RADAR_UPD_BY_PRS_ID,
'N' as RADAR_DLT_IND,
'1051041' as SRC_SYS_CD
from tmp_gnrl_ldgr_acct_typ
""")

#df_tar.write.mode('overwrite').insertInto("radar.gnrl_ldgr_acct_typ",overwrite=True)

df_tar.write.insertInto("radar.gnrl_ldgr_acct_typ",overwrite=True)

#df_tar.show(10)
