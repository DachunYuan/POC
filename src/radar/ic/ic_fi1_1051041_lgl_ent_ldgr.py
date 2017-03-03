# Script Name: ic_fi1_1051041_lgl_ent_ldgr.py
# Description: Used to load data into IC table lgl_ent_ldgr.
# Created By: Anho
# Created Date: 05/31/2016
# Comments:
# Changed Date:
# Changed By:
# Changed Date:

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import types
from pyspark.sql import functions as func

APP_NAME = "ic_fi1_1051041_lgl_ent_ldgr"
spark = SparkSession \
	.Builder() \
    .appName(APP_NAME) \
    .config("hive.support.concurrency", "false") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .master("yarn") \
	.enableHiveSupport() \
    .getOrCreate()

#spark.sql("set hive.support.concurrency=false")
#spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("CREATE TEMPORARY FUNCTION lgl_ent_ldgr_row_sequence AS 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence' ")

var_sql = """ INSERT OVERWRITE TABLE radar.lgl_ent_ldgr PARTITION(SRC_SYS_CD='1051041') select '1051041' as p_src_sys_cd,lgl_ent_ldgr_row_sequence() as LGL_ENT_LDGR_ID,T004T.KTOPL as LGL_ENT_LDGR_CD,NULL as LGL_ENT_LDGR_DN,T004T.KTPLT as LGL_ENT_LDGR_NM,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as RADAR_UPD_TS,'ic_fi1_1051041_lgl_ent_ldgr' as RADAR_UPD_BY_PRS_ID,'N' as RADAR_DLT_IND from 200836_az_fi1_1051041.T004T where SPRAS = 'E' """

spark.sql(var_sql)

spark.sql("select * from radar.lgl_ent_ldgr limit 1").show()
