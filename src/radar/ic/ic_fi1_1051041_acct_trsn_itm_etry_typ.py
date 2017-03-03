################## new version ######################
# -*- coding: UTF-8 -*-
# @author: Yang, zhen-peng (Arvin)

## Spark Application - execute with spark-submit：spark-submit app.py

# Imports
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import types
from pyspark.sql import functions as func


# Module Constants
# note: Builder() and getOrCreate(), don't miss ()
APP_NAME = "ic_fi1_1051041_acct_trsn_itm_etry_typ"
spark = SparkSession.Builder() \
    .appName(APP_NAME) \
    .master("local") \
    .getOrCreate()


# Closure Functions


# Main functionality
def main(spark: SparkSession):

    df_acct_trsn_itm_etry_typ = sc.parallelize([ \
    	Row(P_SRC_SYS_CD='1051041', ACCT_TRSN_ITM_ETRY_TYP_CD='S', ACCT_TRSN_ITM_ETRY_TYP_NM='Debit', ACCT_TRSN_ITM_ETRY_TYP_DN='', RADAR_UPD_BY_PRS_ID='ic_fi1_1051041_acct_trsn_itm_etry_typ', RADAR_DLT_IND='N'), \
    	Row(P_SRC_SYS_CD='1051041', ACCT_TRSN_ITM_ETRY_TYP_CD='H', ACCT_TRSN_ITM_ETRY_TYP_NM='Credit', ACCT_TRSN_ITM_ETRY_TYP_DN='', RADAR_UPD_BY_PRS_ID='ic_fi1_1051041_acct_trsn_itm_etry_typ', RADAR_DLT_IND='N'), \
    	Row(P_SRC_SYS_CD='1051041', ACCT_TRSN_ITM_ETRY_TYP_CD='DR', ACCT_TRSN_ITM_ETRY_TYP_NM='Debit', ACCT_TRSN_ITM_ETRY_TYP_DN='', RADAR_UPD_BY_PRS_ID='ic_fi1_1051041_acct_trsn_itm_etry_typ', RADAR_DLT_IND='N'), \
    	Row(P_SRC_SYS_CD='1051041', ACCT_TRSN_ITM_ETRY_TYP_CD='CR', ACCT_TRSN_ITM_ETRY_TYP_NM='Credit', ACCT_TRSN_ITM_ETRY_TYP_DN='', RADAR_UPD_BY_PRS_ID='ic_fi1_1051041_acct_trsn_itm_etry_typ', RADAR_DLT_IND='N')]).toDF()
    	
    
    df_acct_trsn_itm_etry_typ.select('P_SRC_SYS_CD','ACCT_TRSN_ITM_ETRY_TYP_CD','ACCT_TRSN_ITM_ETRY_TYP_NM','ACCT_TRSN_ITM_ETRY_TYP_DN','RADAR_UPD_BY_PRS_ID','RADAR_DLT_IND',func.current_timestamp()).write.mode('owerwrite').insertinto('radar.acct_trsn_itm_etry_typ',overwrite=True)


if __name__ == '__main__':
    main(spark)

################## old version ######################
# Script name: ic_fi1_1051041_acct_trsn_itm_etry_typ.py
# Used to insert some hard code records into IC table radar.acct_trsn_itm_etry_typ
# Created By: zhen-peng.yang@hpe.com (Arvin)
# Created Date: 2016-12-22
# Comments:
# Chanded Date:
# Changed By:
# Changed Date:

spark.sql("set hive.support.concurrency=false")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

sql1 = "SELECT '1051041', 'S',  'Debit',  '', from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), 'ic_fi1_1051041_acct_trsn_itm_etry_typ', 'N'"
sql2 = "SELECT '1051041', 'H',  'Credit', '', from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), 'ic_fi1_1051041_acct_trsn_itm_etry_typ', 'N'"
sql3 = "SELECT '1051041', 'DR', 'Debit',  '', from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), 'ic_fi1_1051041_acct_trsn_itm_etry_typ', 'N'"
sql4 = "SELECT '1051041', 'CR', 'Credit', '', from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), 'ic_fi1_1051041_acct_trsn_itm_etry_typ', 'N'"

df_sql1 = spark.sql(sql1)
df_sql2 = spark.sql(sql2)
df_sql3 = spark.sql(sql3)
df_sql4 = spark.sql(sql4)

df_acct_trsn_itm_etry_typ = df_sql1.unionAll(df_sql2).unionAll(df_sql3).unionAll(df_sql4)
# for debug
df_acct_trsn_itm_etry_typ.show()

df_acct_trsn_itm_etry_typ.createOrReplaceTempView("acct_trsn_itm_etry_typ")
spark.sql("insert overwrite table radar.acct_trsn_itm_etry_typ partition (src_sys_cd = \'1051041\') select * from acct_trsn_itm_etry_typ")
# test data
spark.sql("select * from radar.acct_trsn_itm_etry_typ where src_sys_cd = 1051041").show();
