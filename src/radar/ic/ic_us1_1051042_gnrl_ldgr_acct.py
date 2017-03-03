# -*- coding: UTF-8 -*-

# Script Name: ic_us1_1051042_gnrl_ldgr_acct.hql
# Description: Used to load data into IC table gnrl_ldgr_acct.
# Created By: Fred
# Created Date: 12/23/2016
# Comments:
# Changed Date:
# Changed By:
# Changed Date:

from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL for us1 GNRL_LDGR_ACCT") \
    .enableHiveSupport() \
	.master("local") \
    .getOrCreate()


spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

df_src = spark.sql("""select distinct 
concat(SKB1.BUKRS,'---',SKB1.SAKNR,'---') as GNRL_LDGR_ACCT_ID,
c.TXT20 as GNRL_LDGR_ACCT_NM,c.TXT50 as GNRL_LDGR_ACCT_DN,
concat(SKB1.BUKRS,SKB1.FSTAG) as GNRL_LDGR_ACCT_TYP_CD
from 200836_az_us1_1051042.SKB1
inner join (select KTOPL,BUKRS
from 200836_az_us1_1051042.T001 where BUKRS <> 'IN10') t on t.BUKRS = SKB1.BUKRS
left join (select MANDT,SAKNR,TXT20,TXT50,KTOPL from 200836_az_us1_1051042.SKAT where SPRAS = 'E') c on c.SAKNR = SKB1.SAKNR and c.KTOPL= t.KTOPL""")

df_src.createOrReplaceTempView("src")



df_tar1 = spark.sql("""
SELECT '1051042' as p_src_sys_cd,
GNRL_LDGR_ACCT_ID,
GNRL_LDGR_ACCT_ID as GNRL_LDGR_ACCT_CD,
GNRL_LDGR_ACCT_NM,
GNRL_LDGR_ACCT_DN,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as RADAR_UPD_TS,
'ic_us1_1051042_gnrl_ldgr_acct' as RADAR_UPD_BY_PRS_ID,
'N' as RADAR_DLT_IND,
GNRL_LDGR_ACCT_TYP_CD,
'1051042' as SRC_SYS_CD
FROM src""")

df_tar1.write.mode('overwrite').insertInto("radar.gnrl_ldgr_acct",overwrite=True)


#check data
#spark.sql("select * from radar.gnrl_ldgr_acct where src_sys_cd = 1051042").show(10);

#spark.sql("select count(*) from radar.gnrl_ldgr_acct where src_sys_cd = 1051042").show();


