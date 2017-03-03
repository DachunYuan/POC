
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL for FI1 FSCL_PRD") \
    .enableHiveSupport() \
    .getOrCreate()
sc=spark.sparkContext
sf=sc.textFile('/tmp/FI1_FSCL_PRD.txt')
sfd=sf.map(lambda line:line.split('|'))

schemaString='''fscl_prd_id
src_sys_cd
fscl_prd_sqn_nr
fscl_prd_typ_cd
fscl_prd_nm
fscl_prd_dn
fscl_prd_strt_ts
fscl_prd_end_ts
fscl_prd_yr_nr
fscl_yr_strt_dt
fscl_prd_yr_hf_nr
fscl_prd_yr_qtr_nr
fscl_prd_yr_mth_nr
fscl_prd_yr_vrnt_txt'''
fields=[StructField(name,StringType(), True) for name in schemaString.split('\n')]
schema=StructType(fields)
df=spark.createDataFrame(sfd,schema)
df.createOrReplaceTempView('fp')
sqlStatement='''
select 
'1051041' as p_src_sys_cd,
fscl_prd_id,
fscl_prd_sqn_nr,
fscl_prd_yr_nr,
fscl_prd_typ_cd,
fscl_prd_nm,
fscl_prd_dn,
from_unixtime(unix_timestamp(fscl_prd_strt_ts,'MM-dd-yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as fscl_prd_strt_ts,
from_unixtime(unix_timestamp(fscl_prd_end_ts,'MM-dd-yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as fscl_prd_end_ts,
from_unixtime(unix_timestamp(fscl_yr_strt_dt,'MM-dd-yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as fscl_yr_strt_dt,
fscl_prd_yr_hf_nr,
fscl_prd_yr_qtr_nr,
fscl_prd_yr_mth_nr,
current_timestamp() as radar_upd_ts,
'N' as radar_dlt_ind,
'ic_fi1_1051041_fscl_prd.py' as radar_upd_by_prs_id,
fscl_prd_yr_vrnt_txt,
 '1051041' as src_sys_cd
from fp
'''
result=spark.sql(sqlStatement)
result.createOrReplaceTempView('result')
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("insert overwrite table radar.fscl_prd partition (src_sys_cd) select * from result")
