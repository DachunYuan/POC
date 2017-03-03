# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 11:18:04 2016

@author: tanjianm
"""

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL for RZ ACCT_PRD_DMNSN") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sql("add jar /opt/apache-hive-1.2.1-bin/lib/hive-contrib-1.2.1.jar")
spark.sql("create temporary function acct_prd_dmnsn_row_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence'")
fp=spark.sql("select * from radar.fscl_prd where src_sys_cd='1051041'")
fp.createOrReplaceTempView('fp')
ss=spark.sql("select * from radar.src_sys where src_sys_cd='1051041'")
ss.createOrReplaceTempView('ss')
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
query='''select 
ss.src_sys_sqn_strt_nr,
fp.fscl_prd_yr_nr as fscl_prd_yr_nr,
nvl(fp.fscl_prd_sqn_nr,0) as fscl_prd_sqn_nr,
fp.fscl_prd_typ_cd,
fp.fscl_prd_nm,
case when trim(fp.fscl_prd_dn) is null or trim(fp.fscl_prd_dn)='' then 'NO VALUE' else fp.fscl_prd_dn end as fscl_prd_dn,
fp.fscl_prd_strt_ts,
fp.fscl_prd_end_ts,
fp.fscl_yr_strt_dt,
case when fp.fscl_prd_yr_hf_nr is null 
then 
        (
        case when fp.fscl_prd_yr_qtr_nr is null then 0
        when fp.fscl_prd_yr_qtr_nr in ('1','2') then '1'
        else '2'
        end
        )
else fp.fscl_prd_yr_hf_nr
end as fscl_prd_yr_hf_nr,
nvl(fp.fscl_prd_yr_qtr_nr,'0') as fscl_prd_yr_qtr_nr,
nvl(fp.fscl_prd_yr_mth_nr,'0') as fscl_prd_yr_mth_nr,
'1051041' as p_src_sys_cd,
current_timestamp() as radar_upd_ts,
'rz_acct_prd_dmnsn.py' as radar_upd_by_prs_id,
'N' as radar_dlt_ind,
fp.fscl_prd_yr_vrnt_txt as fscl_prd_yr_vrnt_txt,
case when fp.FSCL_PRD_TYP_CD like '%Year%' then 'Year' 
when fp.FSCL_PRD_TYP_CD like '%Month%' then 'Month'
else fp.FSCL_PRD_TYP_CD end as fscl_prd_std_typ_cd,
'1051041' as src_sys_cd
from fp join ss on fp.src_sys_cd=ss.src_sys_Cd'''
spark.sql(query).repartition(1).createOrReplaceTempView('temp')
result=spark.sql("select cast(rpad(SRC_SYS_CD,18,'0') as bigint )+acct_prd_dmnsn_row_sequence() as acct_prd_ky,fscl_prd_yr_nr,\
fscl_prd_sqn_nr,fscl_prd_typ_cd,fscl_prd_nm,fscl_prd_dn,fscl_prd_strt_ts,fscl_prd_end_ts,fscl_yr_strt_dt,fscl_prd_yr_hf_nr,fscl_prd_yr_qtr_nr, \
fscl_prd_yr_mth_nr,p_src_sys_cd,radar_upd_ts,radar_upd_by_prs_id,radar_dlt_ind,fscl_prd_yr_vrnt_txt,fscl_prd_std_typ_cd,src_sys_cd from temp")

result.write.insertInto("radar_rz.acct_prd_dmnsn",overwrite=True)
