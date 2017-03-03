# -*- coding: UTF-8 -*-
# @author: Qian,Hong (Anho)

import argparse
from pyspark.sql import SparkSession
import logging
import logging.config

################################################################
# Constraints
################################################################
APP_LOG_CONF_FILE = "/opt/app/NewTechPOC/conf/log.properties"
APP_NAME = "ic_fi1_1051041_acct_trsn"
az_schema_name = "200836_az_fi1_1051041"
ic_schema_name = "radar"
table_name = "acct_trsn"
source_system_code = "1051041"
HIVE_SQL_STRING = '''select  '{source_system_code}' as p_src_sys_cd,
concat(BKPF.BUKRS,BKPF.BELNR,BKPF.GJAHR) as ACCT_TRSN_ID,
T001.LAND1 CTRY_CD,
BKPF.BUKRS as ENTRS_LGL_ENT_NR,
FSCL_PRD_ID as FSCL_PRD_ID,
BKPF.CPUDT ACCT_TRSN_ETRY_TS,
concat(BKPF.BUKRS,BKPF.BELNR,BKPF.GJAHR) as ACCT_TRSN_BTCH_ID,
BKPF.BLART	ACCT_TRSN_TYP_CD,
null as ACCT_TRSN_SRC_CD,
BKPF.BKTXT ACCT_TRSN_NM,
null as ACCT_TRSN_DN,
BKPF.BLDAT ACCT_TRSN_ORGNN_TS,
BKPF.BUDAT ACCT_TRSN_PSTG_TS,
BKPF.WWERT ACCT_TRSN_TRNSLTN_TS,
BKPF.XRUEB ACCT_TRSN_BCKPST_IND,
case when KUTY2 is null or trim(KUTY2) ='' then 'M' else KUTY2 end CURR_EXCH_RATE_TYP_CD,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS RADAR_UPD_TS,
'ic_fi1_1051041_acct_trsn' AS RADAR_UPD_BY_PRS_ID,
'N' AS RADAR_DLT_IND,
'COMPANY' as ENTRS_LGL_ENT_BSN_ROL_CD,
CASE WHEN TRIM (BKPF.STGRD) = '' THEN NULL ELSE BKPF.STGRD END as  RVRSL_RSN_CD,
'{source_system_code}' as src_sys_cd
from {az_schema_name}.BKPF
inner join {az_schema_name}.T001	on T001.BUKRS = BKPF.BUKRS
left outer join {ic_schema_name}.FSCL_PRD on FSCL_PRD.FSCL_PRD_YR_NR = BKPF.GJAHR 
AND FSCL_PRD.FSCL_PRD_SQN_NR = BKPF.MONAT 
AND FSCL_PRD.FSCL_PRD_YR_VRNT_TXT = T001.PERIV 
AND SRC_SYS_CD = '{source_system_code}'
where nvl(trim(BKPF.BSTAT), '') = '' '''


def main(): 

    # Instantiate logger
    logging.config.fileConfig(APP_LOG_CONF_FILE)
    logger = logging.getLogger(APP_NAME)
    logger.info("==> Start ...")
    logger.debug("    The script arguments: schema name: {} table_name: {} source_system_list: {}"
                 .format(ic_schema_name, table_name, source_system_code))

    logger.info("==> Start instantiating Spark SQL instance ... ")
    spark = SparkSession \
        .builder \
        .appName(APP_NAME) \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

    # Define data frame based on one Hive SQL
    logger.debug(HIVE_SQL_STRING.format(source_system_code=source_system_code,az_schema_name=az_schema_name,ic_schema_name=ic_schema_name))
    sql_df = spark.sql(HIVE_SQL_STRING.format(source_system_code=source_system_code,az_schema_name=az_schema_name,ic_schema_name=ic_schema_name))    

    # Overwrite partition dynamically  based on resulting data frame	
    sql_df.write.insertInto("{}.{}".format(ic_schema_name, table_name), overwrite=True)

    # The end
    logger.info("==> Completed the whole job.")


if __name__ == '__main__':
    main()
