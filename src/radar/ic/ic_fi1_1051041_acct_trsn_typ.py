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
APP_NAME = "ic_fi1_1051041_acct_trsn_typ"
az_schema_name = "200836_az_fi1_1051041"
ic_schema_name = "radar"
table_name = "acct_trsn_typ"
source_system_code = "1051041"
HIVE_SQL_STRING = '''SELECT  distinct
				'{source_system_code}' as p_src_sys_cd,
			  BLART AS ACCT_TRSN_TYP_CD,
			  NULL AS ACCT_TRSN_TYP_NM,
			  LTEXT AS ACCT_TRSN_TYP_DN,
			  from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS RADAR_UPD_TS,
			 'ic_fi1_1051041_acct_trsn_typ' AS RADAR_UPD_BY_PRS_ID,
			 'N' AS RADAR_DLT_IND,
			 '{source_system_code}' as src_sys_cd 
			 FROM {az_schema_name}.T003T 
			 WHERE SPRAS='E' '''


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
    logger.debug(HIVE_SQL_STRING.format(source_system_code=source_system_code,az_schema_name=az_schema_name))
    sql_df = spark.sql(HIVE_SQL_STRING.format(source_system_code=source_system_code,az_schema_name=az_schema_name))

    # Overwrite partition dynamically  based on resulting data frame	
    sql_df.write.insertInto("{}.{}".format(ic_schema_name, table_name), overwrite=True)

    # The end
    logger.info("==> Completed the whole job.")


if __name__ == '__main__':
    main()
