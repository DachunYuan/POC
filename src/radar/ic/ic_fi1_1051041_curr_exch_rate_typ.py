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
APP_NAME = "ic_fi1_1051041_curr_exch_rate_typ"
az_schema_name = "200836_az_fi1_1051041"
ic_schema_name = "radar"
table_name = "curr_exch_rate_typ"
source_system_code = "1051041"
HIVE_SQL_STRING = '''select distinct 
	'{source_system_code}' as p_src_sys_cd,
	TCURW.KURST as CURR_EXCH_RATE_TYP_CD,
	TCURW.CURVW as CURR_EXCH_RATE_TYP_nm,
	null as CURR_EXCH_RATE_TYP_dn,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as radar_upd_ts,
	'ic_fi1_1051041_curr_exch_rate_typ' as radar_upd_by_prs_id,
	'N' as radar_dlt_id,
	'{source_system_code}' as src_sys_cd
from {az_schema_name}.TCURW
where TCURW.SPRAS='E' '''


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
