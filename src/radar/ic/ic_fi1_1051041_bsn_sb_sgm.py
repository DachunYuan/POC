# -*- coding: UTF-8 -*-
# @author: Max

import argparse
from pyspark.sql import SparkSession
import logging
import logging.config

################################################################
# Constraints
################################################################
APP_LOG_CONF_FILE = "/opt/app/NewTechPOC/conf/log.properties"
APP_NAME = __file__
az_schema_name = "200836_az_fi1_1051041"
ic_schema_name = "radar"
table_name = "bsn_sb_sgm"
source_system_code = "1051041"
HIVE_SQL_STRING = """SELECT 
		 '{source_system_code}' AS p_src_sys_cd,
		BSN_SB_SGM_CD,
		BSN_SB_SGM_NM,
		BSN_SB_SGM_DN,
		 from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS RADAR_UPD_TS,
		 '{APP_NAME}' AS RADAR_UPD_BY_PRS_ID,
		 'N' AS RADAR_DLT_IND
		FROM 
		(   SELECT DISTINCT 
			GSBER AS BSN_SB_SGM_CD,
			NULL AS BSN_SB_SGM_NM,
			GTEXT AS BSN_SB_SGM_DN
			FROM {az_schema_name}.TGSBT	
			WHERE SPRAS= 'E'	
		union 
			select distinct
			GLT0.RBUSA as BSN_SB_SGM_CD,
			NULL as BSN_SB_SGM_NM,
			'Manually inserted' as BSN_SB_SGM_DN
			FROM {az_schema_name}.GLT0
			left join {az_schema_name}.T001 on GLT0.BUKRS = T001.BUKRS 
			where NOT EXISTS( 
			SELECT DISTINCT TGSBT.GSBER FROM  {az_schema_name}.TGSBT where  TGSBT.SPRAS='E' AND  TGSBT.GSBER = GLT0.RBUSA)
			AND  GLT0.RLDNR = '00' and GLT0.RRCTY = '0' and GLT0.RVERS = '001' and GLT0.RBUSA is not null and trim (GLT0.RBUSA) <> ''
			)SRC
		union all
		SELECT 
		 '{source_system_code}' AS p_src_sys_cd,
		 'HR00'  as BSN_SB_SGM_CD,
		 'Human Resources' as BSN_SB_SGM_NM,
		 'DELETED MASTER DATA RECORD - MANUALLY INSERTED BY RADAR' as BSN_SB_SGM_DN,
		  from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS RADAR_UPD_TS,
		   '{APP_NAME}' AS RADAR_UPD_BY_PRS_ID,
		    'N' AS RADAR_DLT_IND
		"""


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