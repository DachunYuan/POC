import argparse
from pyspark.sql import SparkSession
from pyspark import StorageLevel
import logging
import logging.config

################################################################
# Constraints
################################################################
APP_LOG_CONF_FILE = "/opt/app/NewTechPOC/conf/log.properties"
HIVE_SQL_STRING = "select distinct {source_system_list} as p_src_sys_cd, tcurt.waers as CURR_CD, " \
                  "tcurt.ltext as curr_nm,null as curr_dn, from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') " \
                  "as RADAR_UPD_TS, 'ic_fi1_1051041_curr' as RADAR_UPD_BY_PRS_ID, 'N' as radar_dlt_ind, " \
                  "{source_system_list} as src_sys_cd from 200836_az_fi1_1051041.tcurt where tcurt.spras='E'"


################################################################
# Function
################################################################
def function_name(parameter_1, parameter_2, *args, **kwargs):
    """Describe the purpose of functionname, and what should be returned
        Args:
            parameter_1 (str): description of parameter1
            parameter_2 (str): description of parameter2
    Returns:
        str(data type of the return)
    """
    return parameter_1 + parameter_2


def handle_commandline():
    """ handle Command arguments, and return a tuple containing arguments.
    Returns:
        tuple
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--source_system_list_str", type=str,
                        help="The list of source system codes, and delimited by slash", required=True)
    parser.add_argument("-t", "--table_name", type=str, help="Table Name", required=True)
    parser.add_argument("-s", "--schema_name", type=str, help="Name of table schema name", required=True)
    args = parser.parse_args()
    return args.schema_name, args.table_name, args.source_system_list_str


def main():
    # Parse script arguments
    schema_name, table_name, source_system_list_str = handle_commandline()

    # Instantiate logger
    logging.config.fileConfig(APP_LOG_CONF_FILE)
    logger = logging.getLogger("RADAR Spark POC Example")
    logger.info("==> Start ...")
    logger.debug("    The script arguments: schema name: {} table_name: {} source_system_list: {}"
                 .format(schema_name, table_name, source_system_list_str))

    logger.info("==> Start instantiating Spark SQL instance ... ")
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

    # Define data frame based on one Hive SQL
    logger.debug("Current Spark SQL query is : %s" % HIVE_SQL_STRING.format(source_system_list=source_system_list_str))
    sql_df = spark.sql(HIVE_SQL_STRING.format(source_system_list=source_system_list_str))

    # Create temporary function. Before that, we need to include corresponding jar file
    logger.info("==> Start testing Hive UDF function")
    func1 = spark.sql("create temporary function addr_sequence_105104 AS "
                      "'org.apache.hadoop.hive.contrib.udf.UDFRowSequence'")
    # We must use function name followed by brace "()".
    func1_test = spark.sql("SELECT concat(addr_sequence_105104(),'res') as func_value").count()

    # Create UDF based on Python function
    spark.sql.udf.register("func", function_name)
    func2 = spark.sql("select func('a', 'b')")

    # Persist data frame in memory and then spilling disk "MEMORY_AND_DISK" for future multiple access.
    logger.info("==> Start caching data frame sql_df ...")
    sql_df.persist(StorageLevel(True, True, False, False, 1))

    # Register temporary view
    logger.info("==> Start creating temporary view ...")
    sql_df.createOrReplaceTempView("curr")

    # Do anything processing data on data frame

    # Overwrite partition dynamically  based on resulting data frame
    logger.info("==> Start loading results to target table {}.{}".format(schema_name, table_name))
    sql_df.write.insertInto("{}.{}".format(schema_name, table_name), overwrite=True)

    # The end
    logger.info("==> Completed the whole job.")


if __name__ == '__main__':
    main()
