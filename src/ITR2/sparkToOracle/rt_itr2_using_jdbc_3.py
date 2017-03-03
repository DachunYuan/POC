'''
Created on Dec 27, 2016
@author: Sakina
27-Dec   Sakina  Load data into Oracle from rdd - original version
'''
from __future__ import print_function
import json
import logging.config
import itr2_constants
import sys
from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions,SparkSession
import os
"""
    Loads a DataFrame from a relational database table over JDBC,
    manipulates the data, and saves the results back to a table.
    For ITR2 jdbc connection testing - to real-time project
"""

APP_NAME = "rt_itr2_using_jdbc_3"
logging.config.fileConfig(itr2_constants.PROPERTIES_LOG_FILE)
logger = logging.getLogger(APP_NAME)
db_file_path=(itr2_constants.DB_PROPERTIES_FILE)
delimiter = ','
# intinial sqlcontext
sc = SparkContext()
sqlContext = SQLContext(sc)  # or HiveContext

def itr2_rt_read(tab_name):
    logger.debug("%s : *************A DataFrame loading from the entire contents of a ITR2 table over JDBC ...",APP_NAME)
    where = tab_name
    DF_HPESCLTA3 = spark.read.jdbc(jdbcUrl, where, properties=db_propertyFile)
    print(DF_HPESCLTA3)
    DF_HPESCLTA3.printSchema()
    #print(DF_HPESCLTA3.first())
    DF_HPESCLTA3.show()
    print("------------------success show the datas----------------------")

#query = "(SELECT * from INFR.RT_HPESCLTA3)"
#jdbcurl = "jdbc:jdbc:oracle:thin://g1u2115.austin.hp.com:1525/ITRD?user=INFR&password=4.!INFRitr2DEVpw_20160504"
#df = sqlContext.load(url=jdbcurl, source="jdbc", dbtable="INFR.RT_HPESCLTA3")
#DF_HPESCLTA3_2 = sqlContext.load(url="jdbc:oracle:thin://g1u2115.austin.hp.com:1525/ITRD?user=INFR&password=4.!INFRitr2DEVpw_20160504",source="jdbc",dbtable="INFR.RT_HPESCLTA3")
#for esclta3 in df.collect():
#    print(esclta3)
def itr2_rt_write(tab_name):
    logger.debug("%s : *************Reading data from hdfs for insert ...", APP_NAME)
    rawData = sc.textFile("/user/jizho/raw_hpesclta3_20161227.txt").map(lambda Row: Row.split(delimiter))
    
    # map data to columns
    HPESCLTA3 = rawData.map(lambda p: Row(HP_ESCLT_ID=p[0], RECORD_NUMBER=p[1], ML_CPCTY_ISS_TYPE_KY=p[2], HP_CPCTY_ISS_TDL_TX=p[3])).toDF()
    HPESCLTA3.registerTempTable("RDD_HPESCLTA3")
    UPD_HPESCLTA3 = sqlContext.sql("SELECT HP_ESCLT_ID,RECORD_NUMBER,90,HP_CPCTY_ISS_TDL_TX FROM RDD_HPESCLTA3")
    #print data before insert into oracle
    for esclta3 in UPD_HPESCLTA3.collect():
        print(esclta3)
    logger.debug("%s : *************Insert data content into a ITR2 table over JDBC ...", APP_NAME)
    # where = "INFR.RT_HPESCLTA3"
    where = tab_name
    UPD_HPESCLTA3.write.jdbc(jdbcUrl, where, properties=dbProperties, mode="error")
#main program
if __name__ == "__main__":
    spark = SparkSession.builder.appName("rt_itr2_using_jdbc").getOrCreate()
    # Load properties from file
    with open(db_file_path) as db_propertyFile:
        db_propertyFile = json.load(db_propertyFile)
    jdbcUrl = db_propertyFile["jdbcUrl"]
    dbProperties = {
        "user" : db_propertyFile["user"],
        "password" : db_propertyFile["password"]
    }
    # get update type for jdbc connection testing
    if ( len(sys.argv)==3 ):
        type = sys.argv[1]
        if (type == "read"):
            itr2_rt_read(sys.argv[2])
        elif(type == "write"):
            itr2_rt_write(sys.argv[2])
        else:
            logger.debug("%s : *************The type is not supported for this script right now", APP_NAME)
    else:
        print(' Usage: rt_itr2_using_jdbc_2 <type> <table_name>')
        print(' <type> : read | write ')
    sc.stop()
    spark.stop()
    sys.exit(-1)
