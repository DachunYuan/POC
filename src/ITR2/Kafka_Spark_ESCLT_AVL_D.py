# -*- coding: utf-8 -*-

"""
Created on Mar 22, 2016
Purpose: Capture traffic from kafka queues and save to Cassandra
"""

# Spark Application - execute with spark-submit

# Imports
#import sys
#sys.path.append(".")

import logging
import logging.config
import time
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row
from pyspark.sql import SQLContext, functions,SparkSession,HiveContext

import datetime
from kafka2spark import kafka2spark
from redis_hash import RedisOp
from generateSql import *
import pdb
import cx_Oracle
import hashlib
# Module Constants
APP_LOG_CONF_FILE = "/opt/mount1/app/conf"
# APP_LOG_CONF_FILE = "hdfs:///conf/log.properties"
APP_NAME = "Kafka_Spark"
VERSION_VAL = "0.11"


info = {"sql":"select sessiontimezone from dual","username":"INFR","passward":"4.!INFRitr2DEVpw_20160504",
        "host":"g1u2115.austin.hp.com","port":"1525","sid":"ITRD"        
       }

# ==============================================================================
# Function
# ==============================================================================
def construct_logger(in_logger_file_path):
    """Instantiate and construct logger object based on log properties file by default. Otherwise logger object will be
        constructed with default properties.
        Args:
            in_logger_dir_path (str): path followed by file name where logger properties/configuration file resides
        Returns:
            logger object
        this check only works under unix not on hdfs thus commented
        if os.path.exists(in_logger_file_path):
    """
    logger_configfile_path = in_logger_file_path + "/log.properties"
    # print logger_configfile_path
    logging.config.fileConfig(logger_configfile_path)
    logger = logging.getLogger("ITR2")
    return logger

#arg:
#   contion one or more than one list
#return:
#   null

def process_data(iterator):
    sql_insert = generateInsertSQL("ITR23.RLT_ESCLT_AVL_D","./RLT_ESCLT_AVL_D.conf")
    dsn = cx_Oracle.makedsn(info["host"],info["port"],info["sid"])
    con = cx_Oracle.connect(info["username"],info["passward"],dsn)
    cursor = con.cursor()
    
    for oneArray in iterator:
        oneArray = list(eval(oneArray))
        for i in range(len(oneArray)):
            afterProcessArray = []
            oneTuple = oneArray[i]

            checkDataMd5 = ''
            avl_id = str(oneTuple[0])
            # operating a tuple of a list
            for j in range(len(oneTuple)):
                oneElement = oneTuple[j]
                if oneElement != None :
                    
                    if type(oneElement) is str:
                        proData = oneElement.strip()
                        afterProcessArray.append(proData)
                        checkDataMd5 = checkDataMd5 + proData
                    else:
                        afterProcessArray.append(oneElement)
                        checkDataMd5 = checkDataMd5 + str(oneElement)
                    
                else:
                    afterProcessArray.append('NULL')
            #check if the md5 are the same or existence

            operation = ["get","HPSC_MSTR_L_CD_D"]
            res_mstr = RedisOp(operation)
            list_res_mstr = list(eval(res_mstr[1]))
            for i in list_res_mstr:
                if afterProcessArray[4] in i:
                    if i[6] == "ML_ESCLT_TYPE_KY":
                        afterProcessArray[4] = i[8]
                        checkDataMd5 = checkDataMd5 + str(afterProcessArray[4])
                        if i[8] == None:
                            afterProcessArray[4] = 0
                            checkDataMd5 = checkDataMd5 + str(afterProcessArray[4])
                        elif afterProcessArray[4] == None:
                            afterProcessArray[4] = -2
                            checkDataMd5 = checkDataMd5 + str(afterProcessArray[4])

            operation = ["get","HPSC_MSTR_L_CD_D"]
            res_mstr = RedisOp(operation)
            list_res_mstr = list(eval(res_mstr[1]))
            for i in list_res_mstr:
                if afterProcessArray[4] in i:
                    if i[6] == "ML_TMLNSS_TYPE_KY":
                        afterProcessArray[5] = i[8]
                        checkDataMd5 = checkDataMd5 + str(afterProcessArray[5])
                        if i[8] == None:
                            afterProcessArray[5] = 0
                            checkDataMd5 = checkDataMd5 + str(afterProcessArray[5])
                        elif afterProcessArray[5] == None:
                            afterProcessArray[5] = -2
                            checkDataMd5 = checkDataMd5 + str(afterProcessArray[5])

            operation = ["get","CI_D"]
            res_ci = RedisOp(operation)
            list_res_ci = list(eval(res_ci[1]))
            for i in list_res_ci:
                if afterProcessArray[10] in i :
                    afterProcessArray[10] = i[1]
                    checkDataMd5 = checkDataMd5 + str(afterProcessArray[10])
                    if i[1] == None:
                        afterProcessArray[10] = 0
                        checkDataMd5 = checkDataMd5 + str(afterProcessArray[10])
                    elif i[1] == None:
                        afterProcessArray[10] = -2
                        checkDataMd5 = checkDataMd5 + str(afterProcessArray[10])

            operation = ["get","HP_CO_TO_SPRN_CO_MAPG"]
            res_sprn = RedisOp(operation)
            list_res_sprn = list(eval(res_sprn[1]))
            for i in list_res_sprn:
                if afterProcessArray[11] in i:
                    afterProcessArray[11] == i[1]
                    checkDataMd5 = checkDataMd5 + str(afterProcessArray[10])
            
            md5_str = hashlib.md5(checkDataMd5).hexdigest()
            operation = []
            operation.append("get")
            operation.append(avl_id)
            rs = RedisOp(operation)
            avl_id = 'RLT_ESCLT_AVL_D' + avl_id
            if rs[0]:
                r_s = list(eval(rs[1]))
                if r_s[1] != md5_str:
                    r_s[1] = md5_str
                    #update the data int the database
                    update_sql = generateUpdateSql("ITR23.RLT_ESCLT_AVL_D","RLT_ESCLT_AVL_D.conf",["ESCLT_AVL_D_KY",r_s[0]])
                    res=cursor.execute(info["sql"])
                    con.commit()
                    RedisOp(["set",avl_id,r_s])
            else:
                res = RedisOp(["get","ESCLT_AVL_D_count"])
                r_s = []
                if res[0]:
                    count = int(res[1]) + 1
                    r_s.append(count)
                    #inster data to the database
                    afterProcessArray.insert(0,count)
                    r = tuple(afterProcessArray)
                    RedisOp(["set","Rs_ESCLT_AVL_D",r])
                    sql_v = sql_insert%r
                    RedisOp(["set","sql_v_ESCLT_AVL_D_KY",sql_v])
                    res=cursor.execute((sql_insert%r))
                    con.commit()
                    RedisOp(["set","ESCLT_AVL_D_count",count])
                else:
                    r_s.append(1)
                    #inster data to the database
                    afterProcessArray.insert(0,1)
                    r = tuple(afterProcessArray)
                    RedisOp(["set","Rs_ESCLT_AVL_D",r])
                    sql_v = sql_insert%r
                    RedisOp(["set","sql_v_ESCLT_AVL_D_KY",sql_v])
                    res=cursor.execute(sql)
                    con.commit()
                    RedisOp(["set","ESCLT_AVL_D_count",1])
                r_s.append(md5_str)
                RedisOp(["set",avl_id,r_s])


def valid_data(rdd):
    number_of_records_in_rdd = rdd.count()
    if number_of_records_in_rdd == 0:
        logger.debug("    NO data is coming from topics %s in Kafka broker %s")
        return
    else:
        logger.debug("    Start proccessing %d reocrds for each partition ...", number_of_records_in_rdd)
        rdd.foreachPartition(process_data)
        #rdd.foreach(process_data)

# ==============================================================================
# Main
# ==============================================================================
if __name__ == "__main__":
    process_start_time = time.time()
    print ("Reading Configuration from %s") % APP_LOG_CONF_FILE
    logger = construct_logger(APP_LOG_CONF_FILE)
    #logger.info(str(sys.path()))
    # Obsolete paramater for createStream
    # zk_quorum = "c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181"
    # group = "ldap"

    # reading paramaters for createDirectStream from ITR2_config.ini
    #kafka_broker_list = "c9t26359.itcs.hpecorp.net:9092,c9t26360.itcs.hpecorp.net:9092,c9t26361.itcs.hpecorp.net:9092"
    #topics = "HPESCLTA1,HPESCLTA2,HPESCLTA3"
 
    # Split topics into a dict and remove empty strings e.g. {'topic1': 1, 'topic2': 1}

    # Config for Spark
    batch_duration = 10
    conf = SparkConf().setAppName(APP_NAME)
    
    # create SparkContext and ssc spark instance
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, batch_duration)

    logger.info("==> Creating Spark DStream ...")
    #topics = "HPESCLTDVCM1"
    topics = "HPESCLTAVLM1"
    kafka_stream = kafka2spark(ssc,topics)
    #logger.debug(kafka_stream)
    print(type(kafka_stream))
    logger.info("Created kafka_stream successfully")
    lines = kafka_stream.map(lambda x: x[1])
    lines.pprint(num = 50)

    lines.foreachRDD(lambda rdd : valid_data(rdd))

    # start spark instance
    ssc.start()
    ssc.awaitTermination()


