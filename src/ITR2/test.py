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

from generateSql import * 
import datetime
from kafka2spark import kafka2spark
from redis_hash import RedisOp
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

sql_update = "update ITR23.RLT_ESCLT_ACTN_D set   where ESCLT_ACTN_D_KY="
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
    #column = "ESCLT_ACTN_ID,ESCLT_ID,ACTN_ID,ACTN_SUB_ID,ACTN_OWN_EMAIL_NM,MSTR_L_ESCLT_ACTN_STAT_KY,DUE_DT,ACTN_TX,SYS_MODF_DT,SYS_MODF_USER_NM,HP_CO_ID,SPRN_CO_ID"
    #values = "VALUES(%d,%s,%d,%d,%s，%d,NVL(TO_DATE(DECODE('%s','NULL',NULL),'YYYY-MM-DD HH24:MI:SS'),%s,,NVL(TO_DATE(DECODE('%s','NULL',NULL),'YYYY-MM-DD HH24:MI:SS'),%s,%s,%d)"
    #value = "VALUES(%d,%d,DECODE('%s','NULL',NULL),DECODE('%s','NULL',NULL),DECODE('%s','NULL',NULL),%d,DECODE('%s','NULL',NULL),%d,%d,DECODE('%s','NULL',NULL),DECODE('%s','NULL',NULL),NVL(TO_DATE(DECODE('%s','NULL',NULL),'YYYY-MM-DD HH24:MI:SS'),null),NVL(TO_DATE(DECODE('%s','NULL',NULL),'YYYY-MM-DD HH24:MI:SS'),null),NVL(TO_NUMBER(%s),NULL),DECODE('%s','NULL',NULL),DECODE('%s','NULL',NULL),NVL(TO_DATE(DECODE('%s','NULL',NULL),'YYYY-MM-DD HH24:MI:SS'),null),DECODE('%s','NULL',NULL),DECODE('%s','NULL',NULL),NVL(TO_NUMBER(%s),NULL))"

    sql_insert = generateInsertSQL("ITR23.RLT_ESCLT_ACTN_D","./RLT_ESCLT_ACTN_D.conf")
    dsn = cx_Oracle.makedsn(info["host"],info["port"],info["sid"])
    con = cx_Oracle.connect(info["username"],info["passward"],dsn)
    cursor = con.cursor()
    
    for oneArray in iterator:
        oneArray = list(eval(oneArray))
    for i in range(len(oneArray)):
        afterProcessArray = []
        oneTuple = oneArray[i]

        checkDataMd5 = ''
        actn_id = str(oneTuple[0])
        # operating a tuple of a list
        for j in range(len(oneTuple)):
            oneElement = oneTuple[j]
            if oneElement != None :
                checkDataMd5 = checkDataMd5 + str(oneElement)
                afterProcessArray.append(oneElement)    
            else:
                afterProcessArray.append('NULL')
                
        if oneElement != None :
            operation = ["get","HPSC_MSTR_L_CD_D"]
            rs_mst = RedisOp(operation)
            one_res_mst = list(eval(rs_mst[1]))
            
            for i in one_res_mst:
                if afterProcessArray[5] in i:
                    if i[6] == "ML_ESCLT_ACTN_STAT_KY":
                        checkDataMd5 = checkDataMd5 + str(afterProcessArray[0])
                        afterProcessArray[5] = afterProcessArray[0]
                        if i[0] == None:
                            afterProcessArray[0] =0
                            checkDataMd5 = checkDataMd5 + str(afterProcessArray[0])
                        elif afterProcessArray[5] == None:
                            afterProcessArray[0]=-2 
                            checkDataMd5 = checkDataMd5 + str(afterProcessArray[0])
                    
            operation = ["get","HP_CO_TO_SPRN_CO_MAPG"]
            rs_sprn = RedisOp(operation)
            rs_sprn_list = list(eval(rs_sprn[1]))
            state = False
            for item in rs_sprn_list:
                if afterProcessArray[-1] in item:
                    proData = item[1]
                    checkDataMd5 = checkDataMd5 + str(proData)
                    afterProcessArray.append(item[1])
                    state = True
            if not state:
                afterProcessArray.append(-2)
                checkDataMd5 = checkDataMd5 + str(afterProcessArray[-1])


            md5_str = hashlib.md5(checkDataMd5).hexdigest()
            operation = []
            operation.append("get")
            operation.append(actn_id)
            rs = RedisOp(operation)
            if rs[0]:
                r_s = list(eval(rs[1]))
                if r_s[1] != md5_str:
                    r_s[1] = md5_str
                    RedisOp(["set",actn_id,r_s])
                    #update the data int the database
                    res=cursor.execute(info["sql"])
            else:
                res = RedisOp(["get","ESCLT_ACTN_D_count"])
                r_s = []
                if res[0]:
                    count = int(res[1]) + 1
                    RedisOp(["set","ESCLT_ACTN_D_count",count])
                    r_s.append(count)
                    #inster data to the database
                    afterProcessArray.insert(0,count)
                    r = tuple(afterProcessArray)
                    RedisOp(["set","Rs",r])
                    sql_v = sql_inster%r
                    RedisOp(["set","sql_v",sql_v])
                    res=cursor.execute((sql_inster%r))
                    con.commit()
                else:
                    RedisOp(["set","ESCLT_ACTN_D_count",1])
                    r_s.append(1)
                    #inster data to the database
                    afterProcessArray.insert(0,1)
                    r = tuple(afterProcessArray)
                    sql = sql_inster%r
                    RedisOp(["set","sql_v",sql])
                    res=cursor.execute(sql)
                    con.commit()
                    r_s.append(md5_str)
                    RedisOp(["set",actn_id,r_s])


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
    topics = "HPESCLTACTNM1"
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


