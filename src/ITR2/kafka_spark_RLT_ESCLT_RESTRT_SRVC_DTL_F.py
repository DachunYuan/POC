'''
spark-submit --master yarn --deploy-mode client --executor-memory 512M --driver-memory 512M --total-executor-cores 1 \
--py-files /home/zengchu/kafka2spark.py,/home/zengchu/ITR2_config.ini,/home/zengchu/redis_hash.py \
--jars $SPARK_HOME/lib/spark-streaming-kafka-assembly_2.11-1.6.3.jar \
/home/zengchu/kafka_spark_RLT_ESCLT_RESTRT_SRVC_DTL_F.py

yarn application -kill application_1488184969290_0073
'''

import logging
import logging.config
import time
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from kafka2spark import kafka2spark
from redis_hash import RedisOp
import cx_Oracle
import hashlib

# Module Constants
APP_LOG_CONF_FILE = "/opt/mount1/app/conf"
# APP_LOG_CONF_FILE = "hdfs:///conf/log.properties"
APP_NAME = "HPESCLTA4"
VERSION_VAL = "0.11"

info = {"sql": "select sessiontimezone from dual", "username": "INFR", "passward": "4.!INFRitr2DEVpw_20160504",
        "host": "g1u2115.austin.hp.com", "port": "1525", "sid": "ITRD"
        }

# ==============================================================================
# Function
# ==============================================================================

def construct_logger(in_logger_file_path):
    logger_configfile_path = in_logger_file_path + "/log.properties"
    # print logger_configfile_path
    logging.config.fileConfig(logger_configfile_path)
    logger = logging.getLogger("ITR2")
    return logger


def process_data(iterator):
    column = "ESCLT_ID,REC_NR,ESCLT_RESTRT_SRVC_TYPE_D_KY,ESCLT_MGMT_D_KY,MSTR_L_RESTRT_SRVC_TYPE_KY"
    value = "VALUES('%s',%d,%d,%d,%d)"
    sql_inster = "INSERT INTO ITR23.RLT_ESCLT_RESTRT_SRVC_DTL_F" + "(" + column + ")" + value

    dsn = cx_Oracle.makedsn(info["host"], info["port"], info["sid"])
    con = cx_Oracle.connect(info["username"], info["passward"], dsn)
    cursor = con.cursor()
    for oneArray in iterator:
        oneArray = list(eval(oneArray))
        for i in range(len(oneArray)):
            afterProcessArray = []
            oneTuple = oneArray[i]
            del oneTuple[3]
            checkDataMd5 = ''

            dvc_id = "RLT_ESCLT_RESTRT_SRVC_DTL_F"+str(oneTuple[0])+str(oneTuple[1])

            lkp_condition = "ESCLT_RESTRT_SRVC_TYPE_D"+str(oneTuple[0])+str(oneTuple[1])
            res = RedisOp(["get", lkp_condition])
            if res[0]:
                r_s = list(eval(res[1]))
                oneTuple.append(int(r_s[0]))
                RedisOp(["set", "lkp_ESCLT_RESTRT_SRVC_DTL_F", r_s[0]])
            else:
                oneTuple.append(0)

            #cursor.execute("SELECT DISTINCT ESCLT_RESTRT_SRVC_TYPE_D_KY FROM ITR23.RLT_ESCLT_RESTRT_SRVC_TYPE_D WHERE ESCLT_ID='%s' AND REC_NR=%d" % (oneTuple[0], oneTuple[1]))
            #row =  cursor.fetchone()
            #if row !=None:
            #    oneTuple.append(row[0])
            #else:
            #    oneTuple.append(0)

            cursor.execute("SELECT DISTINCT ESCLT_MGMT_D_KY FROM ITR23.ESCLT_D WHERE ESCLT_ID = '%s'" % oneTuple[0])
            row = cursor.fetchone()
            if row !=None:
                oneTuple.append(row[0])
            else:
                oneTuple.append(0)

            if oneTuple[2] !=None:
                cursor.execute("SELECT DISTINCT HPSC_MSTR_L_KY  from ITR23.HPSC_MSTR_L_CD_D where MSTR_L_COL_ID='ML_RSTAT_SEVC_TYPE_KY' and VALU_ID=%d" % oneTuple[2])
                row = cursor.fetchone()
                if row !=None:
                    oneTuple.append(row[0])
                else:
                    oneTuple.append(0)
            else:
                oneTuple.append(-2)

            #oneTuple.append(oneTuple[2])

            #delElement = [2] #Remove ML_RSTAT_SEVC_TYPE_KY,exits in L1(3th),do not needed in D
            #for i in delElement:
            del oneTuple[2]

            # operating a tuple of a list
            for j in range(len(oneTuple)):
                oneElement = oneTuple[j]
                if oneElement != None:
                    if type(oneElement) is str:
                        proData = oneElement.strip()
                        afterProcessArray.append(proData)
                        checkDataMd5 = checkDataMd5 + proData
                    else:
                        afterProcessArray.append(oneElement)
                        checkDataMd5 = checkDataMd5 + str(oneElement)
                else:
                    afterProcessArray.append('NULL')
            # check if the md5 are the same or existence

            md5_str = hashlib.md5(checkDataMd5).hexdigest()
            #afterProcessArray.append(md5_str)

            operation = []
            operation.append("get")
            operation.append(dvc_id)
            rs = RedisOp(operation)
            if rs[0]:
                r_s = list(eval(rs[1]))
                if r_s[1] != md5_str:
                    r_s[1] = md5_str
                    #RedisOp(["set", dvc_id, r_s])
                    RedisOp(["set", dvc_id, r_s])

                    res = RedisOp(["get", "RLT_ESCLT_RESTRT_SRVC_DTL_F_count"])
                    #count = int(res[1])
                    #afterProcessArray.insert(0, count)
                    # update the data int the database
                    #res = cursor.execute(info["sql"])
                    #r = tuple(afterProcessArray)
                    #sql = "UPDATE ITR23.RLT_ESCLT_RESTRT_SRVC_TYPE_D SET RESTRT_SRVC_DTL_DN='%s' WHERE ESCLT_RESTRT_SRVC_TYPE_D_KY=%d"%(afterProcessArray[3],count)
                    sql = "UPDATE ITR23.RLT_ESCLT_RESTRT_SRVC_DTL_F SET ESCLT_RESTRT_SRVC_TYPE_D_KY= %d,ESCLT_MGMT_D_KY=%d,MSTR_L_RESTRT_SRVC_TYPE_KY=%d WHERE ESCLT_ID= '%s' AND REC_NR = %d "%(afterProcessArray[2],afterProcessArray[3],afterProcessArray[4],afterProcessArray[0],afterProcessArray[1])
                    cursor.execute(sql)
                    con.commit()
            else:
                res = RedisOp(["get", "RLT_ESCLT_RESTRT_SRVC_DTL_F_count"])
                r_s = []
                #count = 0
                if res[0]:
                    count = int(res[1]) + 1
                    RedisOp(["set", "RLT_ESCLT_RESTRT_SRVC_DTL_F_count", count])
                    r_s.append(count)
                    # inster data to the database
                    #afterProcessArray.insert(0, count)
                    r = tuple(afterProcessArray)
                    RedisOp(["set", "Rs_ESCLT_RESTRT_SRVC_DTL_F", r])
                    sql_v = sql_inster % r
                    RedisOp(["set", "ESCLT_RESTRT_SRVC_DTL_F_sql_v", sql_v])
                    cursor.execute((sql_inster % r))
                    con.commit()
                else:
                    RedisOp(["set", "RLT_ESCLT_RESTRT_SRVC_DTL_F_count", 1])
                    r_s.append(1)
                    # inster data to the database
                    #afterProcessArray.insert(0, 1)
                    r = tuple(afterProcessArray)
                    sql = sql_inster % r
                    RedisOp(["set", "ESCLT_RESTRT_SRVC_DTL_F_sql_v", sql])
                    cursor.execute(sql)
                    con.commit()
                r_s.append(md5_str)
                RedisOp(["set", dvc_id, r_s])


def valid_data(rdd):
    number_of_records_in_rdd = rdd.count()
    if number_of_records_in_rdd == 0:
        logger.debug("    NO data is coming from topics %s in Kafka broker"%topics)
        return
    else:
        logger.debug("Start proccessing %d reocrds for each partition ..."%number_of_records_in_rdd)
        #print "****Below is data in RDD*****"
        #for each_row in rdd.collect():
        #    print each_row

        rdd.foreachPartition(process_data)
        # rdd.foreach(process_data)

# ==============================================================================
# Main
# ==============================================================================
if __name__ == "__main__":
    process_start_time = time.time()
    print ("Reading Configuration from %s") % APP_LOG_CONF_FILE
    logger = construct_logger(APP_LOG_CONF_FILE)
    # Config for Spark
    batch_duration = 10
    conf = SparkConf().setAppName(APP_NAME)
    # create SparkContext and ssc spark instance
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, batch_duration)
    logger.info("==> Creating Spark DStream ...")
    # topics = "HPESCLTDVCM1"
    topics = "HPESCLTA4"
    kafka_stream = kafka2spark(ssc, topics)
    # logger.debug(kafka_stream)
    #print(type(kafka_stream))
    logger.info("Created kafka_stream successfully")
    lines = kafka_stream.map(lambda x: x[1])
    lines.pprint(num=50)
    lines.foreachRDD(lambda rdd: valid_data(rdd))
    # start spark instance
    ssc.start()
    ssc.awaitTermination()