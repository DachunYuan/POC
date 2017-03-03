# -*- coding: utf-8 -*-

"""
Created on Mar 22, 2016
Purpose: Capture traffic from kafka queues and save to Cassandra
"""

## Spark Application - execute with spark-submit

## Imports
import json
import logging
import logging.config
import re
import sys
import uuid
from ConfigParser import SafeConfigParser
import os

import time

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

## Module Constants
APP_LOG_CONF_FILE = "/opt/mount1/app/conf"
#APP_LOG_CONF_FILE = "hdfs:///conf/log.properties"
APP_NAME = "Kafka_Spark"
VERSION_VAL = "0.11"

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
        """
	"""this check only works under unix not on hdfs thus commented
        if os.path.exists(in_logger_file_path):
        """
	
	logger_configfile_path = in_logger_file_path + "/log.properties"
	#print logger_configfile_path
	logging.config.fileConfig(logger_configfile_path)
        logger = logging.getLogger("ITR2")
        """
	else:
            # If logger property/configuration file doesn't exist,
            # and logger object will be constructed with default properties.
            logger = logging.getLogger(os.path.basename(__file__))
            logger.setLevel(logging.DEBUG)
            # Create a new logger file
            logger_file_path_object = open(in_logger_file_path, 'a+')
            logger_file_path_object.close()
            # create a file handler
            handler = logging.FileHandler(in_logger_file_path)
            handler.setLevel(logging.INFO)
            # create a logging format
            formatter = logging.Formatter('[%(asctime)s - %(name)s - %(levelname)s] %(message)s')
            handler.setFormatter(formatter)
            # add the handlers to the logger
            logger.addHandler(handler)
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
            logger.warning("The logger configuration file %s doesn't exist, "
                            "so logger object will be constructed with default properties.", in_logger_file_path)
	"""
        return logger

def valid_data(rdd):
    """Do transformation and filter out RDDs from DStream before writing data to Cassandra or KariosDB,
    and return transformed and filtered RDD
    Args:
        rdd (rdd):
    Return:
        rdd
    """
    # Create cluster object and start session
    global topics, zk_quorum, process_start_time
    #logger.debug("    %s" %(get_elapsed_time(process_start_time, time.time(), "Writing data to Cassandra and Karios")))
    number_of_records_in_rdd = rdd.count()
    if number_of_records_in_rdd == 0:
	#logger.debug("    NO data is coming from topics %s in Kafka broker %s", " | ".join(topics.keys()), zk_quorum)
        return
    else:
        return
	#logger.debug("    Start proccessing %d reocrds for each partition ...", number_of_records_in_rdd)
        #rdd.foreachPartition(process_data)

# ==============================================================================
# Main
# ==============================================================================
if __name__ == "__main__":
    process_start_time = time.time()
 
    print ("Reading Configuration from %s") % APP_LOG_CONF_FILE
    logger = construct_logger(APP_LOG_CONF_FILE)
    
    #Obsolete paramater for createStream
    #zk_quorum = "c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181"
    #group = "ldap"

    #paramater for createDirectStream
    kafka_broker_list = "c9t26359.itcs.hpecorp.net:9092,c9t26360.itcs.hpecorp.net:9092,c9t26361.itcs.hpecorp.net:9092"
    topics = "HPESCLTA1;HPESCLTA2;HPESCLTA3"
 
    # Split topics into a dict and remove empty strings e.g. {'topic1': 1, 'topic2': 1}
    topics = [x.strip() for x in topics.split(';')]
    topics = filter(None, topics)
    topics = dict(zip(topics, [1] * len(topics)))

    # Config for Spark
    batch_duration = 10
    conf = SparkConf().setAppName(APP_NAME)
    
    # create SparkContext and ssc spark instance
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, batch_duration)

    logger.info("==> Creating Spark DStream ...")
    try:
	#Obsolete function        
        #kafka_stream = KafkaUtils.createStream(ssc, zk_quorum, group, topics)
        logger.debug("topics are")
	logger.debug(topics.keys())
	logger.debug("kafka_broker_list is")
	logger.debug(kafka_broker_list)
	
	#topics is a list kafka parameter is a dict
	kafka_stream = KafkaUtils.createDirectStream(ssc, topics.keys(), {"metadata.broker.list": kafka_broker_list})
        
        logger.debug("Created kafka_stream successfully")
	lines = kafka_stream.map(lambda x: x[1])
        lines.pprint()

        #logger.debug("hello world")
        #lines.foreachRDD(lambda rdd: valid_data(rdd))
        
	# start spark instance
        ssc.start()
        ssc.awaitTermination()
    except KeyboardInterrupt:
	logger.debug("    %s\n    %s" % ("Skip", get_elapsed_time(process_start_time, time.time(), APP_NAME)))
    except Exception as e:
        process_end_time = time.time()
        logger.error("    %s\n    %s" %(e.message, get_elapsed_time(process_start_time, process_end_time, APP_NAME)))
