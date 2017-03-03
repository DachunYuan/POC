# -*- coding: utf-8 -*-
"""
Created on Wed Feb 22 14:19:37 2017

@author: yinlin
"""

# -*- coding: utf-8 -*-

## Spark Application - execute with spark-submit

## Imports
from configparser import SafeConfigParser
import cx_Oracle

# ==============================================================================
# Main
# ==============================================================================
def connect_oracle():
    """ This function is used for kafka2spark connectivity
    Input: 
        ssc : Receive a sparkcontext or sparksession for process
        topics : separated by comma a string containing all kafka topics
        kafka_broker_list : separated by comma a string of the form hostname:port containing all kafka brokers
    Return:
        kafka_stream an object of type rdd
    """
    #Obsolete paramater for createStream
    #zk_quorum = "c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181"
    #group = "ldap"
    # get config_parser for config file
    config_parser = SafeConfigParser()
    config_parser.read('./ITR2_config.ini')
    APP_NAME = "dest_database"
 
    host = config_parser.get(APP_NAME, "host")
    port = config_parser.get(APP_NAME, "port")
    sid = config_parser.get(APP_NAME, "sid")
    username = config_parser.get(APP_NAME, "username")
    password = config_parser.get(APP_NAME, "password")
    
    dsn = cx_Oracle.makedsn(host,port,sid)
    con = cx_Oracle.connect(username,password,dsn)
    return con
    #logger.debug("hello world")
    #lines.foreachRDD(lambda rdd: valid_data(rdd))
    
