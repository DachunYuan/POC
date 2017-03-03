# -*- coding: utf-8 -*-

## Spark Application - execute with spark-submit

## Imports
from configparser import SafeConfigParser
from pyspark.streaming.kafka import KafkaUtils

# ==============================================================================
# Main
# ==============================================================================
def kafka2spark(ssc, topic):
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
    APP_NAME = "kafka2spark"
 
    kafka_broker_list = config_parser.get(APP_NAME, "kafka.broker.list")
    #paramater for createDirectStream
    #kafka_broker_list = "c9t26359.itcs.hpecorp.net:9092,c9t26360.itcs.hpecorp.net:9092,c9t26361.itcs.hpecorp.net:9092"
    #topics = "HPESCLTA1,HPESCLTA2,HPESCLTA3"
    topic_list = list()
    topic_list.append(topic)
    # Split topics into a dict and remove empty strings e.g. {'topic1': 1, 'topic2': 1}
    #topic = [x.strip() for x in topic.split(',')]
    #topic = filter(None, topic.strip())
    #topic = dict(zip(topic, [1] * len(topic)))

    #Obsolete function        
    #kafka_stream = KafkaUtils.createStream(ssc, zk_quorum, group, topics)
    #topics is a list kafka parameter is a dict
    kafka_stream = KafkaUtils.createDirectStream(ssc, topic_list, {"metadata.broker.list": kafka_broker_list})
    return kafka_stream
    #logger.debug("hello world")
    #lines.foreachRDD(lambda rdd: valid_data(rdd))
    
