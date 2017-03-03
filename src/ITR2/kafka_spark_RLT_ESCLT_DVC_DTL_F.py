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

from kafka2spark import kafka2spark
#from connect_oracle import connect_oracle
import cx_Oracle

from kafka import KafkaClient,SimpleProducer

import hashlib
from redis_hash import RedisOp
#from filter_ import dstct

# Module Constants
APP_LOG_CONF_FILE = "/opt/mount1/app/conf"
# APP_LOG_CONF_FILE = "hdfs:///conf/log.properties"
APP_NAME = "Kafka_Spark"
VERSION_VAL = "0.11"
global preprocess_flag, sent_flag

# ========================================================================
# Functions
# ======================================================================
def dstct(res, input_dict, output_list):
     
    records = list(eval(res.replace('[','').replace(']','')))
    filter_records = list()
    out_res = list()
    check_flag = False
     
    for record in records:
        for key in input_dict.keys():
        #if source is None then return -2
            if input_dict[key] == record[key]:
                check_flag = True
            else:
                check_flag = False
                 
    #if all conditions are satisfied append record to output        
    if check_flag == True:
        filter_records.append(tuple(record))
    check_flag = False
     
    filter_records = list(tuple(set(filter_records)))
     
    #if no key is found return 0
    if len(filter_records) == 0:
        return 0
         
    #if input is [1] return the 1st element at index 0
    for filter_records in filter_records:
        for index in output_list:
            out_res.append(tuple(filter_records)[output_list[index]])
                
    return out_res
        
def create_kafka_msg(operation, dest_table='', dest_col='', mid_table='', mid_col='', src_table='', src_col='', lookup_table='', lookup_col=[], lookup_key=''):
    
    op_connector = ': '
    connector = ' <- '
    col_connector = '.'
    eq_connector = ' = '
    
    if operation == 'MAP':
        kafka_msg = operation + op_connector + \
        dest_table + col_connector + dest_col + connector + \
        mid_table + col_connector + mid_col + connector + \
        src_table + col_connector + src_col
    elif operation == 'LKUP_MAP':
        kafka_msg = operation + op_connector + \
        dest_table + col_connector + dest_col + connector + \
        lookup_table + col_connector + lookup_key + connector
        for item in lookup_col:
            if isinstance(item, tuple):
                kafka_msg += lookup_table + col_connector + item[0] + eq_connector + '\'' + item[1] + '\''
            else:
                kafka_msg += lookup_table + col_connector + item + eq_connector
        kafka_msg += mid_table + col_connector + mid_col + connector + \
        src_table + col_connector + src_col
    elif operation == 'ASSIGN':
        kafka_msg = operation + op_connector + \
        dest_table + col_connector + dest_col
    elif operation == 'TF_TRANSFORM':
        kafka_msg = operation + op_connector + \
        dest_table + col_connector + dest_col
    else:
        pass
    
    return kafka_msg
    
def fast_get_value(redis_table_string, source, col_num, producer, topic):
    """
    input: redis_table_string    list of strings
           source           list of tuples
           col_num             tuple
    return lookup_value           value
    """
    
    if source != None:
        mod_key = redis_table_string + str(source)
        record = RedisOp(['get',mod_key])
        if record[0]:
            return tuple(eval(record[1]))[col_num]
        else:
            kafka_msg = "no record found return 0"
            producer.send_messages(topic, kafka_msg)
            return 0
    else:
        kafka_msg = "source is None return -2"
        producer.send_messages(topic, kafka_msg)
        return -2

def LKUP_MAP(dest, redis_table, input_key_tuple_list, output_key, producer, topic_test):
    r = RedisOp(['get',redis_table])
    input_dict = dict()
    #in case there are multiple conditions
    for item in input_key_tuple_list:
        key = item[0]
        value = item[1]
        if value == None:
            kafka_msg = "The input_key is None return -2"
            producer.send_messages(topic_test, kafka_msg)
            dest.__dict__[dest.get_key(output_key)] = -2
            return
        else:
            input_dict[key] = value
            #kafka_msg = "The lookup return value is " + str(value)
            #producer.send_messages(topic_test, kafka_msg)
      
    dest.__dict__[dest.get_key(output_key)] = dstct(r[1],input_dict,[0])
    input_dict.clear()
    return

def STR_TRANSFORM(source, producer, topic):
    if source != None:
        return str(source)
    else:
        kafka_msg = "The value is None return nothing"
        producer.send_messages(topic, kafka_msg)
        return ''
     
def TF_TRANSFORM(source, producer, topic):
    if source != None:
        if source == 't':
            return 'y'
        elif source == 'f':
            return 'n'
        else:
            kafka_msg = "source is neither 't' nor 'f' doing nothing"
            producer.send_messages(topic, kafka_msg)
            pass
    else:
        kafka_msg = "The source is None doing nothing"
        producer.send_messages(topic, kafka_msg)
        pass
        
# ========================================================================
# Classes
# ======================================================================
class ITR2_Tables(object):
    """ Keep common attributes for snesor, and provides generic method for different sensor.

    Args:
        None

    Attributes:
        __event_id (str): uuid value in string for uniquely identify an event in sensor
        attr_2_column_mapping (dict): Stands for mapping between instance attribute and (column name in Cassandra
         table, data type)
        karrios_data_points_list (list): a list of data points (dict) in Karrios data point:
        for example: one data point is {'timestamp': 1456939614201L, 'name': 'raw_gy_steps_ascended', 'value': 4408,
        'tags': {'user_id': u'vincent.planat@hpe.com', 'event_type': u'altimeter',
        'event_id': u'664df9f0-f838-4059-8cf2-470586a3f2af', 'training_mode': u'SITTING', 'device_type': u'msband2',
        'device_id': u'b81e905908354542'}}
    """
    def __init__(self):
        pass
    
    
    
    #def STR_transform(self, key, producer, topic_test):
        #value = self.__dict__[key]
        #if value != None:
            #return str(value)
        #else:
            #kafka_msg = "The value at _ITR2_Tables" + key + " is None"
            #producer.send_messages(topic_test, kafka_msg)
            #return ''
    def init_database_process(self, sql_query, value_list, producer, topic):
        username = 'INFR'
        password = '4.!INFRitr2DEVpw_20160504'
        host = 'g1u2115.austin.hp.com'
        port = '1525'
        sid = 'ITRD'
        
        dsn = cx_Oracle.makedsn(host,port,sid)
        con = cx_Oracle.connect(username,password,dsn)
        #con.cursor().execute(insert_query, eval(tuple(value_list)))
        producer.send_messages(topic, sql_query % tuple(value_list))
        con.cursor().execute(sql_query % tuple(value_list))
        #con.cursor().prepare('insert into game_server_name(server_id, server_name, chinese_name) values(:1, :2, :3)')
        #con.cursor().executemany(None, param)
        con.commit()
        con.cursor().close()        
        con.close()


    

    def init_process(self, src_m1, src_dvc_m1, dest, device_str, producer, topic):
        
        m1_recv_flag = RedisOp(['get','HP_ESCLT_M1_RECV_FLAG_' + device_str])
        if m1_recv_flag[0]:
            kafka_msg = "HP_ESCLT_M1_RECV_FLAG_" + device_str + " is " + m1_recv_flag[1]
            producer.send_messages(topic, kafka_msg)
        else:
            kafka_msg = m1_recv_flag[1] + " -> HP_ESCLT_M1_RECV_FLAG_" + device_str
            producer.send_messages(topic, kafka_msg)
            
        dvc_m1_recv_flag = RedisOp(['get','HP_ESCLT_DVC_M1_RECV_FLAG_' + device_str])
        if dvc_m1_recv_flag[0]:
            kafka_msg = "HP_ESCLT_DVC_M1_RECV_FLAG_" + device_str + " is " + dvc_m1_recv_flag[1]
            producer.send_messages(topic, kafka_msg)
        else:
            kafka_msg = dvc_m1_recv_flag[1] + " -> HP_ESCLT_DVC_M1_RECV_FLAG_" + device_str
            producer.send_messages(topic, kafka_msg)
            
        # only process if both part are received
        if m1_recv_flag[1] == 'true' and dvc_m1_recv_flag[1] == 'true':
            
            RedisOp(['set','HP_ESCLT_M1_RECV_FLAG_' + device_str,'false'])
            RedisOp(['set','HP_ESCLT_DVC_M1_RECV_FLAG_' + device_str,'false'])
            
            #for key in src.__dict__.keys():
            kafka_msg = "starting processing record for " + device_str
            producer.send_messages(topic, kafka_msg)
            res_m1 = RedisOp(['get','HP_ESCLT_M1_RECORD_' + device_str])
            res_dvc_m1 = RedisOp(['get','HP_ESCLT_DVC_M1_RECORD_' + device_str])
                
                # retrieve list from redis str and put into class dict
            src_m1.set_record(eval(res_m1[1]))
            src_dvc_m1.set_record(eval(res_dvc_m1[1]))
            
            #process_flag = RedisOp(['get','PROCESSING_FLAG_' + device_str])
            #if process_flag[1] == 'false':
                #RedisOp(['set','PROCESSING_FLAG_' + device_str,'true'])
                
            src_m1.HP_ESCLT_M1_process(dest, producer, topic)
            src_dvc_m1.HP_ESCLT_DVC_M1_process(dest, producer, topic)
                #RedisOp(['set','PROCESSING_FLAG_' + device_str,'false']) 
            #else:
                #return
            #RedisOp(['set','HP_ESCLT_M1_FIN_FLAG','true'])
            #producer.send_messages(topic_test, "HP_ESCLT_M1 starts sleeping")
            #while RedisOp(['get','HP_ESCLT_DVC_M1_FIN_FLAG']) != 'true':
            #    time.sleep(1)
                
            #return an ordered list from dict
            dest_record_list = dest.get_record()
            producer.send_messages(topic, "concatenating started")              
            #Calculate MD5 value before adding system generated values
            record_str = "".join(STR_TRANSFORM(item, producer, topic) for item in dest_record_list)
        
            producer.send_messages(topic, "MD5 calculating started")
            #calculate MD5
            md5_str = hashlib.md5(record_str).hexdigest()
            kafka_msg = "MD5 for current record is " + md5_str
            producer.send_messages(topic, kafka_msg)
            
            kafka_msg = "DATA_WHSE_ROW_ID is automatically generated"
            producer.send_messages(topic, kafka_msg)
            dest.__dict__[dest.get_key('DATA_WHSE_ROW_ID')] = 123456789123456789
                          
            kafka_msg = "SRC_SYS_KY is automatically generated"
            producer.send_messages(topic, kafka_msg)
            dest.__dict__[dest.get_key('SRC_SYS_KY')] = 123456789
                          
            kafka_msg = "SRC_SYS_INSTNC_KY is automatically generated"
            producer.send_messages(topic, kafka_msg)              
            dest.__dict__[dest.get_key('SRC_SYS_INSTNC_KY')] = 123456789
                          
            kafka_msg = "CRT_BY_JOB_ID is automatically generated"
            producer.send_messages(topic, kafka_msg)  
            dest.__dict__[dest.get_key('CRT_BY_JOB_ID')] = 1
            
            kafka_msg = "DATA_WHSE_CRT_TS is automatically generated"
            producer.send_messages(topic, kafka_msg)                
            dest.__dict__[dest.get_key('DATA_WHSE_CRT_TS')] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            
            kafka_msg = "UPD_BY_JOB_ID is automatically generated"
            producer.send_messages(topic, kafka_msg)  
            dest.__dict__[dest.get_key('UPD_BY_JOB_ID')] = 1
            
            kafka_msg = "DATA_WHSE_UPD_TS is automatically generated"
            producer.send_messages(topic, kafka_msg)  
            dest.__dict__[dest.get_key('DATA_WHSE_UPD_TS')] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            
            kafka_msg = "CRC_CHCKSM_TX is calculated and given"
            producer.send_messages(topic, kafka_msg)  
            dest.__dict__[dest.get_key('CRC_CHCKSM_TX')] = md5_str
            
            kafka_msg = "LGCL_DEL_FG is automatically generated"
            producer.send_messages(topic, kafka_msg)                
            dest.__dict__[dest.get_key('LGCL_DEL_FG')] = 'y'
                
            #RLT_ESCLT_DVC_DTL_F_EM1025232              
            res = RedisOp(['get','RLT_ESCLT_DVC_DTL_F_' + device_str])
            #kafka_msg = res[0]
            #producer.send_messages(topic, kafka_msg)
            #kafka_msg = res[1]
            #producer.send_messages(topic, kafka_msg)
            scheme_name = 'ITR23'
            table_name = 'RLT_ESCLT_DVC_DTL_F'
            if res[0]:
                # and MD5 is different do update
                kafka_msg = "MD5 from redis cache is "+ res[1]
                producer.send_messages(topic, kafka_msg)
                if res[1] != md5_str:
                    #update record into database
                    kafka_msg = "MD5 is different update the record"
                    producer.send_messages(topic, kafka_msg)
                    RedisOp(['set','RLT_ESCLT_DVC_DTL_F_' + device_str,md5_str])
                    self.issue_query_oracle(scheme_name, table_name, producer, topic, 'UPDATE')
                else:
                    # and MD5 is the same do nothing
                    kafka_msg = "MD5 is the same doing nothing"
                    producer.send_messages(topic, kafka_msg)
                    pass
                    
            else:
                #insert record into database
                kafka_msg = "MD5 is not found insert the record"
                producer.send_messages(topic, kafka_msg)
                RedisOp(['set','RLT_ESCLT_DVC_DTL_F_' + device_str,md5_str])
                self.issue_query_oracle(scheme_name, table_name, producer, topic, 'INSERT')
                
            #msg2test(producer, topic_test, str(list(record)))
            
            kafka_msg = "DONE processing record for " + device_str
            producer.send_messages(topic, kafka_msg)  
            
            
    def issue_query_oracle(self, scheme_name, table_name, producer, topic, operation):
        """Set attributes of the instance
            Args:
                table_name(str): name of table in Cassandra, which is used for metric name in Karrios.
                cassandra_session_obj(cassandra session object): reference to Cassandra session object
        Returns:
            None
        """
        value_list = []
        column_clause = ""
        value_clause = ""
        
        # Loop through attributes in the self object and generate INSERT query and issue the query
        for key in self.__dict__.keys():
               # Only get attributes of self object for polymorphism
               # if key.startswith("_" + self.__class__.__name__):
            if key.startswith("_" ):
                # Only get attributes starting with "__" in instance
                instance_attribute_name = key.replace("_" + self.__class__.__name__, "")
                column_name = self.attr_2_column_mapping[instance_attribute_name][0]
                column_type = self.attr_2_column_mapping[instance_attribute_name][1]
                # print instance_attribute_name
                column_clause = column_clause + column_name + ", "
                
                if column_type == "NUMBER":
                    value_clause += "NVL(TO_NUMBER(%s),NULL), "
                elif column_type == "DATE2NUMBER":
                    value_clause += "NVL(TO_NUMBER(TO_CHAR(TO_DATE(NULLIF('%s','NULL'),'YYYY-MM-DD HH24:MI:SS'),'YYYYMMDD')),NULL), "
                elif column_type == "DATE":
                    value_clause += "NVL(TO_DATE(NULLIF('%s','NULL'),'YYYY-MM-DD HH24:MI:SS'),NULL), "
                else:
                    value_clause += "NULLIF('%s','NULL'), "
                    
                if self.__dict__[key] == None:
                    self.__dict__[key] = 'NULL'
                    
                value_list.append(self.__dict__[key])
                #value_list.append(self.__dict__[key] if self.__dict__[key] != None or 'NULL')
        
        if operation == 'INSERT':
            kafka_msg = "inserting ..."
            producer.send_messages(topic, kafka_msg)
            sql_query = "INSERT INTO " + scheme_name + "." + table_name + "(" + column_clause.strip(", ") + ") VALUES (" + value_clause.strip(", ") + ")"
        elif operation == 'UPDATE':
            kafka_msg = "updating ... "
            producer.send_messages(topic, kafka_msg)
            sql_query = "UPDATE " + scheme_name + "." + table_name + "(SET" + column_clause.strip(", ") + ") VALUES (" + value_clause.strip(", ") + ")"
        # print insert_query
        # Issue the query
        #producer.send_messages(topic_test, insert_query % value_list)
        #con.cursor.execute_async(insert_query, value_list)
        self.init_database_process(sql_query, value_list, producer, topic)
        
class HP_ESCLT_M1(ITR2_Tables):
    def __init__(self):
        super(HP_ESCLT_M1, self).__init__()
        self.__HP_ESCLT_ID = None
        self.__HP_TTL_TX      = None        
        self.__ML_ESCLT_STAT_KY= None       
        self.__ML_ESCLT_TYPE_KY= None       
        self.__HP_REQR_EMAIL_NM= None       
        self.__HP_REQR_ORG_NM  = None       
        self.__ML_TMLNSS_TYPE_KY= None      
        self.__HP_NXT_MTG_DT    = None      
        self.__HP_INCID_DSCVR_DT= None      
        self.__HP_ICC_ENGMT_DT  = None      
        self.__HP_ESCLT_REQ_DT  = None      
        self.__HP_ESCLT_OPN_DT  = None      
        self.__HP_BUS_CNTNTY_RSTR_DT= None  
        self.__HP_ESCLT_CLOSE_DT    = None  
        self.__HP_ESCLT_CNCL_DT     = None  
        self.__HP_SRVC_ID           = None  
        self.__HP_SRVC_CATG_ID      = None  
        self.__HP_SRVC_SUBCAT_ID    = None  
        self.__HP_INIT_INCID_ID     = None  
        self.__HP_IS_TEMPL_FG       = None  
        self.__HP_TEMPL_TTL_TX      = None  
        self.__HP_TEMPL_TYPE_NM     = None  
        self.__HP_TEMPL_OWN_NM      = None  
        self.__HP_CURR_STTN_TX      = None  
        self.__HP_INIT_STTN_TX      = None  
        self.__HP_CURR_IMPC_TX      = None  
        self.__HP_INIT_IMPC_TX      = None  
        self.__HP_WRKRND_TX         = None  
        self.__HP_CNCLN_TX          = None  
        self.__HP_INIT_CHG_ID       = None  
        self.__HP_INIT_PRBLM_ID     = None  
        self.__HP_CRT_PRBLM_ID      = None  
        self.__HP_PRIM_CI_LGCL_NM   = None  
        self.__HP_PRIM_APP_PRTFL_ID = None  
        self.__HP_DTL_UPD_DN        = None  
        self.__HP_ASGN_GRP_NM         = None
        self.__HP_ESCLT_MGR_EMAIL_NM  = None
        self.__HP_ESCLT_CRT_EMAIL_NM  = None
        self.__SYSMODTIME             = None
        self.__SYSMODUSER             = None
        self.__ML_ESCLT_DEV_ENGMT_KY = None
        self.__ML_DEFN_WARR_PER_KY  = None
        #self.__DATA_WHSE_ROW_ID       = None
        #self.__SRC_SYS_KY             = None
        #self.__SRC_SYS_INSTNC_KY      = None
        #self.__CRT_BY_JOB_ID          = None
        #self.__DATA_WHSE_CRT_TS       = None
        #self.__UPD_BY_JOB_ID          = None
        #self.__DATA_WHSE_UPD_TS       = None
        #self.__CRC_CHCKSM_TX          = None
        #self.__LGCL_DEL_FG            = None
        self.__HP_DTL_UPD_TX = None
        self.__HP_EMRG_CHG_RQR_FG= None
        self.__HP_EMRG_CHG_ID= None
        self.__HP_EMRG_CHG_SBMT_EMAIL_NM= None
        self.__HP_RECUR_ISS_FG= None
        self.__ML_CPCTY_ISS_TYPE_KY= None
        self.__HP_RBT_RQR_FG= None
        self.__HP_EVDNC_DTL_TX= None
        self.__HP_VNDR_ENGMT_FG= None
        self.__VENDOR= None
        self.__HP_BUS_CNTNTY_ISS_TX= None
        self.__HP_VENDOR_INFO= None
        self.__HP_VENDOR_RESPONSE= None
        self.__HP_VENDOR_REFID_TX= None
        self.__HP_VENDOR_CON_TX= None
        self.__HP_PROACTIVEMI_FG= None
        self.__HP_RESLT_CHG_FG= None
        self.__HP_CAPCTY_ISS_FG= None
        self.__HP_KM_UPD_FG= None
        self.__HP_DEV_ENGMT_FG= None
        self.__HP_BUS_CNTNTY_ISS_FG= None
        self.__HP_CO_ID= None
        self.attr_2_column_mapping = dict()
        self.attr_2_column_mapping["__HP_ESCLT_ID"] = ("HP_ESCLT_ID", "VARCHAR2(100)")
        self.attr_2_column_mapping["__HP_TTL_TX"] = ("HP_TTL_TX", "VARCHAR2(80)")
        self.attr_2_column_mapping["__ML_ESCLT_STAT_KY"] = ("ML_ESCLT_STAT_KY", "NUMBER")
        self.attr_2_column_mapping["__ML_ESCLT_TYPE_KY"] = ("ML_ESCLT_TYPE_KY", "NUMBER")
        self.attr_2_column_mapping["__HP_REQR_EMAIL_NM"] = ("HP_REQR_EMAIL_NM", "VARCHAR2(140)")
        self.attr_2_column_mapping["__HP_REQR_ORG_NM"] = ("HP_REQR_ORG_NM", "VARCHAR2(250)")
        self.attr_2_column_mapping["__ML_TMLNSS_TYPE_KY"] = ("ML_TMLNSS_TYPE_KY", "NUMBER")
        self.attr_2_column_mapping["__HP_NXT_MTG_DT"] = ("HP_NXT_MTG_DT", "DATE")
        self.attr_2_column_mapping["__HP_INCID_DSCVR_DT"] = ("HP_INCID_DSCVR_DT", "DATE")
        self.attr_2_column_mapping["__HP_ICC_ENGMT_DT"] = ("HP_ICC_ENGMT_DT", "DATE")
        self.attr_2_column_mapping["__HP_ESCLT_REQ_DT"] = ("HP_ESCLT_REQ_DT", "DATE")
        self.attr_2_column_mapping["__HP_ESCLT_OPN_DT"] = ("HP_ESCLT_OPN_DT", "DATE")
        self.attr_2_column_mapping["__HP_BUS_CNTNTY_RSTR_DT"] = ("HP_BUS_CNTNTY_RSTR_DT", "DATE")
        self.attr_2_column_mapping["__HP_ESCLT_CLOSE_DT"] = ("HP_ESCLT_CLOSE_DT", "DATE")
        self.attr_2_column_mapping["__HP_ESCLT_CNCL_DT"] = ("HP_ESCLT_CNCL_DT", "DATE")
        self.attr_2_column_mapping["__HP_SRVC_ID"] = ("HP_SRVC_ID", "VARCHAR2(200)")
        self.attr_2_column_mapping["__HP_SRVC_CATG_ID"] = ("HP_SRVC_CATG_ID", "VARCHAR2(200)")
        self.attr_2_column_mapping["__HP_SRVC_SUBCAT_ID"] = ("HP_SRVC_SUBCAT_ID", "VARCHAR2(200)")
        self.attr_2_column_mapping["__HP_INIT_INCID_ID"] = ("HP_INIT_INCID_ID", "VARCHAR2(90)")
        self.attr_2_column_mapping["__HP_IS_TEMPL_FG"] = ("HP_IS_TEMPL_FG", "CHAR(1)")
        self.attr_2_column_mapping["__HP_TEMPL_TTL_TX"] = ("HP_TEMPL_TTL_TX", "VARCHAR2(80)")
        self.attr_2_column_mapping["__HP_TEMPL_TYPE_NM"] = ("HP_TEMPL_TYPE_NM", "VARCHAR2(60)")
        self.attr_2_column_mapping["__HP_TEMPL_OWN_NM"] = ("HP_TEMPL_OWN_NM", "VARCHAR2(60)")
        self.attr_2_column_mapping["__HP_CURR_STTN_TX"] = ("HP_CURR_STTN_TX", "VARCHAR2(4000)")
        self.attr_2_column_mapping["__HP_INIT_STTN_TX"] = ("HP_INIT_STTN_TX", "VARCHAR2(4000)")
        self.attr_2_column_mapping["__HP_CURR_IMPC_TX"] = ("HP_CURR_IMPC_TX", "VARCHAR2(4000)")
        self.attr_2_column_mapping["__HP_INIT_IMPC_TX"] = ("HP_INIT_IMPC_TX", "VARCHAR2(4000)")
        self.attr_2_column_mapping["__HP_WRKRND_TX"] = ("HP_WRKRND_TX", "VARCHAR2(4000)")
        self.attr_2_column_mapping["__HP_CNCLN_TX"] = ("HP_CNCLN_TX", "VARCHAR2(4000)")
        self.attr_2_column_mapping["__HP_INIT_CHG_ID"] = ("HP_INIT_CHG_ID", "VARCHAR2(100)")
        self.attr_2_column_mapping["__HP_INIT_PRBLM_ID"] = ("HP_INIT_PRBLM_ID", "VARCHAR2(100)")
        self.attr_2_column_mapping["__HP_CRT_PRBLM_ID"] = ("HP_CRT_PRBLM_ID", "VARCHAR2(100)")
        self.attr_2_column_mapping["__HP_PRIM_CI_LGCL_NM"] = ("HP_PRIM_CI_LGCL_NM", "VARCHAR2(200)")
        self.attr_2_column_mapping["__HP_PRIM_APP_PRTFL_ID"] = ("HP_PRIM_APP_PRTFL_ID", "NUMBER")
        self.attr_2_column_mapping["__HP_DTL_UPD_DN"] = ("HP_DTL_UPD_DN", "VARCHAR2(4000)")
        self.attr_2_column_mapping["__HP_ASGN_GRP_NM"] = ("HP_ASGN_GRP_NM", "VARCHAR2(60)")
        self.attr_2_column_mapping["__HP_ESCLT_MGR_EMAIL_NM"] = ("HP_ESCLT_MGR_EMAIL_NM", "VARCHAR2(140)")
        self.attr_2_column_mapping["__HP_ESCLT_CRT_EMAIL_NM"] = ("HP_ESCLT_CRT_EMAIL_NM", "VARCHAR2(140)")
        self.attr_2_column_mapping["__SYSMODTIME"] = ("SYSMODTIME", "DATE")
        self.attr_2_column_mapping["__SYSMODUSER"] = ("SYSMODUSER", "VARCHAR2(140)")
        self.attr_2_column_mapping["__ML_ESCLT_DEV_ENGMT_KY"] = ("ML_ESCLT_DEV_ENGMT_KY", "NUMBER")
        self.attr_2_column_mapping["__ML_DEFN_WARR_PER_KY"] = ("ML_DEFN_WARR_PER_KY", "NUMBER")
        #self.attr_2_column_mapping["__DATA_WHSE_ROW_ID"] = ("DATA_WHSE_ROW_ID", "NUMBER(18)")
        #self.attr_2_column_mapping["__SRC_SYS_KY"] = ("SRC_SYS_KY", "NUMBER(9)")
        #self.attr_2_column_mapping["__SRC_SYS_INSTNC_KY"] = ("SRC_SYS_INSTNC_KY", "NUMBER(9)")
        #self.attr_2_column_mapping["__CRT_BY_JOB_ID"] = ("CRT_BY_JOB_ID", "NUMBER")
        #self.attr_2_column_mapping["__DATA_WHSE_CRT_TS"] = ("DATA_WHSE_CRT_TS", "DATE")
        #self.attr_2_column_mapping["__UPD_BY_JOB_ID"] = ("UPD_BY_JOB_ID", "NUMBER")
        #self.attr_2_column_mapping["__DATA_WHSE_UPD_TS"] = ("DATA_WHSE_UPD_TS", "DATE")
        #self.attr_2_column_mapping["__CRC_CHCKSM_TX"] = ("CRC_CHCKSM_TX", "VARCHAR2(32 CHAR)")
        #self.attr_2_column_mapping["__LGCL_DEL_FG"] = ("LGCL_DEL_FG", "CHAR(1 CHAR)")
        self.attr_2_column_mapping["__HP_DTL_UPD_TX"] = ("HP_DTL_UPD_TX", "CLOB")
        self.attr_2_column_mapping["__HP_EMRG_CHG_RQR_FG"] = ("HP_EMRG_CHG_RQR_FG", "CHAR(1)")
        self.attr_2_column_mapping["__HP_EMRG_CHG_ID"] = ("HP_EMRG_CHG_ID", "VARCHAR2(100)")
        self.attr_2_column_mapping["__HP_EMRG_CHG_SBMT_EMAIL_NM"] = ("HP_EMRG_CHG_SBMT_EMAIL_NM", "VARCHAR2(140)")
        self.attr_2_column_mapping["__HP_RECUR_ISS_FG"] = ("HP_RECUR_ISS_FG", "CHAR(1)")
        self.attr_2_column_mapping["__ML_CPCTY_ISS_TYPE_KY"] = ("ML_CPCTY_ISS_TYPE_KY", "NUMBER")
        self.attr_2_column_mapping["__HP_RBT_RQR_FG"] = ("HP_RBT_RQR_FG", "CHAR(1)")
        self.attr_2_column_mapping["__HP_EVDNC_DTL_TX"] = ("HP_EVDNC_DTL_TX", "VARCHAR2(4000)")
        self.attr_2_column_mapping["__HP_VNDR_ENGMT_FG"] = ("HP_VNDR_ENGMT_FG", "CHAR(1)")
        self.attr_2_column_mapping["__VENDOR"] = ("VENDOR", "VARCHAR2(180)")
        self.attr_2_column_mapping["__HP_BUS_CNTNTY_ISS_TX"] = ("HP_BUS_CNTNTY_ISS_TX", "VARCHAR2(4000)")
        self.attr_2_column_mapping["__HP_VENDOR_INFO"] = ("HP_VENDOR_INFO", "VARCHAR2(1620)")
        self.attr_2_column_mapping["__HP_VENDOR_RESPONSE"] = ("HP_VENDOR_RESPONSE", "VARCHAR2(1620)")
        self.attr_2_column_mapping["__HP_VENDOR_REFID_TX"] = ("HP_VENDOR_REFID_TX", "VARCHAR2(4000)")
        self.attr_2_column_mapping["__HP_VENDOR_CON_TX"] = ("HP_VENDOR_CON_TX", "VARCHAR2(4000)")
        self.attr_2_column_mapping["__HP_PROACTIVEMI_FG"] = ("HP_PROACTIVEMI_FG", "CHAR(1)")
        self.attr_2_column_mapping["__HP_RESLT_CHG_FG"] = ("HP_RESLT_CHG_FG", "CHAR(1)")
        self.attr_2_column_mapping["__HP_CAPCTY_ISS_FG"] = ("HP_CAPCTY_ISS_FG", "CHAR(1)")
        self.attr_2_column_mapping["__HP_KM_UPD_FG"] = ("HP_KM_UPD_FG", "CHAR(1)")
        self.attr_2_column_mapping["__HP_DEV_ENGMT_FG"] = ("HP_DEV_ENGMT_FG", "CHAR(1)")
        self.attr_2_column_mapping["__HP_BUS_CNTNTY_ISS_FG"] = ("HP_BUS_CNTNTY_ISS_FG", "CHAR(1)")
        self.attr_2_column_mapping["__HP_CO_ID"] = ("HP_CO_ID", "VARCHAR2 (60)")

    def get_value(self, key):
        #return self.__dict__["_ITR2Tables_" + self.__class__.__name__ + "__" + key]
        return self.__dict__["_" + self.__class__.__name__ + "__" + key]

    def get_key(self, key):
        #key_mod = "_ITR2Tables_" + self.__class__.__name__ + "__" + key
        key_mod = "_" + self.__class__.__name__ + "__" + key
        return key_mod
            
    def set_record(self, kwargs):
        self.__HP_ESCLT_ID = kwargs[0]
        self.__HP_TTL_TX      = kwargs[1]     
        self.__ML_ESCLT_STAT_KY= kwargs[2] 
        self.__ML_ESCLT_TYPE_KY= kwargs[3]   
        self.__HP_REQR_EMAIL_NM= kwargs[4]    
        self.__HP_REQR_ORG_NM  = kwargs[5]   
        self.__ML_TMLNSS_TYPE_KY= kwargs[6] 
        self.__HP_NXT_MTG_DT    = kwargs[7]
        self.__HP_INCID_DSCVR_DT= kwargs[8]   
        self.__HP_ICC_ENGMT_DT  = kwargs[9]  
        self.__HP_ESCLT_REQ_DT  = kwargs[10]    
        self.__HP_ESCLT_OPN_DT  = kwargs[11]   
        self.__HP_BUS_CNTNTY_RSTR_DT= kwargs[12]
        self.__HP_ESCLT_CLOSE_DT    = kwargs[13]
        self.__HP_ESCLT_CNCL_DT     = kwargs[14]
        self.__HP_SRVC_ID           = kwargs[15]
        self.__HP_SRVC_CATG_ID      = kwargs[16]
        self.__HP_SRVC_SUBCAT_ID    = kwargs[17]
        self.__HP_INIT_INCID_ID     = kwargs[18]
        self.__HP_IS_TEMPL_FG       = kwargs[19]
        self.__HP_TEMPL_TTL_TX      = kwargs[20] 
        self.__HP_TEMPL_TYPE_NM     = kwargs[21]
        self.__HP_TEMPL_OWN_NM      = kwargs[22]
        self.__HP_CURR_STTN_TX      = kwargs[23] 
        self.__HP_INIT_STTN_TX      = kwargs[24]
        self.__HP_CURR_IMPC_TX      = kwargs[25] 
        self.__HP_INIT_IMPC_TX      = kwargs[26] 
        self.__HP_WRKRND_TX         = kwargs[27] 
        self.__HP_CNCLN_TX          = kwargs[28]
        self.__HP_INIT_CHG_ID       = kwargs[29]
        self.__HP_INIT_PRBLM_ID     = kwargs[30]
        self.__HP_CRT_PRBLM_ID      = kwargs[31]
        self.__HP_PRIM_CI_LGCL_NM   = kwargs[32]
        self.__HP_PRIM_APP_PRTFL_ID = kwargs[33]
        self.__HP_DTL_UPD_DN        = kwargs[34]
        self.__HP_ASGN_GRP_NM         = kwargs[35]
        self.__HP_ESCLT_MGR_EMAIL_NM  = kwargs[36]
        self.__HP_ESCLT_CRT_EMAIL_NM  = kwargs[37]
        self.__SYSMODTIME             = kwargs[38]
        self.__SYSMODUSER             = kwargs[39]
        self.__ML_ESCLT_DEV_ENGMT_KY = kwargs[40]
        self.__ML_DEFN_WARR_PER_KY  =kwargs[41]
        #self.__DATA_WHSE_ROW_ID       = kwargs[42]
        #self.__SRC_SYS_KY             = kwargs[43]
        #self.__SRC_SYS_INSTNC_KY      = kwargs[44]
        #self.__CRT_BY_JOB_ID          = kwargs[45]
        #self.__DATA_WHSE_CRT_TS       = kwargs[46]
        #self.__UPD_BY_JOB_ID          = kwargs[47]
        #self.__DATA_WHSE_UPD_TS       = kwargs[48]
        #self.__CRC_CHCKSM_TX          =kwargs[49]
        #self.__LGCL_DEL_FG            = kwargs[50]
        #self.__HP_DTL_UPD_TX = kwargs[51]
        #self.__HP_EMRG_CHG_RQR_FG= kwargs[52]
        #self.__HP_EMRG_CHG_ID= kwargs[53]
        #self.__HP_EMRG_CHG_SBMT_EMAIL_NM= kwargs[54]
        #self.__HP_RECUR_ISS_FG= kwargs[55]
        #self.__ML_CPCTY_ISS_TYPE_KY= kwargs[56]
        #self.__HP_RBT_RQR_FG= kwargs[57]
        #self.__HP_EVDNC_DTL_TX= kwargs[58]
        #self.__HP_VNDR_ENGMT_FG= kwargs[59]
        #self.__VENDOR= kwargs[60]
        #self.__HP_BUS_CNTNTY_ISS_TX=kwargs[61]
        #self.__HP_VENDOR_INFO=kwargs[62]
        #self.__HP_VENDOR_RESPONSE= kwargs[63]
        #self.__HP_VENDOR_REFID_TX= kwargs[64]
        #self.__HP_VENDOR_CON_TX=kwargs[65]
        #self.__HP_PROACTIVEMI_FG= kwargs[66]
        #self.__HP_RESLT_CHG_FG= kwargs[67]
        #self.__HP_CAPCTY_ISS_FG= kwargs[68]
        #self.__HP_KM_UPD_FG= kwargs[69]
        #self.__HP_DEV_ENGMT_FG= kwargs[70]
        #self.__HP_BUS_CNTNTY_ISS_FG= kwargs[71]
        #self.__HP_CO_ID= kwargs[72]
        self.__HP_DTL_UPD_TX = kwargs[42]
        self.__HP_EMRG_CHG_RQR_FG= kwargs[43]
        self.__HP_EMRG_CHG_ID= kwargs[44]
        self.__HP_EMRG_CHG_SBMT_EMAIL_NM= kwargs[45]
        self.__HP_RECUR_ISS_FG= kwargs[46]
        #self.__ML_CPCTY_ISS_TYPE_KY= kwargs[47]
        self.__HP_RBT_RQR_FG= kwargs[47]
        self.__HP_EVDNC_DTL_TX= kwargs[48]
        self.__HP_VNDR_ENGMT_FG= kwargs[49]
        self.__VENDOR= kwargs[50]
        self.__HP_BUS_CNTNTY_ISS_TX=kwargs[51]
        self.__HP_VENDOR_INFO=kwargs[52]
        self.__HP_VENDOR_RESPONSE= kwargs[53]
        self.__HP_VENDOR_REFID_TX= kwargs[54]
        self.__HP_VENDOR_CON_TX=kwargs[55]
        self.__HP_PROACTIVEMI_FG= kwargs[56]
        self.__HP_RESLT_CHG_FG= kwargs[57]
        self.__HP_CAPCTY_ISS_FG= kwargs[58]
        self.__HP_KM_UPD_FG= kwargs[59]
        self.__HP_DEV_ENGMT_FG= kwargs[60]
        self.__HP_BUS_CNTNTY_ISS_FG= kwargs[61]
        self.__HP_CO_ID= kwargs[62]

    def get_record(self):
        #key_mod = "_ITR2Tables_" + self.__class__.__name__ + "__" + key
        kwargs = list()
        for key in self.__dict__.keys():
            kwargs.append(key)
            
        for key in self.__dict__.keys():
            if key.startswith("_" ):
                # Only get attributes starting with "__" in instance
                key_mod = key.replace("_" + self.__class__.__name__, "")
                if key_mod == '__HP_ESCLT_ID':
                    kwargs[0] = self.__dict__[key]
                if key_mod == '__HP_TTL_TX':
                    kwargs[1] = self.__dict__[key]
                if key_mod == '__ML_ESCLT_STAT_KY':
                    kwargs[2] = self.__dict__[key]   
                if key_mod == '__ML_ESCLT_TYPE_KY':
                    kwargs[3] = self.__dict__[key]
                if key_mod == '__HP_REQR_EMAIL_NM':
                    kwargs[4] = self.__dict__[key]
                if key_mod == '__HP_REQR_ORG_NM':
                    kwargs[5] = self.__dict__[key]
                if key_mod == '__ML_TMLNSS_TYPE_KY':
                    kwargs[6] = self.__dict__[key]
                if key_mod == '__HP_NXT_MTG_DT':
                    kwargs[7] = self.__dict__[key]
                if key_mod == '__HP_INCID_DSCVR_DT':
                    kwargs[8] = self.__dict__[key]
                if key_mod == '__HP_ICC_ENGMT_DT':
                    kwargs[9] = self.__dict__[key]
                if key_mod == '__HP_ESCLT_REQ_DT':
                    kwargs[10] = self.__dict__[key]
                if key_mod == '__HP_ESCLT_OPN_DT':
                    kwargs[11] = self.__dict__[key]
                if key_mod == '__HP_BUS_CNTNTY_RSTR_DT':
                    kwargs[12] = self.__dict__[key]
                if key_mod == '__HP_ESCLT_CLOSE_DT':
                    kwargs[13] = self.__dict__[key]
                if key_mod == '__HP_ESCLT_CNCL_DT':
                    kwargs[14] = self.__dict__[key]
                if key_mod == '__HP_SRVC_ID':
                    kwargs[15] = self.__dict__[key]
                if key_mod == '__HP_SRVC_CATG_ID':
                    kwargs[16] = self.__dict__[key]
                if key_mod == '__HP_SRVC_SUBCAT_ID':
                    kwargs[17] = self.__dict__[key]
                if key_mod == '__HP_INIT_INCID_ID':
                    kwargs[18] = self.__dict__[key]
                if key_mod == '__HP_IS_TEMPL_FG':
                    kwargs[19] = self.__dict__[key]
                if key_mod == '__HP_TEMPL_TTL_TX':
                    kwargs[20] = self.__dict__[key]
                if key_mod == '__HP_TEMPL_TYPE_NM':
                    kwargs[21] = self.__dict__[key]
                if key_mod == '__HP_TEMPL_OWN_NM':
                    kwargs[22] = self.__dict__[key]
                if key_mod == '__HP_CURR_STTN_TX':
                    kwargs[23] = self.__dict__[key]
                if key_mod == '__HP_INIT_STTN_TX':
                    kwargs[24] = self.__dict__[key]
                if key_mod == '__HP_CURR_IMPC_TX':
                    kwargs[25] = self.__dict__[key]
                if key_mod == '__HP_INIT_IMPC_TX':
                    kwargs[26] = self.__dict__[key]
                if key_mod == '__HP_WRKRND_TX':
                    kwargs[27] = self.__dict__[key]
                if key_mod == '__HP_CNCLN_TX':
                    kwargs[28] = self.__dict__[key]
                if key_mod == '__HP_INIT_CHG_ID':
                    kwargs[29] = self.__dict__[key]
                if key_mod == '__HP_INIT_PRBLM_ID':
                    kwargs[30] = self.__dict__[key]
                if key_mod == '__HP_CRT_PRBLM_ID':
                    kwargs[31] = self.__dict__[key]
                if key_mod == '__HP_PRIM_CI_LGCL_NM':
                    kwargs[32] = self.__dict__[key]
                if key_mod == '__HP_PRIM_APP_PRTFL_ID':
                    kwargs[33] = self.__dict__[key]
                if key_mod == '__HP_DTL_UPD_DN':
                    kwargs[34] = self.__dict__[key]
                if key_mod == '__HP_ASGN_GRP_NM':
                    kwargs[35] = self.__dict__[key]
                if key_mod == '__HP_ESCLT_MGR_EMAIL_NM':
                    kwargs[36] = self.__dict__[key]
                if key_mod == '__HP_ESCLT_CRT_EMAIL_NM':
                    kwargs[37] = self.__dict__[key]
                if key_mod == '__SYSMODTIME':
                    kwargs[38] = self.__dict__[key]
                if key_mod == '__SYSMODUSER':
                    kwargs[39] = self.__dict__[key]
                if key_mod == '__ML_ESCLT_DEV_ENGMT_KY':
                    kwargs[40] = self.__dict__[key]
                if key_mod == '__ML_DEFN_WARR_PER_KY':
                    kwargs[41] = self.__dict__[key]
                #self.__DATA_WHSE_ROW_ID':
                    #kwargs[42] = self.__dict__[key]
                #self.__SRC_SYS_KY':
                    #kwargs[43] = self.__dict__[key]
                #self.__SRC_SYS_INSTNC_KY':
                    #kwargs[44] = self.__dict__[key]
                #self.__CRT_BY_JOB_ID':
                    #kwargs[45] = self.__dict__[key]
                #self.__DATA_WHSE_CRT_TS':
                    #kwargs[46] = self.__dict__[key]
                #self.__UPD_BY_JOB_ID':
                    #kwargs[47] = self.__dict__[key]
                #self.__DATA_WHSE_UPD_TS':
                    #kwargs[48] = self.__dict__[key]
                #self.__CRC_CHCKSM_TX':
                    #kwargs[49] = self.__dict__[key]
                #self.__LGCL_DEL_FG':
                    #kwargs[50] = self.__dict__[key]
                #self.__HP_S2S_RDY_FG':
                    #kwargs[51] = self.__dict__[key]
                #self.__HP_INSTNC_PRTFL_ID':
                    #kwargs[52] = self.__dict__[key]
                if key_mod == '__HP_DTL_UPD_TX':
                    kwargs[42] = self.__dict__[key]
                if key_mod == '__HP_EMRG_CHG_RQR_FG':
                    kwargs[43] = self.__dict__[key]
                if key_mod == '__HP_EMRG_CHG_ID':
                    kwargs[44] = self.__dict__[key]
                if key_mod == '__HP_EMRG_CHG_SBMT_EMAIL_NM':
                    kwargs[45] = self.__dict__[key]
                if key_mod == '__HP_RECUR_ISS_FG':
                    kwargs[46] = self.__dict__[key]
                if key_mod == '__HP_RBT_RQR_FG':
                    kwargs[47] = self.__dict__[key]
                if key_mod == '__HP_EVDNC_DTL_TX':
                    kwargs[48] = self.__dict__[key]
                if key_mod == '__HP_VNDR_ENGMT_FG':
                    kwargs[49] = self.__dict__[key]
                if key_mod == '__VENDOR':
                    kwargs[50] = self.__dict__[key]
                if key_mod == '__HP_BUS_CNTNTY_ISS_TX':
                    kwargs[51] = self.__dict__[key]
                if key_mod == '__HP_VENDOR_INFO':
                    kwargs[52] = self.__dict__[key]
                if key_mod == '__HP_VENDOR_RESPONSE':
                    kwargs[53] = self.__dict__[key]
                if key_mod == '__HP_VENDOR_REFID_TX':
                    kwargs[54] = self.__dict__[key]
                if key_mod == '__HP_VENDOR_CON_TX':
                    kwargs[55] = self.__dict__[key]
                if key_mod == '__HP_PROACTIVEMI_FG':
                    kwargs[56] = self.__dict__[key]
                if key_mod == '__HP_RESLT_CHG_FG':
                    kwargs[57] = self.__dict__[key]
                if key_mod == '__HP_CAPCTY_ISS_FG':
                    kwargs[58] = self.__dict__[key]
                if key_mod == '__HP_KM_UPD_FG':
                    kwargs[59] = self.__dict__[key]
                if key_mod == '__HP_DEV_ENGMT_FG':
                    kwargs[60] = self.__dict__[key]
                if key_mod == '__HP_BUS_CNTNTY_ISS_FG':
                    kwargs[61] = self.__dict__[key]
                if key_mod == '__HP_CO_ID':
                    kwargs[62] = self.__dict__[key]
        return kwargs
    
    def HP_ESCLT_M1_process(self, dest, producer, topic):
        
        src_table = 'ITR21.HPESCLTM1'
        mid_table = 'ITR22.APP_ESCLT'
        dest_table = 'ITR23.ESCLT_DVC_DTL_F'
        #=========================================================================================================
        #TF_TRANSFORM
        #=========================================================================================================
        dest_table = 'ITR21.HPESCLTM1'
        operation = 'TF_TRANSFORM'
        
        kafka_msg = operation + ' started for ' + src_table
        producer.send_messages(topic, kafka_msg)
        
        dest_col = 'HP_EMRG_CHG_RQR_FG'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col))
        self.__dict__[self.get_key(dest_col)] = TF_TRANSFORM(self.get_value(dest_col), producer, topic)
        
        dest_col = 'HP_RECUR_ISS_FG'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col))
        self.__dict__[self.get_key(dest_col)] = TF_TRANSFORM(self.get_value(dest_col), producer, topic)
        
        dest_col = 'HP_RBT_RQR_FG'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col))    
        self.__dict__[self.get_key(dest_col)] = TF_TRANSFORM(self.get_value(dest_col), producer, topic)
        
        dest_col = 'HP_VNDR_ENGMT_FG'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col))
        self.__dict__[self.get_key(dest_col)] = TF_TRANSFORM(self.get_value(dest_col), producer, topic)
        
        #=========================================================================================================
        #MAP
        #=========================================================================================================
        dest_table = 'ITR23.ESCLT_DVC_DTL_F'
        operation = 'MAP'
        
        kafka_msg = operation + ' started for ' + src_table
        producer.send_messages(topic, kafka_msg)
        
        src_col = 'HP_NXT_MTG_DT'
        mid_col = 'NEXT_MTNG_DT'
        dest_col = 'NEXT_MTNG_DT_DAY_KY'             
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col))
        dest.__dict__[dest.get_key(dest_col)] = self.get_value(src_col)
        
        src_col = 'HP_INCID_DSCVR_DT'
        mid_col = 'INCID_DSCVR_DT'
        dest_col = 'INCID_DSCVR_DT_DAY_KY'     
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col))
        dest.__dict__[dest.get_key(dest_col)] = self.get_value(src_col)
        
        src_col = 'HP_ICC_ENGMT_DT'
        mid_col = 'ICC_ENGMT_DT'
        dest_col = 'ICC_ENGMT_DT_DAY_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col))
        dest.__dict__[dest.get_key(dest_col)] = self.get_value(src_col)
        
        src_col = 'HP_ESCLT_REQ_DT'
        mid_col = 'ESCLT_REQ_DT'
        dest_col = 'ESCLT_REQ_DT_DAY_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col))
        dest.__dict__[dest.get_key(dest_col)] = self.get_value(src_col)
        
        src_col = 'HP_ESCLT_OPN_DT'
        mid_col = 'ESCLT_OPEN_DT'
        dest_col = 'ESCLT_OPEN_DT_DAY_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col))
        dest.__dict__[dest.get_key(dest_col)] = self.get_value(src_col)
        
        src_col = 'HP_BUS_CNTNTY_RSTR_DT'
        mid_col = 'BUS_CNTNTY_RESTOR_DT'
        dest_col = 'BUS_CNTNTY_RESTOR_DT_DAY_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col))
        dest.__dict__[dest.get_key(dest_col)] = self.get_value(src_col)
        
        src_col = 'HP_ESCLT_CLOSE_DT'
        mid_col = 'ESCLT_CLOSE_DT'
        dest_col = 'ESCLT_CLOSE_DT_DAY_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col))
        dest.__dict__[dest.get_key(dest_col)] = self.get_value(src_col)
        
        src_col = 'HP_ESCLT_CNCL_DT'
        mid_col = 'ESCLT_CNCL_DT'
        dest_col = 'ESCLT_CNCL_DT_DAY_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col))
        dest.__dict__[dest.get_key(dest_col)] = self.get_value(src_col)
        
        #=========================================================================================================
        #LKUP_MAP
        #=========================================================================================================
        operation = 'LKUP_MAP'
        
        kafka_msg = operation + ' started for ' + src_table
        producer.send_messages(topic, kafka_msg)
        
        lookup_table = 'ASGN_GRP_D'
        lookup_col = ['ASGN_GRP_NM']
        lookup_key = 'ASGN_GRP_KY'
        
        src_col = 'HP_ASGN_GRP_NM'
        mid_col = 'ASGN_GRP_NM'
        dest_col = 'ASGN_GRP_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        LKUP_MAP(dest, lookup_table, [(1,self.get_value(src_col))], dest_col, producer, topic)
        
        #===================================================================
        #HR_PERS_D fast lookup
        #===================================================================
        lookup_table = 'HR_PERS_D'
        lookup_col = ['EMAIL_ADDR_NM']
        lookup_key = 'EMP_KY'
        
        src_col = 'HP_ESCLT_MGR_EMAIL_NM'
        mid_col = 'ESCLT_MGR_EMAIL_NM'
        dest_col = 'ESCLT_MGR_EMP_KY'
        col_num = 1
        producer.send_messages(topic, str(self.get_value(src_col)))
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        dest.__dict__[dest.get_key(dest_col)] = fast_get_value(lookup_table, self.get_value(src_col), col_num, producer, topic)     
        
        src_col = 'HP_ESCLT_CRT_EMAIL_NM'
        mid_col = 'ESCLT_CRT_EMAIL_NM'
        dest_col = 'ESCLT_CRT_EMP_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        dest.__dict__[dest.get_key(dest_col)] = fast_get_value(lookup_table, self.get_value(src_col), col_num, producer, topic)     
        
        src_col = 'HP_REQR_EMAIL_NM'
        mid_col = 'REQR_EMAIL_NM'
        dest_col = 'ESCLT_REQR_EMP_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        dest.__dict__[dest.get_key(dest_col)] = fast_get_value(lookup_table, self.get_value(src_col), col_num, producer, topic)     
        
        lookup_table = 'HPSC_MSTR_L_CD_D'
        lookup_col = [('MSTR_L_COL_ID','ML_ESCLT_STAT_KY'),'VALU_ID']
        lookup_key = 'HPSC_MSTR_L_KY'
        
        src_col = 'ML_ESCLT_STAT_KY'
        mid_col = 'MSTR_L_ESCLT_STAT_KY'
        dest_col = 'MSTR_L_ESCLT_STAT_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        LKUP_MAP(dest, lookup_table, [(6,lookup_col[0][1]),(7,self.get_value(src_col))], dest_col, producer, topic)
        
        lookup_col = [('MSTR_L_COL_ID','ML_ESCLT_TYPE_KY'),'VALU_ID']
        src_col = 'ML_ESCLT_TYPE_KY'
        mid_col = 'MSTR_L_ESCLT_TYPE_KY'
        dest_col = 'MSTR_L_ESCLT_TYPE_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        LKUP_MAP(dest, lookup_table, [(6,lookup_col[0][1]),(7,self.get_value(src_col))], dest_col, producer, topic)
        
        lookup_col = [('MSTR_L_COL_ID','ML_TMLNSS_TYPE_KY'),'VALU_ID']
        src_col = 'ML_TMLNSS_TYPE_KY'
        mid_col = 'MSTR_L_TMLNSS_TYPE_KY'
        dest_col = 'MSTR_L_ESCLT_TMLNSS_TYPE_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        LKUP_MAP(dest, lookup_table, [(6,lookup_col[0][1]),(7,self.get_value(src_col))], dest_col, producer, topic)
        
        lookup_col = [('MSTR_L_COL_ID','ML_DEV_ENGMT_KY'),'VALU_ID']
        src_col = 'ML_ESCLT_DEV_ENGMT_KY'
        mid_col = 'MSTR_L_DEV_ENGMT_KY'
        dest_col = 'MSTR_L_DEV_ENGMT_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        LKUP_MAP(dest, lookup_table, [(6,lookup_col[0][1]),(7,self.get_value(src_col))], dest_col, producer, topic)
        
        lookup_col = [('MSTR_L_COL_ID','ML_DEFN_WARR_PER_KY'),'VALU_ID']
        src_col = 'ML_DEFN_WARR_PER_KY'
        mid_col = 'MSTR_L_DEFN_WARR_PER_KY'
        dest_col = 'MSTR_L_DEFN_WARR_PER_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        LKUP_MAP(dest, lookup_table, [(6,lookup_col[0][1]),(7,self.get_value(src_col))], dest_col, producer, topic)
        
        
        kafka_msg = "table process finished successfully for " + src_table
        producer.send_messages(topic, kafka_msg)

class HP_ESCLT_DVC_M1(ITR2_Tables):
    def __init__(self):
        super(HP_ESCLT_DVC_M1, self).__init__()
        self.__HP_ESCLT_DVC_ID  = None           
        self.__HP_ESCLT_ID      = None           
        self.__LOGICAL_NAME     = None           
        self.__HP_IT_ASSET_OWN_ORG_HIER1_TX= None
        self.__HP_IT_ASSET_OWN_ORG_HIER1_ID= None
        self.__HP_SUPP_OWN_ORG_HIER1_TX    = None
        self.__HP_SUPP_OWN_ORG_HIER1_ID    = None
        self.__HP_APP_PRTFL_ID             = None
        self.__HP_S2S_ENBL_FG              = None
        self.__ML_RGN_KY                   = None
        self.__HP_S2S_STAT_TX              = None
        self.__ML_FAIL_OVR_STAT_KY         = None
        self.__ML_LAST_AVL_ESCLT_TYPE_KY   = None
        self.__ML_LAST_AVL_TMLNSS_TYPE_KY  = None
        self.__HP_LAST_AVL_STRT_DT         = None
        self.__HP_LAST_AVL_END_DT          = None
        self.__HP_ESCLT_AVL_ID             = None
        self.__HP_DVC_TYPE_NM              = None
        self.__HP_PRIM_CI_FG               = None
        self.__SYSMODTIME                  = None
        self.__SYSMODUSER                  = None
        #self.__DATA_WHSE_ROW_ID       = None
        #self.__SRC_SYS_KY             = None
        #self.__SRC_SYS_INSTNC_KY      = None
        #self.__CRT_BY_JOB_ID          = None
        #self.__DATA_WHSE_CRT_TS       = None
        #self.__UPD_BY_JOB_ID          = None
        #self.__DATA_WHSE_UPD_TS       = None
        #self.__CRC_CHCKSM_TX          = None
        #self.__LGCL_DEL_FG            = None
        self.__HP_S2S_RDY_FG               = None  
        self.__HP_INSTNC_PRTFL_ID          = None
        self.attr_2_column_mapping = dict()
        self.attr_2_column_mapping["__HP_ESCLT_DVC_ID"] = ("HP_ESCLT_DVC_ID", "NUMBER")
        self.attr_2_column_mapping["__HP_ESCLT_ID"] = ("HP_ESCLT_ID", "VARCHAR2(100 CHAR)")
        self.attr_2_column_mapping["__LOGICAL_NAME"] = ("LOGICAL_NAME", "VARCHAR2(200 CHAR)")
        self.attr_2_column_mapping["__HP_IT_ASSET_OWN_ORG_HIER1_TX"] = ("HP_IT_ASSET_OWN_ORG_HIER1_TX", "VARCHAR2(250 CHAR)")
        self.attr_2_column_mapping["__HP_IT_ASSET_OWN_ORG_HIER1_ID"] = ("HP_IT_ASSET_OWN_ORG_HIER1_ID", "NUMBER")
        self.attr_2_column_mapping["__HP_APP_PRTFL_ID"] = ("HP_APP_PRTFL_ID", "NUMBER")
        self.attr_2_column_mapping["__HP_S2S_ENBL_FG"] = ("HP_S2S_ENBL_FG", "CHAR(1 CHAR)")
        self.attr_2_column_mapping["__ML_RGN_KY"] = ("ML_RGN_KY", "NUMBER")
        self.attr_2_column_mapping["__HP_S2S_STAT_TX"] = ("HP_S2S_STAT_TX", "VARCHAR2(1000 CHAR)")
        self.attr_2_column_mapping["__ML_FAIL_OVR_STAT_KY"] = ("ML_FAIL_OVR_STAT_KY", "NUMBER")
        self.attr_2_column_mapping["__ML_LAST_AVL_ESCLT_TYPE_KY"] = ("ML_LAST_AVL_ESCLT_TYPE_KY", "NUMBER")
        self.attr_2_column_mapping["__ML_LAST_AVL_TMLNSS_TYPE_KY"] = ("ML_LAST_AVL_TMLNSS_TYPE_KY", "NUMBER")
        self.attr_2_column_mapping["__HP_LAST_AVL_STRT_DT"] = ("HP_LAST_AVL_STRT_DT", "DATE")
        self.attr_2_column_mapping["__HP_LAST_AVL_END_DT"] = ("HP_LAST_AVL_END_DT", "DATE")
        self.attr_2_column_mapping["__HP_ESCLT_AVL_ID"] = ("HP_ESCLT_AVL_ID", "NUMBER")
        self.attr_2_column_mapping["__HP_DVC_TYPE_NM"] = ("HP_DVC_TYPE_NM", "VARCHAR2(60 CHAR)")
        self.attr_2_column_mapping["__HP_PRIM_CI_FG"] = ("HP_PRIM_CI_FG", "CHAR(1 CHAR)")
        self.attr_2_column_mapping["__SYSMODTIME"] = ("SYSMODTIME", "DATE")
        self.attr_2_column_mapping["__SYSMODUSER"] = ("SYSMODUSER", "VARCHAR2(140 CHAR)")
        #self.attr_2_column_mapping["__DATA_WHSE_ROW_ID"] = ("DATA_WHSE_ROW_ID", "NUMBER(18)")
        #self.attr_2_column_mapping["__SRC_SYS_KY"] = ("SRC_SYS_KY", "NUMBER(9)")
        #self.attr_2_column_mapping["__SRC_SYS_INSTNC_KY"] = ("SRC_SYS_INSTNC_KY", "NUMBER(9)")
        #self.attr_2_column_mapping["__CRT_BY_JOB_ID"] = ("CRT_BY_JOB_ID", "NUMBER")
        #self.attr_2_column_mapping["__DATA_WHSE_CRT_TS"] = ("DATA_WHSE_CRT_TS", "DATE")
        #self.attr_2_column_mapping["__UPD_BY_JOB_ID"] = ("UPD_BY_JOB_ID", "NUMBER")
        #self.attr_2_column_mapping["__DATA_WHSE_UPD_TS"] = ("DATA_WHSE_UPD_TS", "DATE")
        #self.attr_2_column_mapping["__CRC_CHCKSM_TX"] = ("CRC_CHCKSM_TX", "VARCHAR2(32 CHAR)")
        #self.attr_2_column_mapping["__LGCL_DEL_FG"] = ("LGCL_DEL_FG", "CHAR(1 CHAR)")
        self.attr_2_column_mapping["__HP_S2S_RDY_FG"] = ("HP_S2S_RDY_FG", "CHAR(1)")
        self.attr_2_column_mapping["__HP_INSTNC_PRTFL_ID"] = ("HP_INSTNC_PRTFL_ID", "NUMBER")

    def get_value(self, key):
        #return self.__dict__["_ITR2Tables_" + self.__class__.__name__ + "__" + key]
        return self.__dict__["_" + self.__class__.__name__ + "__" + key]

    def get_key(self, key):
        #key_mod = "_ITR2Tables_" + self.__class__.__name__ + "__" + key
        key_mod = "_" + self.__class__.__name__ + "__" + key
        return key_mod
    
    def LKUP_map(self, dest, redis_table, input_key_tuple_list, output_key, producer, topic_test):
        r = RedisOp(['get',redis_table])
        input_dict = dict()
        #in case there are multiple conditions
        for item in input_key_tuple_list:
            key = item[0]
            value = item[1]
            if value == None:
                kafka_msg = "The input_key is None return -2"
                producer.send_messages(topic_test, kafka_msg)
                dest.__dict__[dest.get_key(output_key)] = -2
                return
            else:
                input_dict[key] = value
        
        dest.__dict__[dest.get_key(output_key)] = super(HP_ESCLT_DVC_M1, self).dstct(r[1],input_dict,[0])
        input_dict.clear()
        return
    
    def get_record(self):
        #key_mod = "_ITR2Tables_" + self.__class__.__name__ + "__" + key
        kwargs = list()
        for key in self.__dict__.keys():
            kwargs.append(key)
            
        for key in self.__dict__.keys():
            if key.startswith("_" ):
                # Only get attributes starting with "__" in instance
                key_mod = key.replace("_" + self.__class__.__name__, "")
                if key_mod == '__HP_ESCLT_DVC_ID':
                    kwargs[0] = self.__dict__[key]
                if key_mod == '__HP_ESCLT_ID':
                    kwargs[1] = self.__dict__[key]
                if key_mod == '__LOGICAL_NAME':
                    kwargs[2] = self.__dict__[key]   
                if key_mod == '__HP_IT_ASSET_OWN_ORG_HIER1_TX':
                    kwargs[3] = self.__dict__[key]
                if key_mod == '__HP_IT_ASSET_OWN_ORG_HIER1_ID':
                    kwargs[4] = self.__dict__[key]
                if key_mod == '__HP_SUPP_OWN_ORG_HIER1_TX':
                    kwargs[5] = self.__dict__[key]
                if key_mod == '__HP_SUPP_OWN_ORG_HIER1_ID':
                    kwargs[6] = self.__dict__[key]
                if key_mod == '__HP_APP_PRTFL_ID':
                    kwargs[7] = self.__dict__[key]
                if key_mod == '__HP_S2S_ENBL_FG':
                    kwargs[8] = self.__dict__[key]
                if key_mod == '__ML_RGN_KY':
                    kwargs[9] = self.__dict__[key]
                if key_mod == '__HP_S2S_STAT_TX':
                    kwargs[10] = self.__dict__[key]
                if key_mod == '__ML_FAIL_OVR_STAT_KY':
                    kwargs[11] = self.__dict__[key]
                if key_mod == '__ML_LAST_AVL_ESCLT_TYPE_KY':
                    kwargs[12] = self.__dict__[key]
                if key_mod == '__ML_LAST_AVL_TMLNSS_TYPE_KY':
                    kwargs[13] = self.__dict__[key]
                if key_mod == '__HP_LAST_AVL_STRT_DT':
                    kwargs[14] = self.__dict__[key]
                if key_mod == '__HP_LAST_AVL_END_DT':
                    kwargs[15] = self.__dict__[key]
                if key_mod == '__HP_ESCLT_AVL_ID':
                    kwargs[16] = self.__dict__[key]
                if key_mod == '__HP_DVC_TYPE_NM':
                    kwargs[17] = self.__dict__[key]
                if key_mod == '__HP_PRIM_CI_FG':
                    kwargs[18] = self.__dict__[key]
                if key_mod == '__SYSMODTIME':
                    kwargs[19] = self.__dict__[key]
                if key_mod == '__SYSMODUSER':
                    kwargs[20] = self.__dict__[key]
                #self.__DATA_WHSE_ROW_ID':
                    #kwargs[21] = self.__dict__[key]
                #self.__SRC_SYS_KY':
                    #kwargs[22] = self.__dict__[key]
                #self.__SRC_SYS_INSTNC_KY':
                    #kwargs[23] = self.__dict__[key]
                #self.__CRT_BY_JOB_ID':
                    #kwargs[24] = self.__dict__[key]
                #self.__DATA_WHSE_CRT_TS':
                    #kwargs[25] = self.__dict__[key]
                #self.__UPD_BY_JOB_ID':
                    #kwargs[26] = self.__dict__[key]
                #self.__DATA_WHSE_UPD_TS':
                    #kwargs[27] = self.__dict__[key]
                #self.__CRC_CHCKSM_TX':
                    #kwargs[28] = self.__dict__[key]
                #self.__LGCL_DEL_FG':
                    #kwargs[29] = self.__dict__[key]
                if key_mod == '__HP_S2S_RDY_FG':
                    kwargs[21] = self.__dict__[key]
                if key_mod == '__HP_INSTNC_PRTFL_ID':
                    kwargs[22] = self.__dict__[key]
        return kwargs
        
    def set_record(self, kwargs):
        self.__HP_ESCLT_DVC_ID  = kwargs[0]
        self.__HP_ESCLT_ID      = kwargs[1]         
        self.__LOGICAL_NAME     = kwargs[2]   
        self.__HP_IT_ASSET_OWN_ORG_HIER1_TX= kwargs[3]
        self.__HP_IT_ASSET_OWN_ORG_HIER1_ID= kwargs[4]
        self.__HP_SUPP_OWN_ORG_HIER1_TX    = kwargs[5]
        self.__HP_SUPP_OWN_ORG_HIER1_ID    = kwargs[6]
        self.__HP_APP_PRTFL_ID             = kwargs[7]
        self.__HP_S2S_ENBL_FG              = kwargs[8]
        self.__ML_RGN_KY                   = kwargs[9]
        self.__HP_S2S_STAT_TX              = kwargs[10]
        self.__ML_FAIL_OVR_STAT_KY         = kwargs[11]
        self.__ML_LAST_AVL_ESCLT_TYPE_KY   = kwargs[12]
        self.__ML_LAST_AVL_TMLNSS_TYPE_KY  = kwargs[13]
        self.__HP_LAST_AVL_STRT_DT         = kwargs[14]
        self.__HP_LAST_AVL_END_DT          = kwargs[15]
        self.__HP_ESCLT_AVL_ID             = kwargs[16]
        self.__HP_DVC_TYPE_NM              = kwargs[17]
        self.__HP_PRIM_CI_FG               = kwargs[18]
        self.__SYSMODTIME                  = kwargs[19]
        self.__SYSMODUSER                  = kwargs[20]
        #self.__DATA_WHSE_ROW_ID       = kwargs[21]
        #self.__SRC_SYS_KY             = kwargs[22]
        #self.__SRC_SYS_INSTNC_KY      = kwargs[23]
        #self.__CRT_BY_JOB_ID          = kwargs[24]
        #self.__DATA_WHSE_CRT_TS       = kwargs[25]
        #self.__UPD_BY_JOB_ID          = kwargs[26]
        #self.__DATA_WHSE_UPD_TS       = kwargs[27]
        #self.__CRC_CHCKSM_TX          = kwargs[28]
        #self.__LGCL_DEL_FG            = kwargs[29]
        self.__HP_S2S_RDY_FG               = kwargs[21]
        self.__HP_INSTNC_PRTFL_ID          = kwargs[22]

    def HP_ESCLT_DVC_M1_process(self, dest, producer, topic):
        
        src_table = 'ITR21.HPESCLTDVCM1'
        mid_table = 'ITR22.APP_ESCLT_DVC'
        dest_table = 'ITR23.ESCLT_DVC_DTL_F'
        #=========================================================================================================
        #MAP
        #=========================================================================================================
        operation = 'MAP'
        
        kafka_msg = operation + ' started for ' + src_table
        producer.send_messages(topic, kafka_msg)
        
        dest_col = 'ESCLT_DVC_ID'
        mid_col = 'ESCLT_DVC_ID'
        src_col = 'HP_ESCLT_DVC_ID'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col))        
        dest.__dict__[dest.get_key(dest_col)] = self.get_value(src_col)
        
        dest_col = 'ESCLT_ID'
        mid_col = 'ESCLT_ID'
        src_col = 'HP_ESCLT_ID'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col))        
        dest.__dict__[dest.get_key(dest_col)] = self.get_value(src_col)
        
        dest_col = 'APP_PRTFL_ID'
        mid_col = 'APP_PRTFL_ID'
        src_col = 'HP_APP_PRTFL_ID'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col))      
        dest.__dict__[dest.get_key(dest_col)] = self.get_value(src_col)
        
        dest_col = 'LAST_AVL_EVNT_STRT_DT_DAY_KY'
        mid_col = 'LAST_AVL_EVNT_STRT_DT'
        src_col = 'HP_LAST_AVL_STRT_DT'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col))      
        dest.__dict__[dest.get_key(dest_col)] = self.get_value(src_col)
        
        dest_col = 'LAST_AVL_EVNT_END_DT_DAY_KY'
        mid_col = 'LAST_AVL_EVNT_END_DT'
        src_col = 'HP_LAST_AVL_END_DT'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col))      
        dest.__dict__[dest.get_key(dest_col)] = self.get_value(src_col)
        
        #=========================================================================================================
        #LKUP_MAP
        #=========================================================================================================
        operation = 'LKUP_MAP'
        
        kafka_msg = operation + ' started for ' + src_table
        producer.send_messages(topic, kafka_msg)
        
        lookup_table = 'APP_D'
        lookup_col = ['APP_CI_LGCL_NM']
        lookup_key = 'APP_KY'
        dest_col = 'APP_KY'
        mid_col = 'ESCLT_CI_LGCL_NM'
        src_col = 'LOGICAL_NAME'
        producer.send_messages(topic, str(self.get_value(src_col)))
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        LKUP_MAP(dest, lookup_table, [(1,self.get_value(src_col))], dest_col, producer, topic)
        
        #===================================================================
        #CI_D fast lookup
        #=======================================================================
        lookup_table = 'CI_D'
        lookup_col = ['CI_LGCL_NM']
        lookup_key = 'CI_D_KY'
        dest_col = 'CI_D_KY'
        mid_col = 'ESCLT_CI_LGCL_NM'
        src_col = 'LOGICAL_NAME'
        col_num = 1
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        
        dest.__dict__[dest.get_key(dest_col)] = fast_get_value(lookup_table, self.get_value(src_col), col_num, producer, topic)       
        
        lookup_table = 'ASGN_GRP_ORG_HIER_D'
        lookup_col = ['ASGN_GRP_ORG_HIER_ID']
        lookup_key = 'ASGN_GRP_ORG_HIER_KY'
        
        dest_col = 'IT_OWN_ASGN_GRP_ORG_HIER_KY'
        mid_col = 'IT_ASSET_OWN_ORG_HIER_ID'
        src_col = 'HP_IT_ASSET_OWN_ORG_HIER1_ID'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        LKUP_MAP(dest, lookup_table, [(1,self.get_value(src_col))], dest_col, producer, topic)
        
        dest_col = 'SUPP_OWN_ASGN_GRP_ORG_HIER_KY'
        mid_col = 'SUPP_OWN_ORG_HIER_ID'
        src_col = 'HP_SUPP_OWN_ORG_HIER1_ID'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        LKUP_MAP(dest, lookup_table, [(1,self.get_value(src_col))], dest_col, producer, topic)
        
        lookup_table = 'HPSC_MSTR_L_CD_D'
        lookup_col = [('MSTR_L_COL_ID','ML_RGN_KY'),'VALU_ID']
        lookup_key = 'HPSC_MSTR_L_KY'
        
        dest_col = 'MSTR_L_RGN_KY'
        mid_col = 'MSTR_L_RGN_KY'
        src_col = 'ML_RGN_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        LKUP_MAP(dest, lookup_table, [(6,lookup_col[0][1]),(7,self.get_value(src_col))], dest_col, producer, topic)
        
        lookup_col = [('MSTR_L_COL_ID','ML_FAIL_OVR_STAT_KY'),'VALU_ID']
        dest_col = 'MSTR_L_APP_FAILOVR_STAT_KY'
        mid_col = 'MSTR_L_FAILOVR_STAT_KY'
        src_col = 'ML_FAIL_OVR_STAT_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        LKUP_MAP(dest, lookup_table, [(6,lookup_col[0][1]),(7,self.get_value(src_col))], dest_col, producer, topic)
        
        lookup_col = [('MSTR_L_COL_ID','ML_ESCLT_TYPE_KY'),'VALU_ID']
        dest_col = 'MSTR_L_LAST_AVL_ESCLT_TYPE_KY'
        mid_col = 'MSTR_L_LAST_AVL_ESCLT_TYPE_KY'
        src_col = 'ML_LAST_AVL_ESCLT_TYPE_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        LKUP_MAP(dest, lookup_table, [(6,lookup_col[0][1]),(7,self.get_value(src_col))], dest_col, producer, topic)
        
        lookup_col = [('MSTR_L_COL_ID','ML_TMLNSS_TYPE_KY'),'VALU_ID']
        dest_col = 'MSTR_L_LAST_AVL_TMLNSS_TYPE_KY'
        mid_col = 'MSTR_L_LAST_AVL_TMLNSS_TYPE_KY'
        src_col = 'ML_LAST_AVL_TMLNSS_TYPE_KY'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col, mid_table, mid_col, src_table, src_col, lookup_table, lookup_col, lookup_key))
        LKUP_MAP(dest, lookup_table, [(6,lookup_col[0][1]),(7,self.get_value(src_col))], dest_col, producer, topic)
        
        #=========================================================================================================
        #ASSIGN
        #=========================================================================================================
        operation = 'ASSIGN'
        
        kafka_msg = operation + ' started for ' + src_table
        producer.send_messages(topic, kafka_msg)
        
        dest_col = 'APP_ESCLT_CT'
        producer.send_messages(topic, create_kafka_msg(operation, dest_table, dest_col))
        dest.__dict__[dest.get_key(dest_col)] = 1
                      
        kafka_msg = "table process finished successfully for HP_ESCLT_DVC_M1"
        producer.send_messages(topic, kafka_msg) 

class RLT_ESCLT_DVC_DTL_F(ITR2_Tables):
    def __init__(self):
        super(RLT_ESCLT_DVC_DTL_F, self).__init__()
        self.__ESCLT_DVC_ID = None                
        self.__ESCLT_DVC_D_KY = None
        self.__ESCLT_ID       = None               
        self.__ESCLT_MGMT_D_KY = None
        self.__APP_PRTFL_ID    = None              
        self.__APP_KY          = None              
        self.__CI_D_KY         = None              
        self.__IT_OWN_ASGN_GRP_ORG_HIER_KY = None  
        self.__SUPP_OWN_ASGN_GRP_ORG_HIER_KY = None
        self.__ASGN_GRP_KY                   = None
        self.__ESCLT_MGR_EMP_KY              = None
        self.__ESCLT_CRT_EMP_KY              = None
        self.__ESCLT_REQR_EMP_KY      = None       
        self.__NEXT_MTNG_DT_DAY_KY    = None       
        self.__INCID_DSCVR_DT_DAY_KY   = None      
        self.__ICC_ENGMT_DT_DAY_KY    = None       
        self.__ESCLT_REQ_DT_DAY_KY    = None       
        self.__ESCLT_OPEN_DT_DAY_KY     = None     
        self.__BUS_CNTNTY_RESTOR_DT_DAY_KY  = None 
        self.__ESCLT_CLOSE_DT_DAY_KY      = None   
        self.__ESCLT_CNCL_DT_DAY_KY     = None     
        self.__LAST_AVL_EVNT_STRT_DT_DAY_KY  = None         
        self.__LAST_AVL_EVNT_END_DT_DAY_KY   = None       
        self.__MSTR_L_ESCLT_STAT_KY     = None     
        self.__MSTR_L_ESCLT_TYPE_KY        = None  
        self.__MSTR_L_ESCLT_TMLNSS_TYPE_KY  = None 
        self.__MSTR_L_RGN_KY        = None         
        self.__MSTR_L_APP_FAILOVR_STAT_KY   = None 
        self.__MSTR_L_LAST_AVL_ESCLT_TYPE_KY = None
        self.__MSTR_L_LAST_AVL_TMLNSS_TYPE_KY = None
        self.__MSTR_L_DEV_ENGMT_KY   = None
        self.__MSTR_L_DEFN_WARR_PER_KY  = None
        self.__APP_ESCLT_CT        = None          
        self.__DATA_WHSE_ROW_ID     = None
        self.__SRC_SYS_KY           = None   
        self.__SRC_SYS_INSTNC_KY    = None
        self.__CRT_BY_JOB_ID      = None    
        self.__DATA_WHSE_CRT_TS     = None
        self.__UPD_BY_JOB_ID      = None     
        self.__DATA_WHSE_UPD_TS    = None
        self.__CRC_CHCKSM_TX      = None
        self.__LGCL_DEL_FG         = None
        self.attr_2_column_mapping = dict()
        self.attr_2_column_mapping["__ESCLT_DVC_ID"] = ("ESCLT_DVC_ID", "NUMBER")
        self.attr_2_column_mapping["__ESCLT_DVC_D_KY"] = ("ESCLT_DVC_D_KY", "NUMBER")
        self.attr_2_column_mapping["__ESCLT_ID"] = ("ESCLT_ID", "VARCHAR2(100)")
        self.attr_2_column_mapping["__ESCLT_MGMT_D_KY"] = ("ESCLT_MGMT_D_KY", "NUMBER")
        self.attr_2_column_mapping["__APP_PRTFL_ID"] = ("APP_PRTFL_ID", "NUMBER")
        self.attr_2_column_mapping["__APP_KY"] = ("APP_KY", "NUMBER")
        self.attr_2_column_mapping["__CI_D_KY"] = ("CI_D_KY", "NUMBER")
        self.attr_2_column_mapping["__IT_OWN_ASGN_GRP_ORG_HIER_KY"] = ("IT_OWN_ASGN_GRP_ORG_HIER_KY", "NUMBER")
        self.attr_2_column_mapping["__SUPP_OWN_ASGN_GRP_ORG_HIER_KY"] = ("SUPP_OWN_ASGN_GRP_ORG_HIER_KY", "NUMBER")
        self.attr_2_column_mapping["__ASGN_GRP_KY"] = ("ASGN_GRP_KY", "NUMBER")
        self.attr_2_column_mapping["__ESCLT_MGR_EMP_KY"] = ("ESCLT_MGR_EMP_KY", "NUMBER")
        self.attr_2_column_mapping["__ESCLT_CRT_EMP_KY"] = ("ESCLT_CRT_EMP_KY", "NUMBER")
        self.attr_2_column_mapping["__ESCLT_REQR_EMP_KY"] = ("ESCLT_REQR_EMP_KY", "NUMBER")
        
        #to_number(to_char())
        self.attr_2_column_mapping["__NEXT_MTNG_DT_DAY_KY"] = ("NEXT_MTNG_DT_DAY_KY", "DATE2NUMBER")
        self.attr_2_column_mapping["__INCID_DSCVR_DT_DAY_KY"] = ("INCID_DSCVR_DT_DAY_KY", "DATE2NUMBER")
        self.attr_2_column_mapping["__ICC_ENGMT_DT_DAY_KY"] = ("ICC_ENGMT_DT_DAY_KY", "DATE2NUMBER")
        self.attr_2_column_mapping["__ESCLT_REQ_DT_DAY_KY"] = ("ESCLT_REQ_DT_DAY_KY", "DATE2NUMBER")
        self.attr_2_column_mapping["__ESCLT_OPEN_DT_DAY_KY"] = ("ESCLT_OPEN_DT_DAY_KY", "DATE2NUMBER")
        self.attr_2_column_mapping["__BUS_CNTNTY_RESTOR_DT_DAY_KY"] = ("BUS_CNTNTY_RESTOR_DT_DAY_KY", "DATE2NUMBER")
        self.attr_2_column_mapping["__ESCLT_CLOSE_DT_DAY_KY"] = ("ESCLT_CLOSE_DT_DAY_KY", "DATE2NUMBER")
        self.attr_2_column_mapping["__ESCLT_CNCL_DT_DAY_KY"] = ("ESCLT_CNCL_DT_DAY_KY", "DATE2NUMBER")
        self.attr_2_column_mapping["__LAST_AVL_EVNT_STRT_DT_DAY_KY"] = ("LAST_AVL_EVNT_STRT_DT_DAY_KY", "DATE2NUMBER")
        self.attr_2_column_mapping["__LAST_AVL_EVNT_END_DT_DAY_KY"] = ("LAST_AVL_EVNT_END_DT_DAY_KY", "DATE2NUMBER")
        
        self.attr_2_column_mapping["__MSTR_L_ESCLT_STAT_KY"] = ("MSTR_L_ESCLT_STAT_KY", "NUMBER")
        self.attr_2_column_mapping["__MSTR_L_ESCLT_TYPE_KY"] = ("MSTR_L_ESCLT_TYPE_KY", "NUMBER")
        self.attr_2_column_mapping["__MSTR_L_ESCLT_TMLNSS_TYPE_KY"] = ("MSTR_L_ESCLT_TMLNSS_TYPE_KY", "NUMBER")
        self.attr_2_column_mapping["__MSTR_L_RGN_KY"] = ("MSTR_L_RGN_KY", "NUMBER")
        self.attr_2_column_mapping["__MSTR_L_APP_FAILOVR_STAT_KY"] = ("MSTR_L_APP_FAILOVR_STAT_KY", "NUMBER")
        self.attr_2_column_mapping["__MSTR_L_LAST_AVL_ESCLT_TYPE_KY"] = ("MSTR_L_LAST_AVL_ESCLT_TYPE_KY", "NUMBER")
        
        #mistake in excel
        self.attr_2_column_mapping["__MSTR_L_LAST_AVL_TMLNSS_TYPE_KY"] = ("MSTR_L_LAST_AVL_TMLNSS_TYPE_KY", "NUMBER")
        self.attr_2_column_mapping["__MSTR_L_DEV_ENGMT_KY"] = ("MSTR_L_DEV_ENGMT_KY", "NUMBER")
        self.attr_2_column_mapping["__MSTR_L_DEFN_WARR_PER_KY"] = ("MSTR_L_DEFN_WARR_PER_KY", "NUMBER")
        self.attr_2_column_mapping["__APP_ESCLT_CT"] = ("APP_ESCLT_CT", "NUMBER")
        self.attr_2_column_mapping["__DATA_WHSE_ROW_ID"] = ("DATA_WHSE_ROW_ID", "NUMBER")
        self.attr_2_column_mapping["__SRC_SYS_KY"] = ("SRC_SYS_KY", "NUMBER")
        self.attr_2_column_mapping["__SRC_SYS_INSTNC_KY"] = ("SRC_SYS_INSTNC_KY", "NUMBER")
        self.attr_2_column_mapping["__CRT_BY_JOB_ID"] = ("CRT_BY_JOB_ID", "NUMBER")
        self.attr_2_column_mapping["__DATA_WHSE_CRT_TS"] = ("DATA_WHSE_CRT_TS", "DATE")
        self.attr_2_column_mapping["__UPD_BY_JOB_ID"] = ("UPD_BY_JOB_ID", "NUMBER")
        self.attr_2_column_mapping["__DATA_WHSE_UPD_TS"] = ("DATA_WHSE_UPD_TS", "DATE")
        self.attr_2_column_mapping["__CRC_CHCKSM_TX"] = ("CRC_CHCKSM_TX", "VARCHAR2(32)")
        self.attr_2_column_mapping["__LGCL_DEL_FG"] = ("LGCL_DEL_FG", "CHAR(1)")
        
    def get_value(self, key):
        #return self.__dict__["_ITR2Tables_" + self.__class__.__name__ + "__" + key]
        return self.__dict__["_" + self.__class__.__name__ + "__" + key]

    def get_key(self, key):
        #key_mod = "_ITR2Tables_" + self.__class__.__name__ + "__" + key
        key_mod = "_" + self.__class__.__name__ + "__" + key
        return key_mod
    """
    def LKUP_map(self, dest, redis_table, input_key_tuple_list, output_key, producer, topic_test):
        r = RedisOp(['get',redis_table])
        input_dict = dict()
        #in case there are multiple conditions
        for item in input_key_tuple_list:
            key = item[0]
            value = item[1]
            if value == None:
                kafka_msg = "The input_key is None return -2"
                producer.send_messages(topic_test, kafka_msg)
                dest.__dict__[dest.get_key(output_key)] = -2
                return
            else:
                input_dict[key] = value
        
        dest.__dict__[dest.get_key(output_key)] = super(RLT_ESCLT_DVC_DTL_F, self).dstct(r[1],input_dict,[0])
        input_dict.clear()
        return
    """
    #def md5_check(self, producer, topic):
        
        
    def set_record(self, kwargs):
        self.__ESCLT_DVC_ID = kwargs[0]              
        self.__ESCLT_DVC_D_KY = kwargs[1]  
        self.__ESCLT_ID       = kwargs[2]               
        self.__ESCLT_MGMT_D_KY = kwargs[3]  
        self.__APP_PRTFL_ID    = kwargs[4]             
        self.__APP_KY          = kwargs[5]               
        self.__CI_D_KY         = kwargs[6]            
        self.__IT_OWN_ASGN_GRP_ORG_HIER_KY = kwargs[7]  
        self.__SUPP_OWN_ASGN_GRP_ORG_HIER_KY = kwargs[8]  
        self.__ASGN_GRP_KY                   = kwargs[9]  
        self.__ESCLT_MGR_EMP_KY              = kwargs[10]  
        self.__ESCLT_CRT_EMP_KY              = kwargs[11]  
        self.__ESCLT_REQR_EMP_KY      = kwargs[12]     
        self.__NEXT_MTNG_DT_DAY_KY    = kwargs[13]    
        self.__INCID_DSCVR_DT_DAY_KY   = kwargs[14]     
        self.__ICC_ENGMT_DT_DAY_KY    = kwargs[15]     
        self.__ESCLT_REQ_DT_DAY_KY    = kwargs[16]     
        self.__ESCLT_OPEN_DT_DAY_KY     = kwargs[17]  
        self.__BUS_CNTNTY_RESTOR_DT_DAY_KY  = kwargs[18]  
        self.__ESCLT_CLOSE_DT_DAY_KY      = kwargs[19]  
        self.__ESCLT_CNCL_DT_DAY_KY     = kwargs[20]      
        self.__LAST_AVL_EVNT_STRT_DT_DAY_KY  = kwargs[21]        
        self.__LAST_AVL_EVNT_END_DT_DAY_KY   = kwargs[22]      
        self.__MSTR_L_ESCLT_STAT_KY     = kwargs[23]      
        self.__MSTR_L_ESCLT_TYPE_KY        = kwargs[24]  
        self.__MSTR_L_ESCLT_TMLNSS_TYPE_KY  = kwargs[25]  
        self.__MSTR_L_RGN_KY        = kwargs[26]         
        self.__MSTR_L_APP_FAILOVR_STAT_KY   = kwargs[27]  
        self.__MSTR_L_LAST_AVL_ESCLT_TYPE_KY = kwargs[28]  
        self.__MSTR_L_LAST_AVL_TMLNSS_TYPE_KY = kwargs[29]  
        self.__MSTR_L_DEV_ENGMT_KY   = kwargs[30]  
        self.__MSTR_L_DEFN_WARR_PER_KY  = kwargs[31]  
        self.__APP_ESCLT_CT        = kwargs[32]         
        #self.__DATA_WHSE_ROW_ID     = kwargs[33]  
        #self.__SRC_SYS_KY           = kwargs[34]  
        #self.__SRC_SYS_INSTNC_KY    = kwargs[35]  
        #self.__CRT_BY_JOB_ID      = kwargs[36]  
        #self.__DATA_WHSE_CRT_TS     = kwargs[37]  
        #self.__UPD_BY_JOB_ID      = kwargs[38]  
        #self.__DATA_WHSE_UPD_TS    = kwargs[39]  
        #self.__CRC_CHCKSM_TX      = kwargs[40]  
        #self.__LGCL_DEL_FG         = kwargs[41]

    def get_record(self):
        #key_mod = "_ITR2Tables_" + self.__class__.__name__ + "__" + key
        kwargs = list()
        for key in self.__dict__.keys():
            kwargs.append(key)
            
        for key in self.__dict__.keys():
            if key.startswith("_" ):
                # Only get attributes starting with "__" in instance
                key_mod = key.replace("_" + self.__class__.__name__, "")
                if key_mod == '__ESCLT_DVC_ID':
                    kwargs[0] = self.__dict__[key]
                if key_mod == '__ESCLT_DVC_D_KY':
                    kwargs[1] = self.__dict__[key]
                if key_mod == '__ESCLT_ID':
                    kwargs[2] = self.__dict__[key]   
                if key_mod == '__ESCLT_MGMT_D_KY':
                    kwargs[3] = self.__dict__[key]
                if key_mod == '__APP_PRTFL_ID':
                    kwargs[4] = self.__dict__[key]
                if key_mod == '__APP_KY':
                    kwargs[5] = self.__dict__[key]
                if key_mod == '__CI_D_KY':
                    kwargs[6] = self.__dict__[key]
                if key_mod == '__IT_OWN_ASGN_GRP_ORG_HIER_KY':
                    kwargs[7] = self.__dict__[key]
                if key_mod == '__SUPP_OWN_ASGN_GRP_ORG_HIER_KY':
                    kwargs[8] = self.__dict__[key]
                if key_mod == '__ASGN_GRP_KY':
                    kwargs[9] = self.__dict__[key]
                if key_mod == '__ESCLT_MGR_EMP_KY':
                    kwargs[10] = self.__dict__[key]
                if key_mod == '__ESCLT_CRT_EMP_KY':
                    kwargs[11] = self.__dict__[key]
                if key_mod == '__ESCLT_REQR_EMP_KY':
                    kwargs[12] = self.__dict__[key]
                if key_mod == '__NEXT_MTNG_DT_DAY_KY':
                    kwargs[13] = self.__dict__[key]
                if key_mod == '__INCID_DSCVR_DT_DAY_KY':
                    kwargs[14] = self.__dict__[key]
                if key_mod == '__ICC_ENGMT_DT_DAY_KY':
                    kwargs[15] = self.__dict__[key]
                if key_mod == '__ESCLT_REQ_DT_DAY_KY':
                    kwargs[16] = self.__dict__[key]
                if key_mod == '__ESCLT_OPEN_DT_DAY_KY':
                    kwargs[17] = self.__dict__[key]
                if key_mod == '__BUS_CNTNTY_RESTOR_DT_DAY_KY':
                    kwargs[18] = self.__dict__[key]
                if key_mod == '__ESCLT_CLOSE_DT_DAY_KY':
                    kwargs[19] = self.__dict__[key]
                if key_mod == '__ESCLT_CNCL_DT_DAY_KY':
                    kwargs[20] = self.__dict__[key]
                if key_mod == '__LAST_AVL_EVNT_STRT_DT_DAY_KY':
                    kwargs[21] = self.__dict__[key]
                if key_mod == '__LAST_AVL_EVNT_END_DT_DAY_KY':
                    kwargs[22] = self.__dict__[key]
                if key_mod == '__MSTR_L_ESCLT_STAT_KY':
                    kwargs[23] = self.__dict__[key]
                if key_mod == '__MSTR_L_ESCLT_TYPE_KY':
                    kwargs[24] = self.__dict__[key]
                if key_mod == '__MSTR_L_ESCLT_TMLNSS_TYPE_KY':
                    kwargs[25] = self.__dict__[key]
                if key_mod == '__MSTR_L_RGN_KY':
                    kwargs[26] = self.__dict__[key]
                if key_mod == '__MSTR_L_APP_FAILOVR_STAT_KY':
                    kwargs[27] = self.__dict__[key]
                if key_mod == '__MSTR_L_LAST_AVL_ESCLT_TYPE_KY':
                    kwargs[28] = self.__dict__[key]
                if key_mod == '__MSTR_L_LAST_AVL_TMLNSS_TYPE_KY':
                    kwargs[29] = self.__dict__[key]
                if key_mod == '__MSTR_L_DEV_ENGMT_KY':
                    kwargs[30] = self.__dict__[key]
                if key_mod == '__MSTR_L_DEFN_WARR_PER_KY':
                    kwargs[31] = self.__dict__[key]
                if key_mod == '__APP_ESCLT_CT':
                    kwargs[32] = self.__dict__[key]
                #self.__DATA_WHSE_ROW_ID':
                    #kwargs[33] = self.__dict__[key]
                #self.__SRC_SYS_KY':
                    #kwargs[34] = self.__dict__[key]
                #self.__SRC_SYS_INSTNC_KY':
                    #kwargs[35] = self.__dict__[key]
                #self.__CRT_BY_JOB_ID':
                    #kwargs[36] = self.__dict__[key]
                #self.__DATA_WHSE_CRT_TS':
                    #kwargs[37] = self.__dict__[key]
                #self.__UPD_BY_JOB_ID':
                    #kwargs[38] = self.__dict__[key]
                #self.__DATA_WHSE_UPD_TS':
                    #kwargs[39] = self.__dict__[key]
                #self.__CRC_CHCKSM_TX':
                    #kwargs[40] = self.__dict__[key]
                #self.__LGCL_DEL_FG':
                    #kwargs[41] = self.__dict__[key]
                #self.__HP_S2S_RDY_FG':
                    #kwargs[42] = self.__dict__[key]
                #self.__HP_INSTNC_PRTFL_ID':
                    #kwargs[43] = self.__dict__[key]
        return kwargs

class TableFactory(object):
    def produce_table_object(self, table_name):
        """Return sensor object based on sensor type
            Args:
                sensor_type_name (str): validate type name of sensors like hdhld, accelerometer
        Returns:
            sensor object
        """
        if table_name == "HP_ESCLT_M1":
            return HP_ESCLT_M1()
        elif table_name == "HP_ESCLT_DVC_M1":
            return HP_ESCLT_DVC_M1()
        elif table_name == "RLT_ESCLT_DVC_DTL_F":
            return RLT_ESCLT_DVC_DTL_F()
        else:
            return None
            
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
                 
#@staticmethod
def process_data_HP_ESCLT_M1(iter):
    """Parse JSON data and write to its specific Cassandra Table and kairos database.
    Args:
        iter (iterator): records in Kafka topics
    """
    # Create kafka client
    topic_test="test"
    client = KafkaClient("c9t26359.itcs.hpecorp.net:9092")
    producer = SimpleProducer(client)
    producer.send_messages(topic_test, " ")
    producer.send_messages(topic_test, "One record received from HP_ESCLT_M1")


    src_m1 = table_object_dict['HP_ESCLT_M1']
    src_dvc_m1 = table_object_dict['HP_ESCLT_DVC_M1']        
    dest = table_object_dict['RLT_ESCLT_DVC_DTL_F']
    
    for record in iter:
        #record already mapped orginal is a tuple (None, content)
        #content is of format [(1,2,3)]
        #producer.send_messages(topic_test, str(type(record)))
        # type is unicode
        record_encode = record.encode('utf-8')
        
        # kafka str to python list
        record_list = eval(record_encode.replace('[','').replace(']',''))

        #record_str = "".join(str_transform(record_element) for record_element in record_list)
        
        # put record into redis
        #src_m1.set_record(record_list)
        # python list to redis str
        record_list_str = str(record_list)
        
        src_m1.set_record(record_list)
        device_str = str(src_m1.get_value('HP_ESCLT_ID'))
        
        RedisOp(['set','HP_ESCLT_M1_RECORD_' + device_str,record_list_str])
        
        kafka_msg = "HP_ESCLT_M1_RECORD_" + device_str + " is written to redis"
        producer.send_messages(topic_test, kafka_msg)
        # set receive flag
        RedisOp(['set','HP_ESCLT_M1_RECV_FLAG_' + device_str,'true'])
        #kafka_msg = "One record is done processing bravo"

        super(RLT_ESCLT_DVC_DTL_F, dest).init_process(src_m1, src_dvc_m1, dest, device_str, producer, topic_test)

#@staticmethod
def process_data_HP_ESCLT_DVC_M1(iter):
    """Parse JSON data and write to its specific Cassandra Table and kairos database.
    Args:
        iter (iterator): records in Kafka topics
    """
    topic_test="test"
    client = KafkaClient("c9t26359.itcs.hpecorp.net:9092")
    producer = SimpleProducer(client)
    producer.send_messages(topic_test, " ")
    producer.send_messages(topic_test, "One record received from HP_ESCLT_DVC_M1")
    
    src_m1 = table_object_dict['HP_ESCLT_M1']
    src_dvc_m1 = table_object_dict['HP_ESCLT_DVC_M1']
    dest = table_object_dict['RLT_ESCLT_DVC_DTL_F']

    for record in iter:
        record_encode = record.encode('utf-8')
        record_list = eval(record_encode.replace('[','').replace(']',''))

        record_list_str = str(record_list)
        
        src_dvc_m1.set_record(record_list)
        
        # modified for testing
        #src_dvc_m1.__dict__[src_dvc_m1.get_key('HP_ESCLT_ID')] = 'EM1025258'
        src_dvc_m1.__dict__[src_dvc_m1.get_key('HP_ESCLT_ID')] = 'EM1025373'
        
        device_str = str(src_dvc_m1.get_value('HP_ESCLT_ID'))
        
        RedisOp(['set','HP_ESCLT_DVC_M1_RECORD_' + device_str,record_list_str])
        
        kafka_msg = "HP_ESCLT_DVC_M1_RECORD_" + device_str + " is written to redis"
        producer.send_messages(topic_test, kafka_msg)
        
        RedisOp(['set','HP_ESCLT_DVC_M1_RECV_FLAG_' + device_str,'true'])
        #kafka_msg = "One record is done processing bravo"
        
        super(RLT_ESCLT_DVC_DTL_F, dest).init_process(src_m1, src_dvc_m1, dest, device_str, producer, topic_test)
        #dest.mds_check()
            #for stuff in record_list:
                #producer.send_messages(topic_test, str(stuff))
        #msg2test(producer, topic_test, str(list(record)))
        
def valid_data_HP_ESCLT_M1(rdd):
    """Do transformation and filter out RDDs from DStream before writing data to Cassandra or KariosDB,
    and return transformed and filtered RDD
    Args:
        rdd (rdd):
    Return:
        rdd
    """
    # Create cluster object and start session
    #logger.debug("    %s" %(get_elapsed_time(process_start_time, time.time(), "Writing data to Cassandra and Karios")))
    number_of_records_in_rdd = rdd.count()
    if number_of_records_in_rdd == 0:
        logger.debug("    NO data is coming from topics ")
        return
    else:
        logger.debug("    Start proccessing %d reocrds for each partition ...", number_of_records_in_rdd)
        rdd.foreachPartition(process_data_HP_ESCLT_M1)

def valid_data_HP_ESCLT_DVC_M1(rdd):
    """Do transformation and filter out RDDs from DStream before writing data to Cassandra or KariosDB,
    and return transformed and filtered RDD
    Args:
        rdd (rdd):
    Return:
        rdd
    """
    # Create cluster object and start session
    #logger.debug("    %s" %(get_elapsed_time(process_start_time, time.time(), "Writing data to Cassandra and Karios")))
    number_of_records_in_rdd = rdd.count()
    if number_of_records_in_rdd == 0:
        logger.debug("    NO data is coming from topics ")
        return
    else:
        logger.debug("    Start proccessing %d reocrds for each partition ...", number_of_records_in_rdd)
        rdd.foreachPartition(process_data_HP_ESCLT_DVC_M1)
        
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
    table_object_dict = dict()
    # create SparkContext and ssc spark instance
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, batch_duration)
    
    #con = connect_oracle()
    
    logger.info("==> Creating Spark DStream ...")

    table_object_dict['HP_ESCLT_M1'] = TableFactory().produce_table_object('HP_ESCLT_M1')
    table_object_dict['HP_ESCLT_DVC_M1'] = TableFactory().produce_table_object('HP_ESCLT_DVC_M1')
    table_object_dict['RLT_ESCLT_DVC_DTL_F'] = TableFactory().produce_table_object('RLT_ESCLT_DVC_DTL_F')

    topic_HP_ESCLT_M1 = 'RLT_ESCLT_DVC_DTL_F_HPESCLTM1'
    topic_HP_ESCLT_DVC_M1 = 'RLT_ESCLT_DVC_DTL_F_HPESCLTDVCM1'
    
    kafka_stream_HP_ESCLT_M1 = kafka2spark(ssc,topic_HP_ESCLT_M1)
    lines_HP_ESCLT_M1 = kafka_stream_HP_ESCLT_M1.map(lambda x: x[1])
    lines_HP_ESCLT_M1.foreachRDD(lambda rdd: valid_data_HP_ESCLT_M1(rdd))
    
    kafka_stream_HP_ESCLT_DVC_M1 = kafka2spark(ssc,topic_HP_ESCLT_DVC_M1)
    lines_HP_ESCLT_DVC_M1 = kafka_stream_HP_ESCLT_DVC_M1.map(lambda x: x[1])
    lines_HP_ESCLT_DVC_M1.foreachRDD(lambda rdd: valid_data_HP_ESCLT_DVC_M1(rdd))

    logger.info("Created kafka_stream successfully")
    #lines = kafka_stream.map(lambda x: x[1])
    #kafka_stream.pprint()
    # start spark instance
    ssc.start()
    ssc.awaitTermination()
