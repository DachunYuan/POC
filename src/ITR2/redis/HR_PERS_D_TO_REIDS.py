# -*- coding: utf-8 -*-
"""
Created on Fri Mar  3 15:42:56 2017

@author: yinlin
"""

import cx_Oracle
from redis_hash import RedisOp
#from oracle_to_redis import OracleToRedis

def oracleOp(info):
    dsn = cx_Oracle.makedsn(info["host"],info["port"],info["sid"])
    con = cx_Oracle.connect(info["username"],info["passward"],dsn)
    cursor = con.cursor()
    res=cursor.execute(info["sql"])
    rs = res.fetchall()
    #print rs
    cursor.close()
    con.close()
    return rs

sql="SELECT COUNT(*) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY EMAIL_ADDR_NM ORDER BY EMP_STAT_IND, EMP_KY DESC) AS ROW_RANK,HR_PERS_D.EMAIL_ADDR_NM AS EMAIL_ADDR_NM, \
EMP_TYPE_TX AS EMP_TYPE_TX,FULL_NM AS FULL_NM,SRC_SYS_KY AS SRC_SYS_KY,SRC_SYS_INSTNC_KY AS SRC_SYS_INSTNC_KY,EMP_STAT_IND AS EMP_STAT_IND,BUS_UNIT_NM AS BUS_UNIT_NM, \
BUS_ORG_CD AS BUS_ORG_CD,ORG_CHART_GRP_CD AS ORG_CHART_GRP_CD,BUS_SCTR_CD AS BUS_SCTR_CD,EMP_KY FROM ITR23.HR_PERS_D) WHERE ROW_RANK=1"

info={"sql":sql,"username":"INFR","passward":"4.!INFRitr2DEVpw_20160504",
"host":"g1u2115.austin.hp.com","port":"1525","sid":"ITRD"
}

line_num = oracleOp(info)

for index in range(line_num):
    line_no = index+1	
     
    sql_key="SELECT EMAIL_ADDR_NM AS EMAIL_ADDR_NM \
    FROM (SELECT ROW_NUMBER() OVER (PARTITION BY EMAIL_ADDR_NM ORDER BY EMP_STAT_IND, EMP_KY DESC) AS ROW_RANK, \
    HR_PERS_D.EMAIL_ADDR_NM AS EMAIL_ADDR_NM,EMP_TYPE_TX AS EMP_TYPE_TX,FULL_NM AS FULL_NM,SRC_SYS_KY AS SRC_SYS_KY,SRC_SYS_INSTNC_KY AS SRC_SYS_INSTNC_KY, \
    EMP_STAT_IND AS EMP_STAT_IND,BUS_UNIT_NM AS BUS_UNIT_NM,BUS_ORG_CD AS BUS_ORG_CD,ORG_CHART_GRP_CD AS ORG_CHART_GRP_CD,BUS_SCTR_CD AS BUS_SCTR_CD, EMP_KY \
    FROM ITR23.HR_PERS_D) WHERE ROW_RANK=1 AND ROWNUM = " + line_no
                                                          
    info={"sql":sql_key,"username":"INFR","passward":"4.!INFRitr2DEVpw_20160504","host":"g1u2115.austin.hp.com","port":"1525","sid":"ITRD"}

    redis_key = 'HR_PERS_D.' + str(oracleOp(info))

    sql_value="SELECT EMP_KY AS EMP_KY,EMAIL_ADDR_NM AS EMAIL_ADDR_NM,EMP_TYPE_TX AS EMP_TYPE_TX,FULL_NM AS FULL_NM,SRC_SYS_KY AS SRC_SYS_KY, SRC_SYS_INSTNC_KY AS SRC_SYS_INSTNC_KY, \
    EMP_STAT_IND AS EMP_STAT_IND,BUS_UNIT_NM AS BUS_UNIT_NM,BUS_ORG_CD AS BUS_ORG_CD,ORG_CHART_GRP_CD AS ORG_CHART_GRP_CD, BUS_SCTR_CD AS BUS_SCTR_CD \
    FROM (SELECT ROW_NUMBER() OVER (PARTITION BY EMAIL_ADDR_NM ORDER BY EMP_STAT_IND, EMP_KY DESC) AS ROW_RANK, \
    HR_PERS_D.EMAIL_ADDR_NM AS EMAIL_ADDR_NM,EMP_TYPE_TX AS EMP_TYPE_TX,FULL_NM AS FULL_NM,SRC_SYS_KY AS SRC_SYS_KY,SRC_SYS_INSTNC_KY AS SRC_SYS_INSTNC_KY, \
    EMP_STAT_IND AS EMP_STAT_IND,BUS_UNIT_NM AS BUS_UNIT_NM,BUS_ORG_CD AS BUS_ORG_CD,ORG_CHART_GRP_CD AS ORG_CHART_GRP_CD,BUS_SCTR_CD AS BUS_SCTR_CD, EMP_KY \
    FROM ITR23.HR_PERS_D) WHERE ROW_RANK=1 AND ROWNUM = " + line_no
    
    info={"sql":sql_value,"username":"INFR","passward":"4.!INFRitr2DEVpw_20160504","host":"g1u2115.austin.hp.com","port":"1525","sid":"ITRD"}

    redis_value = oracleOp(info)

    RedisOp(['set',redis_key,redis_value])
