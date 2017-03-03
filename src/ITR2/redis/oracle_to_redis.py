import cx_Oracle
from redis_hash import RedisOp


sql = "select * from VERSION"
username = "INFR"
passward = "4.!INFRitr2DEVpw_20160504"
host = "g1u2115.austin.hp.com"
port = "1525"
sid = "ITRD"
info_or = {"sql":"select sessiontimezone from dual","username":"INFR","passward":"4.!INFRitr2DEVpw_20160504",
        "host":"g1u2115.austin.hp.com","port":"1525","sid":"ITRD"        
    }
info_re = ["set","test1"]

def oracleOp(info):
    dsn = cx_Oracle.makedsn(info["host"],info["port"],info["sid"])
    con = cx_Oracle.connect(info["username"],info["passward"],dsn)
    cursor = con.cursor()
    res=cursor.execute(info["sql"])
    rs = res.fetchall()
    print rs
    return rs

def redisOp(op_list,value):
    operation = []
    operation.append(op_list[0])
    operation.append(op_list[1])
    operation.append(value)
    rs = RedisOp(operation)
    if rs[0]:
        re = []
        re.append(True)
        return re
    else:
        re = []
        re.append(False)
        re.append(rs[1])
        return re

def OracleToRedis(info,op_list):
    rs = oracleOp(info)
    if rs:
        result = redisOp(op_list,rs) 
        if result[0]:
            return result
        else:
            return result

#               Oracle to redis api
# examples of the first argument,it is the oracle information:
#       info_or = {"sql":"select * from VERSION","username":"INFR","passward":"4.!INFRitr2DEVpw_20160504",
#              "host":"g1u2115.austin.hp.com","port":"1525","sid":"ITRD"}
#
# examples of the second argument,the redis requires the argument of the operation.
#       info_re = ["set","test1"]  
#
#return a list,first index is a bool,if the result is error,the reason is in the second index.
#rs = OracleToRedis(info_or,info_re)
#print rs
oracleOp(info_or)
