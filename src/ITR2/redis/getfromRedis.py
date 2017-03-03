import redis_hash
from redis_hash import RedisOp
from filter_ import dstct

"""
operation = ["get",TABLENAME]
    acording to  tablename finding out corresponding datas
return listA
inpstr = ["val1","val2"....]
    then find one data by filter with inpstr
return listB
outstr = [n]
    find col n in the data
    return listC 
"""

operation = ["get","ESCLT_ACTN_D"]
inpstr = []
outstr = []

r = RedisOp(operation)
print r
#res = dstct(r[1],inpstr,outstr)
#print res
