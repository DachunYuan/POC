# -*- coding: UTF-8 -*-
# Date: 2017-1-10 10:08:41
# Author: zhen-peng.yang@hpe.com (Arvin)


# imports
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
# test rate UDF
from UDF_rate_ratio import rate


def func(f, t, dt):
    rt = rate("stg_tcurf_tcurr.txt")
    return rt.evaluate(f, t, dt)


def main():
    spark = SparkSession.Builder() \
    .appName("APP_NAME") \
    .config("hive.support.concurrency", "false") \
    .config("spark.sql.crossJoin.enabled", "true") \
    .enableHiveSupport() \
    .master("yarn") \
    .getOrCreate()
    sc = spark.sparkContext


    spark.udf.register("func", func)
    spark.sql("select cast(func('EUR', 'USD', '2004-07-01') as double) as rate,src_sys_nm from radar.src_sys").show()


if __name__ == '__main__':
    main()

