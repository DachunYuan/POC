# -*- coding: UTF-8 -*-
# @author: Yang, zhen-peng (Arvin)

## Spark Application - execute with spark-submit：spark-submit app.py

# Imports
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import types
from pyspark.sql import functions as func


# Module Constants
# note: Builder() and getOrCreate(), don't miss ()
APP_NAME = "Spark SQL Demo"
spark = SparkSession.Builder() \
    .appName(APP_NAME) \
    .master("local") \
    .getOrCreate()


# Closure Functions


# Main functionality
def main(spark: SparkSession):

    sc = spark.sparkContext
    # load data from textFile, return text file RDD
    t005t = sc.textFile("C:\\200836_az_fi1_105104.T005T_p1.TXT") \
        .map(lambda line: line.split("|")) \
        .map(lambda t: Row(mandt=t[0], spras=t[1], land1=t[2], landx=t[3], natio=t[4]))
    # create DataFrame by RDD
    t005t_df = spark.createDataFrame(t005t)
    # register DataFrame as temp view
    t005t_df.createOrReplaceTempView("t005t")
    t005t_df.printSchema()
    # output:
    # root
    # | -- land1: string(nullable=true)
    # | -- landx: string(nullable=true)
    # | -- mandt: string(nullable=true)
    # | -- natio: string(nullable=true)
    # | -- spars: string(nullable=true)

    # 1. select statement
    t005t_df.select("mandt").show(1)
    # +-----+
    # | mandt |
    # +-----+
    # | "100" |
    # +-----+
    # 2. distinct
    print(t005t_df.select(t005t_df.mandt).distinct().count())
    # 1
    # 3. dropDuplicates, not planned to execute
    # t005t_df.dropDuplicates().show()
    # 4. filter (aka where)
    t005t_df.filter("spras = 'E'").show(1)
    t005t_df.where("spras = 'E'").show(1)
    # 5. groupBy
    t005t_df.groupBy(t005t_df.mandt, t005t_df.spras).agg({'land1': 'max'}).show(2)
    # +-----+-----+----------+
    # | mandt | spras | max(land1) |
    # +-----+-----+----------+
    # | "100" | "M" | "ZW" |
    # | "100" | "I" | "AU" |
    # +-----+-----+----------+
    # 6. join (default ‘inner’. One of inner, outer, left_outer, right_outer, leftsemi), not planned to execute
    # >>> cond = [df.name == df3.name, df.age == df3.age]
    # >>> df.join(df3, cond, 'outer').select(df.name, df3.age).collect()
    # [Row(name=u'Alice', age=2), Row(name=u'Bob', age=5)]
    # 7. orderBy, not planned to execute
    # >>> df.orderBy(df.age.desc()).collect()
    # [Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]

    # >>> df.orderBy(desc("age"), "name").collect()
    # [Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]

    # >>> df.orderBy(["age", "name"], ascending=[0, 1]).collect()
    # [Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
    # 8. alias
    # t005t_df.select('mandt').alias('t005t_mandt').show(1) - error
    t005t_df.select(t005t_df.mandt.alias('too5t_mandt')).show(1)
    # +-----------+
    # | too5t_mandt |
    # +-----------+
    # | "100" |
    # +-----------+
    # 9. cast
    t005t_df.select(t005t_df.mandt.cast(types.StringType()).alias("int_mandt")).show(1)
    # +---------+
    # | int_mandt |
    # +---------+
    # | "100" |
    # +---------+
    # 10. otherwise, should be '==' not '='
    t005t_df.select(t005t_df.mandt, func.when(t005t_df.mandt == 100, 'A').otherwise('B')).show(1)
    # +-----+-----------------------------------------+
    # | mandt | CASE WHEN(mandt=100) THEN A ELSE B END |
    # +-----+-----------------------------------------+
    # | "100" | B |
    # +-----+-----------------------------------------+
    # 11. substr, not planned to execute
    # >>> df.select(df.name.substr(1, 3).alias("col")).collect()
    # [Row(col=u'Ali'), Row(col=u'Bob')]
    # 12. when, not planned to execute
    # >>> from pyspark.sql import functions as F
    # >>> df.select(df.name, F.when(df.age > 4, 1).when(df.age < 3, -1).otherwise(0)).show()
    # +-----+------------------------------------------------------------+
    # | name | CASE WHEN(age > 4) THEN 1 WHEN(age < 3) THEN - 1 ELSE 0 END |
    # +-----+------------------------------------------------------------+
    # | Alice | -1 |
    # | Bob | 1 |
    # +-----+------------------------------------------------------------+
    # 13. window, not planned to execute
    # >>> from pyspark.sql import Window
    # >>> window = Window.partitionBy("name").orderBy("age").rowsBetween(-1, 1)
    # 14. insert data
    # df.write.mode("overwrite").insertInto("radar.addr", overwrite=True)

    # 15. toDF
    # >>> df.toDF('f1', 'f2').collect()
    # [Row(f1=2, f2=u'Alice'), Row(f1=5, f2=u'Bob')]
    # >>> from pyspark.sql import Row
    # >>> df = sc.parallelize([ \
    #     ...     Row(name='Alice', age=5, height=80), \
    #     ...     Row(name='Alice', age=5, height=80), \
    #     ...     Row(name='Alice', age=10, height=80)]).toDF()
    # df.select(concat_ws('-', df.s, df.d).alias('s')).collect()
    # useful for hardcoded data
    df_acct_trsn_itm_etry_typ = sc.parallelize([ \
        Row(P_SRC_SYS_CD='1051041', ACCT_TRSN_ITM_ETRY_TYP_CD='S', ACCT_TRSN_ITM_ETRY_TYP_NM='Debit',
            ACCT_TRSN_ITM_ETRY_TYP_DN='', RADAR_UPD_BY_PRS_ID='ic_fi1_1051041_acct_trsn_itm_etry_typ',
            RADAR_DLT_IND='N'), \
        Row(P_SRC_SYS_CD='1051041', ACCT_TRSN_ITM_ETRY_TYP_CD='H', ACCT_TRSN_ITM_ETRY_TYP_NM='Credit',
            ACCT_TRSN_ITM_ETRY_TYP_DN='', RADAR_UPD_BY_PRS_ID='ic_fi1_1051041_acct_trsn_itm_etry_typ',
            RADAR_DLT_IND='N'), \
        Row(P_SRC_SYS_CD='1051041', ACCT_TRSN_ITM_ETRY_TYP_CD='DR', ACCT_TRSN_ITM_ETRY_TYP_NM='Debit',
            ACCT_TRSN_ITM_ETRY_TYP_DN='', RADAR_UPD_BY_PRS_ID='ic_fi1_1051041_acct_trsn_itm_etry_typ',
            RADAR_DLT_IND='N'), \
        Row(P_SRC_SYS_CD='1051041', ACCT_TRSN_ITM_ETRY_TYP_CD='CR', ACCT_TRSN_ITM_ETRY_TYP_NM='Credit',
            ACCT_TRSN_ITM_ETRY_TYP_DN='', RADAR_UPD_BY_PRS_ID='ic_fi1_1051041_acct_trsn_itm_etry_typ',
            RADAR_DLT_IND='N')]).toDF()
    df_acct_trsn_itm_etry_typ.select('P_SRC_SYS_CD', 'ACCT_TRSN_ITM_ETRY_TYP_CD', 'ACCT_TRSN_ITM_ETRY_TYP_NM',
                                     'ACCT_TRSN_ITM_ETRY_TYP_DN', 'RADAR_UPD_BY_PRS_ID', 'RADAR_DLT_IND',
                                     func.current_timestamp()).write.mode('owerwrite') \
        .insertinto('radar.acct_trsn_itm_etry_typ',overwrite=True)


if __name__ == '__main__':
    main(spark)
