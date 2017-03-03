# -*- coding: UTF-8 -*-
# author: zhen-peng.yang@hpe.com
# date: 2016-12-27
# sample calls: [hadoop@hadoopmaster rz]$ spark-submit --master yarn --jars /opt/apache-hive-1.2.1-bin/lib/hive-contrib-1.2.1.jar rz_gnrl_ldgr_acct_trsn_atr_dmnsn.py

# imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types


# Module Constants
APP_NAME = "rz_gnrl_ldgr_acct_trsn_atr_dmnsn"
spark = SparkSession.Builder() \
    .appName(APP_NAME) \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("hive.exec.reducers.max", "1") \
    .config("spark.sql.crossJoin.enabled", "true") \
    .enableHiveSupport() \
    .master("yarn") \
    .getOrCreate()


# Closure Functions


# Main functionality
def main(spark):

    # sql declaration
    acct_trsn = "select acct_trsn_id,curr_exch_rate_typ_cd,acct_trsn_typ_cd,acct_trsn_bckpst_ind," \
                "rvrsl_rsn_cd from radar.acct_trsn where src_sys_cd='1051041'"
    acct_trsn_itm = "select acct_trsn_id, acct_trsn_itm_etry_typ_cd from radar.acct_trsn_itm where src_sys_cd='1051041'"
    rvrsl_rsn = "select rvrsl_rsn_cd,rvrsl_rsn_nm,rvrsl_rsn_dn from radar.rvrsl_rsn where src_sys_cd='1051041'"
    curr_exch_rate_typ = "select curr_exch_rate_typ_cd,curr_exch_rate_typ_nm,curr_exch_rate_typ_dn" \
                         " from radar.curr_exch_rate_typ where src_sys_cd='1051041'"
    acct_trsn_typ = "select acct_trsn_typ_cd,acct_trsn_typ_nm,acct_trsn_typ_dn" \
                    " from radar.acct_trsn_typ where src_sys_cd='1051041'"
    acct_trsn_itm_etry_typ = "select acct_trsn_itm_etry_typ_cd,acct_trsn_itm_etry_typ_nm,acct_trsn_itm_etry_typ_dn" \
                             " from radar.acct_trsn_itm_etry_typ where src_sys_cd='1051041'"

    # create dataframes
    df_at = spark.sql(acct_trsn)
    df_ati = spark.sql(acct_trsn_itm)
    df_rr = spark.sql(rvrsl_rsn)
    df_cert = spark.sql(curr_exch_rate_typ)
    df_att = spark.sql(acct_trsn_typ)
    df_atiet = spark.sql(acct_trsn_itm_etry_typ)
    df_ss = spark.sql("select * from radar.src_sys")

    # join conditions
    cond_rr = [df_at.rvrsl_rsn_cd  == df_rr.rvrsl_rsn_cd]
    cond_cert = [df_at.curr_exch_rate_typ_cd == df_cert.curr_exch_rate_typ_cd]
    cond_att = [df_at.acct_trsn_typ_cd == df_att.acct_trsn_typ_cd]
    cond_atiet = [df_ati.acct_trsn_itm_etry_typ_cd == df_atiet.acct_trsn_itm_etry_typ_cd]

    # joins
    # at inner join ati
    df_master = df_at.join(df_ati, df_at.acct_trsn_id == df_ati.acct_trsn_id, "inner")
    joined_df = df_master.join(df_rr, cond_rr, 'left_outer') \
        .join(df_cert, cond_cert, 'left_outer') \
        .join(df_att, cond_att, 'left_outer') \
        .join(df_atiet, cond_atiet, 'left_outer') \
        .join(df_ss, df_ss.src_sys_cd == "1051041", 'left_outer')

    spark.sql("create temporary function gnrl_ldgr_acct_trsn_atr_dmnsn_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence'")

    # please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions.
    df_gnrl_ldgr_acct_trsn_atr_dmnsn = joined_df.select(
        f.nanvl(df_at.curr_exch_rate_typ_cd, f.lit("?")).alias("curr_exch_rate_typ_cd"),
        f.coalesce(df_cert.curr_exch_rate_typ_nm, df_cert.curr_exch_rate_typ_dn, f.lit("NO VALUE")).alias(
            "curr_exch_rate_typ_nm"),
        f.coalesce(df_cert.curr_exch_rate_typ_dn, df_cert.curr_exch_rate_typ_nm, f.lit("NO VALUE")).alias(
            "curr_exch_rate_typ_dn"),
        f.nanvl(df_at.acct_trsn_typ_cd, f.lit("?")).alias("acct_trsn_typ_cd"),
        f.coalesce(df_att.acct_trsn_typ_nm, df_att.acct_trsn_typ_dn, f.lit("NO VALUE")).alias("acct_trsn_typ_nm"),
        f.coalesce(df_att.acct_trsn_typ_dn, df_att.acct_trsn_typ_nm, f.lit("NO VALUE")).alias("acct_trsn_typ_dn"),
        f.nanvl(f.when(df_ati.acct_trsn_itm_etry_typ_cd == "S", f.lit("DR"))
                .when(df_ati.acct_trsn_itm_etry_typ_cd == "H", f.lit("CR"))
                .otherwise(df_ati.acct_trsn_itm_etry_typ_cd), f.lit("NO VALUE"))
            .alias("acct_trsn_itm_etry_typ_cd"),
        f.coalesce(df_atiet.acct_trsn_itm_etry_typ_nm, df_atiet.acct_trsn_itm_etry_typ_dn, f.lit("NO VALUE"))
            .alias("acct_trsn_itm_etry_typ_nm"),
        f.nanvl(f.when(df_at.acct_trsn_bckpst_ind == "X", f.lit("Y")).otherwise(f.lit("N")), f.lit("?")).alias("acct_trsn_bckpst_ind"),
        f.when(f.nanvl(df_at.rvrsl_rsn_cd, f.lit("")) == "", f.lit("?")).otherwise(df_at.rvrsl_rsn_cd).alias("rvrsl_rsn_cd"),
        f.coalesce(df_rr.rvrsl_rsn_nm, df_rr.rvrsl_rsn_dn, f.lit("NO VALUE")).alias("rvrsl_rsn_nm"),
        f.coalesce(df_rr.rvrsl_rsn_dn, df_rr.rvrsl_rsn_nm, f.lit("NO VALUE")).alias("rvrsl_rsn_dn"),
        f.lit("1051041"),
        f.nanvl(df_ss.src_sys_nm, f.lit("NO VALUE")).alias("src_sys_nm"),
        f.current_timestamp(),
        f.lit('rz_gnrl_ldgr_acct_trsn_atr_dmnsn.hql'),
        f.lit('N'),
        f.lit("1051041")
    )

    df_hardcode = spark.sql("select -1051041, '?', 'NO VALUE', 'NO VALUE', '?', 'NO VALUE', 'NO VALUE', '?', "
                            "'NO VALUE', 'N', '?', 'NO VALUE', 'NO VALUE', '1051041', ss.src_sys_nm, "
                            "from_unixtime(unix_timestamp(), 'yyyy-mm-dd hh:mm:ss'), "
                            "'rz_gnrl_ldgr_acct_trsn_atr_dmnsn.hql', 'N', '1051041' from radar.src_sys ss "
                            "where ss.src_sys_cd=1051041")

    df_gnrl_ldgr_acct_trsn_atr_dmnsn.createOrReplaceTempView("tmp")
    spark.sql("select cast( rpad('1051041',18,'0') as bigint )+gnrl_ldgr_acct_trsn_atr_dmnsn_sequence() as acct_trsn_atr_ky, "
              "tmp.* from tmp").unionAll(df_hardcode).drop_duplicates()\
        .createOrReplaceTempView("gnrl_ldgr_acct_trsn_atr_dmnsn")
    
    # insert data
    spark.sql("insert overwrite table radar_rz.gnrl_ldgr_acct_trsn_atr_dmnsn partition (src_sys_cd) select * from gnrl_ldgr_acct_trsn_atr_dmnsn")
    # test Data
    spark.sql("select * from radar_rz.gnrl_ldgr_acct_trsn_atr_dmnsn limit 10").show()


if __name__ == '__main__':
    main(spark)
