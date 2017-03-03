# -*- coding: UTF-8 -*-
# author: zhen-peng.yang@hpe.com
# date: 2016-12-27

# imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark import StorageLevel
from pyspark.sql import types


# Module Constants
APP_NAME = __file__
spark = SparkSession.Builder() \
    .appName(APP_NAME) \
    .config("hive.support.concurrency", "false") \
    .config("spark.sql.crossJoin.enabled", "true") \
    .enableHiveSupport() \
    .master("yarn") \
    .getOrCreate()


# Closure Functions


# Main functionality
def main(spark):

    # sql declaration
    bsad = "select bukrs,belnr,gjahr,buzei, blart,rebzg,cpudt,budat, shkzg,sgtxt,kunnr,prctr, kostl,waers,dmbtr, " \
           "monat, wrbtr,dmbe2 from 200836_az_fi1_1051041.bsad"
    bsid = "select bukrs,belnr,gjahr,buzei, blart,rebzg,cpudt,budat, shkzg,sgtxt,kunnr,prctr, kostl,waers,dmbtr, " \
           "monat, wrbtr,dmbe2 from 200836_az_fi1_1051041.bsid"
    bseg = "select * from 200836_az_fi1_1051041.bseg_nw"
    knb1 = "select bukrs, akont, kunnr from 200836_az_fi1_1051041.knb1 WHERE knb1.AKONT is NOT NULL and knb1.akont<>''"
    tka02 = "select bukrs, kokrs from 200836_az_fi1_1051041.tka02 where tka02.GSBER is null or trim(tka02.GSBER)=''"
    lgl_ent_ldgr = "select lgl_ent_ldgr_id, lgl_ent_ldgr_cd from radar.lgl_ent_ldgr where src_sys_cd='1051041'"
    fscl_prd = "select fscl_prd_id, fscl_prd_yr_nr, fscl_prd_sqn_nr, fscl_prd_yr_vrnt_txt from radar.fscl_prd" \
               " where src_sys_cd='1051041' and fscl_prd_typ_cd='Month'"
    vbrk = "select vbeln, fkart from 200836_az_fi1_1051041.vbrk"
    t001 = "select * from 200836_az_fi1_1051041.t001"

    # create dataframes
    df_bsad = spark.sql(bsad).persist(StorageLevel(True, True, False, False, 1))
    df_bsid = spark.sql(bsid).persist(StorageLevel(True, True, False, False, 1))
    df_bseg = spark.sql(bseg)
    df_knb1 = spark.sql(knb1)
    df_tka02 = spark.sql(tka02)
    df_lgl_ent_ldgr = spark.sql(lgl_ent_ldgr)
    df_fscl_prd = spark.sql(fscl_prd)
    df_vbrk = spark.sql(vbrk)
    df_t001 = spark.sql(t001)

    # temporary table
    df_bsad_bsid = df_bsad.select(df_bsad.bukrs, df_bsad.belnr, df_bsad.gjahr, df_bsad.buzei)\
        .unionAll(df_bsid.select(df_bsid.bukrs, df_bsid.belnr, df_bsid.gjahr, df_bsid.buzei))
    df_stg_bsad_bsid_bseg = df_bsad_bsid.join(
        df_bseg,[df_bsad_bsid.bukrs == df_bseg.bukrs, df_bsad_bsid.belnr == df_bseg.belnr,
                 df_bsad_bsid.gjahr == df_bseg.gjahr, df_bsad_bsid.buzei == df_bseg.buzei], 'inner')\
        .select(df_bseg.menge, df_bseg.bukrs, df_bseg.belnr, df_bseg.gjahr, df_bseg.buzei)

    sql_rate = "select from_curr, to_curr, budat, curr_from_dt, CURR_DRVD_EXCH_RATE from ( select stg.from_curr, " \
               "stg.to_curr, budat, stg.curr_from_dt, stg.CURR_DRVD_EXCH_RATE, " \
               "row_number() over(partition by stg.from_curr,stg.to_curr,budat order by stg.curr_from_dt desc) as rn" \
               " from ( select distinct bsad.budat from ( select budat from 200836_az_fi1_1051041.bsad" \
               " union all select budat from 200836_az_fi1_1051041.bsid )bsad )src" \
               "inner join 200836_az_fi1_1051041.stg_tcurf_tcurr stg where budat>=stg.curr_from_dt" \
               ")src1 where rn=1"
    df_rate = spark.sql(sql_rate).persist(StorageLevel(True, True, False, False, 1))

    # joins
    # at inner join ati
    df_master = df_bsad.select("bukrs", "belnr", "gjahr", "buzei", "blart", "rebzg", "cpudt", "budat", "shkzg", "sgtxt",
                               "kunnr", "prctr", "kostl", "waers", "dmbtr", "monat", "wrbtr", "dmbe2")\
        .unionAll(df_bsad.select("bukrs", "belnr", "gjahr", "buzei", "blart", "rebzg", "cpudt", "budat", "shkzg",
                                 "sgtxt", "kunnr", "prctr", "kostl", "waers", "dmbtr", "monat", "wrbtr", "dmbe2"))
    joined_df = df_master.join(df_t001, df_master.bukrs == df_t001.bukrs, 'inner') \
        .join(df_vbrk, df_vbrk.vbeln == df_master.rebzg, 'left_outer') \
        .join(df_knb1, [df_master.bukrs == df_knb1.bukrs, df_master.kunnr == df_knb1.kunnr], 'left_outer') \
        .join(df_fscl_prd, [df_master.gjahr == df_fscl_prd.fscl_prd_yr_nr,
                            df_master.monat == df_fscl_prd.fscl_prd_sqn_nr,
                            df_fscl_prd.fscl_prd_yr_vrnt_txt == df_t001.periv], 'left_outer') \
        .join(df_lgl_ent_ldgr, df_lgl_ent_ldgr.lgl_ent_ldgr_cd == df_t001.ktopl, 'left_outer') \
        .join(df_tka02, df_master.bukrs == df_tka02.bukrs, 'left_outer') \
        .join(df_stg_bsad_bsid_bseg, [df_stg_bsad_bsid_bseg.bukrs == df_master.bukrs,
                                      df_stg_bsad_bsid_bseg.belnr == df_master.belnr,
                                      df_stg_bsad_bsid_bseg.gjahr == df_master.gjahr,
                                      df_stg_bsad_bsid_bseg.buzei == df_master.buzei], 'left_outer') \
        .join(df_rate, [df_rate.from_curr == df_master.waers,
                        df_rate.to_curr == 'USD',
                        df_rate.budat == df_master.budat], 'left_outer').alias("stg1") \
        .join(df_rate, [df_rate.from_curr == df_master.waers,
                        df_rate.to_curr == df_t001.waers,
                        df_rate.budat == df_master.budat], 'left_outer').alias("stg2") \
        .filter("blart not in ('DZ', 'ZP', 'CT', 'CZ', 'DZ', 'FZ', 'PA', 'ZP', 'ZU')")

    # please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions.
    df_sb_ldgr_actv = joined_df.select(
        f.lit("1051041").alias("p_src_sys_cd"),
        f.concat(df_master.bukrs, df_master.belnr, df_master.gjahr.cast("string"), df_master.buzei.cast("string"))
            .alias("sb_ldgr_actv_dcmt_nr"),
        f.lit("AR").alias("sb_ldgr_acct_typ_cd"),
        df_master.blart.alias("sb_ldgr_actv_typ_cd"),
        df_master.rebzg.alias("sb_ldgr_actv_inv_dcmt_nr"),
        f.when(df_master.rebzg != '', df_vbrk.fkart).alias("sb_ldgr_actv_inv_dcmt_typ_dn"),
        f.lit(None).alias("sb_ldgr_actv_csh_dcmt_nr"),
        f.lit(None).alias("sb_ldgr_actv_csh_dcmt_typ_dn"),
        f.concat(df_knb1.bukrs, f.lit('---'), df_knb1.akont, f.lit('---')).alias("sb_ldgr_actv_gnrl_ldgr_acct_cd"),
        df_master.cpudt.alias("sb_ldgr_actv_crt_ts"),
        df_master.cpudt.alias("sb_ldgr_actv_etry_ts"),
        df_master.budat.alias("sb_ldgr_actv_to_be_pstg_ts"),
        df_master.shkzg.alias("acct_trsn_itm_etry_typ_cd"),
        df_master.sgtxt.alias("sb_ldgr_actv_cmt"),
        f.concat(df_master.bukrs, df_master.belnr, df_master.gjahr.cast("string")).alias("sb_ldgr_actv_trsn_btch_id"),
        df_t001.land1.alias("ctry_cd"),
        df_master.bukrs.alias("entrs_lgl_ent_nr"),
        df_lgl_ent_ldgr.lgl_ent_ldgr_id.alias("lgl_ent_ldgr_id"),
        df_master.kunnr.alias("sb_ldgr_acct_id"),
        df_fscl_prd.fscl_prd_id.alias("fscl_prd_id"),
        f.when((f.nanvl(df_tka02.kokrs, f.lit("")) == "").__or__(f.nanvl(df_master.prctr, f.lit("")) == ""), f.lit(None))
            .otherwise(f.concat(df_tka02.kokrs, df_master.prctr)).alias("pft_cntr_cd"),
        f.when((f.nanvl(df_tka02.kokrs, f.lit("")) == "").__or__(f.nanvl(df_master.kostl, f.lit("")) == ""), f.lit(None))
            .otherwise(f.concat(df_tka02.kokrs, df_master.kostl)).alias("pstg_obj_nn_cntrct_id"),
        f.lit(None).alias("pstg_obj_cntrct_id"),
        f.lit(None).alias("prt_id"),
        f.lit(None).alias("prod_id"),
        df_stg_bsad_bsid_bseg.menge.alias("sb_ldgr_actv_qty"),
        f.lit('M').alias("curr_exch_rate_typ_cd"),
        f.lit(None).alias("sb_ldgr_actv_lcl_to_grp_curr_exch_rate"),
        f.when(df_master.waers == 'USD', f.lit(1.0)).otherwise("stg1.CURR_DRVD_EXCH_RATE")
            .alias("sb_ldgr_actv_trsn_to_grp_curr_exch_rate"),
        f.when(df_master.waers == 'USD', f.lit(1.0)).otherwise("stg2.CURR_DRVD_EXCH_RATE")
            .alias("sb_ldgr_actv_trsn_to_lcl_curr_exch_rate"),
        df_t001.waers.alias("sb_ldgr_actv_lcl_curr_cd"),
        df_master.waers.alias("sb_ldgr_actv_trsn_curr_cd"),
        df_master.dmbtr.alias("sb_ldgr_actv_lcl_curr_amt"),
        df_master.wrbtr.alias("sb_ldgr_actv_trsn_curr_amt"),
        df_master.dmbe2.alias("sb_ldgr_actv_grp_curr_amt"),
        f.current_timestamp(),
        f.lit('ic_fi1_1051041_sb_ldgr_actv.hql'),
        f.lit('N').alias("radar_dlt_ind"),
        f.lit(None).alias("sb_ldgr_actv_gnrl_ldgr_acct_typ_dn"),
        f.lit("COMPANY").alias("entrs_lgl_ent_bsn_rol_cd"),
        f.lit("1051041").alias("src_sys_cd")
    )
    
    # insert
    df_sb_ldgr_actv.write.mode("overwrite").insertInto("radar.sb_ldgr_actv", overwrite=True)

    # test data
    spark.sql("select * from radar.sb_ldgr_actv where src_sys_cd=1051041 and sb_ldgr_acct_typ_cd='AR'").take(10)


if __name__ == '__main__':
    main(spark)

