# -*- coding: UTF-8 -*-
# author: zhen-peng.yang@hpe.com
# date: 2016-12-27

# imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


# Module Constants
APP_NAME = "rz_gnrl_ldgr_acct_trsn_itm_fct"
spark = SparkSession.Builder() \
    .appName(APP_NAME) \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .master("local") \
    .getOrCreate()


# Closure Functions


# Main functionality
def main(spark: SparkSession):
    # sql declaration
    acct_trsn = "select * from radar.acct_trsn where src_sys_cd='1051041'"
    acct_trsn_itm = "select * from radar.acct_trsn_itm where src_sys_cd='1051041'"
    curr_dmnsn = "select curr_ky,lcl_curr_cd,trsn_curr_cd,grp_curr_cd from radar_rz.curr_dmnsn where src_sys_cd='1051041'"
    bsn_sb_sgm_dmnsn = "select bsn_sb_sgm_ky,bsn_sb_sgm_cd from radar_rz.bsn_sb_sgm_dmnsn where src_sys_cd='1051041'"
    gnrl_ldgr_acct_dmnsn = "select gnrl_ldgr_acct_ky,gnrl_ldgr_acct_id from radar_rz.gnrl_ldgr_acct_dmnsn where src_sys_cd='1051041'"
    gnrl_ldgr_acct_trsn_atr_dmnsn = ("""select acct_trsn_atr_ky,curr_exch_rate_typ_cd,acct_trsn_typ_cd,acct_trsn_bckpst_ind,acct_trsn_itm_etry_typ_cd,rvrsl_rsn_cd
                                              from radar_rz.gnrl_ldgr_acct_trsn_atr_dmnsn
                                             where src_sys_cd='1051041'""")
    lgl_ent_dmnsn = "select lgl_ent_ky,ctry_cd,entrs_lgl_ent_ldgr_id,entrs_lgl_ent_nr from radar_rz.lgl_ent_dmnsn where src_sys_cd='1051041'"
    fscl_prd = "select fscl_prd_id,fscl_prd_yr_nr,fscl_prd_sqn_nr,fscl_prd_yr_vrnt_txt from radar.fscl_prd where src_sys_cd='1051041'"
    acct_prd_dmnsn = "select acct_prd_ky,fscl_prd_yr_nr,fscl_prd_sqn_nr,fscl_prd_yr_vrnt_txt from radar_rz.acct_prd_dmnsn where src_sys_cd='1051041'"
    pft_cntr_pstg_obj_dmnsn = "select pft_cntr_pstg_obj_ky,pft_cntr_cd,pstg_obj_nn_cntrct_id,pstg_obj_cntrct_id from radar_rz.pft_cntr_pstg_obj_dmnsn where src_sys_cd='1051041'"

    # create dataframes
    df_at = spark.sql(acct_trsn)
    df_ati = spark.sql(acct_trsn_itm)
    df_cd = spark.sql(curr_dmnsn)
    df_bad = spark.sql(bsn_sb_sgm_dmnsn)
    df_gnrld = spark.sql(gnrl_ldgr_acct_dmnsn)
    df_atad = spark.sql(gnrl_ldgr_acct_trsn_atr_dmnsn)
    df_led = spark.sql(lgl_ent_dmnsn)
    df_fp = spark.sql(fscl_prd)
    df_apd = spark.sql(acct_prd_dmnsn)
    df_pcpo = spark.sql(pft_cntr_pstg_obj_dmnsn)

    # join conditions
    cond_cd = [f.coalesce(df_ati.acct_trsn_itm_lcl_curr_cd, f.lit("?")) == df_cd.LCL_CURR_CD,
               f.coalesce(df_ati.acct_trsn_itm_trsn_curr_cd, f.lit("?")) == df_cd.TRSN_CURR_CD,
               ]
    # filter for cd: grp_curr_cd = 'usd'

    cond_bad = [f.coalesce(df_ati.bsn_sb_sgm_cd, f.lit("?")) == df_bad.BSN_SB_SGM_CD]

    cond_gnrld = [f.coalesce(df_ati.gnrl_ldgr_acct_id, f.lit("?")) == df_gnrld.GNRL_LDGR_ACCT_ID]

    cond_atad = [f.coalesce(df_at.curr_exch_rate_typ_cd, f.lit("?")) == df_atad.CURR_EXCH_RATE_TYP_CD,
                 f.coalesce(df_at.acct_trsn_typ_cd, f.lit("?")) == df_atad.ACCT_TRSN_TYP_CD,
                 f.when(df_at.acct_trsn_bckpst_ind == 'x', 'y').otherwise('n') == df_atad.ACCT_TRSN_BCKPST_IND,
                 f.when(df_ati.acct_trsn_itm_etry_typ_cd == 'h', 'cr').when(df_ati.acct_trsn_itm_etry_typ_cd == 's',
                                                                            'dr').otherwise(
                     f.coalesce(df_ati.acct_trsn_itm_etry_typ_cd, f.lit("?"))) == df_atad.ACCT_TRSN_ITM_ETRY_TYP_CD,
                 f.coalesce(df_at.rvrsl_rsn_cd, f.lit("?")) == df_atad.RVRSL_RSN_CD
                 ]

    cond_led = [f.coalesce(df_ati.ctry_cd, f.lit("?")) == df_led.CTRY_CD,
                df_ati.lgl_ent_ldgr_id == df_led.ENTRS_LGL_ENT_LDGR_ID,
                f.coalesce(df_ati.entrs_lgl_ent_nr, f.lit("?")) == df_led.ENTRS_LGL_ENT_NR
                ]

    cond_fp = [df_at.fscl_prd_id == f.coalesce(df_fp.FSCL_PRD_ID, f.lit("?"))]

    cond_apd = [df_apd.FSCL_PRD_YR_NR == df_fp.FSCL_PRD_YR_NR,
                df_apd.FSCL_PRD_SQN_NR == df_fp.FSCL_PRD_SQN_NR,
                df_apd.FSCL_PRD_YR_VRNT_TXT == df_fp.FSCL_PRD_YR_VRNT_TXT]

    cond_pcpo = [f.coalesce(df_ati.pft_cntr_cd, f.lit("?")) == df_pcpo.PFT_CNTR_CD,
                 f.coalesce(df_ati.pstg_obj_nn_cntrct_id, f.lit("?")) == df_pcpo.PSTG_OBJ_NN_CNTRCT_ID,
                 f.coalesce(df_ati.pstg_obj_cntrct_id, f.lit("?")) == df_pcpo.PSTG_OBJ_CNTRCT_ID
                 ]

    # joins
    # at inner join ati
    df_master = df_at.join(df_ati, df_at.acct_trsn_id == df_ati.acct_trsn_id, "inner")
    joined_df = df_master.join(df_cd, cond_cd, 'left_outer').where("GRP_CURR_CD = 'usd'") \
        .join(df_bad, cond_bad, 'left_outer') \
        .join(df_gnrld, cond_gnrld, 'left_outer') \
        .join(df_atad, cond_atad, 'left_outer') \
        .join(df_led, cond_led, 'left_outer') \
        .join(df_fp, cond_fp, 'left_outer') \
        .join(df_apd, cond_apd, 'left_outer') \
        .join(df_pcpo, cond_pcpo, 'left_outer')

    # please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions.
    df_gnrl_ldgr_acct_trsn_itm_fct = joined_df.select(
        f.coalesce(df_apd.ACCT_PRD_KY, f.lit(-1051041)).alias("acct_prd_ky"),
        f.coalesce(df_atad.ACCT_TRSN_ATR_KY, f.lit(-1051041)).alias("acct_trsn_atr_ky"),
        f.coalesce(df_bad.BSN_SB_SGM_KY, f.lit(-1051041)).alias("bsn_sb_sgm_ky"),
        f.coalesce(df_cd.CURR_KY, f.lit(-1051041)).alias("curr_ky"),
        f.coalesce(df_gnrld.GNRL_LDGR_ACCT_KY, f.lit(-1051041)).alias("gnrl_ldgr_acct_ky"),
        f.coalesce(df_led.LGL_ENT_KY, f.lit(-1051041)).alias("lgl_ent_ky"),
        f.coalesce(df_pcpo.PFT_CNTR_PSTG_OBJ_KY, f.lit(-1051041)).alias("pft_cntr_pstg_obj_ky"),
        df_at.acct_trsn_id,
        df_ati.acct_trsn_itm_sqn_nr,
        f.when(df_ati.acct_trsn_itm_etry_typ_cd == "H", "CR").when(df_ati.acct_trsn_itm_etry_typ_cd == "S", "DR")
            .alias("acct_trsn_itm_etry_typ_cd"),
        f.lit("1051041"),
        df_at.acct_trsn_btch_id,
        df_at.acct_trsn_orgnn_ts,
        df_at.acct_trsn_etry_ts,
        df_at.acct_trsn_pstg_ts,
        df_at.acct_trsn_trnsltn_ts,
        df_ati.acct_trsn_itm_gnrl_ldgr_eff_dt,
        df_ati.acct_trsn_itm_cmt,
        df_at.acct_trsn_nm,
        df_at.acct_trsn_dn,
        f.when((f.coalesce(df_ati.acct_trsn_itm_trsn_to_grp_curr_exch_rate, f.lit(0)) == 0).__and__
               (df_ati.acct_trsn_itm_trsn_curr_cd == "USD"), 1).otherwise(
            df_ati.acct_trsn_itm_trsn_to_grp_curr_exch_rate)
            .alias("acct_trsn_itm_trsn_to_grp_curr_exch_rate"),
        f.when((f.coalesce(df_ati.acct_trsn_itm_trsn_to_lcl_curr_exch_rate, f.lit(0)) == 0).__and__
               (df_ati.acct_trsn_itm_trsn_curr_cd == df_ati.acct_trsn_itm_lcl_curr_cd), 1).otherwise(
            df_ati.acct_trsn_itm_trsn_to_lcl_curr_exch_rate)
            .alias("acct_trsn_itm_trsn_to_lcl_curr_exch_rate"),
        f.when((f.coalesce(df_ati.acct_trsn_itm_lcl_to_grp_curr_exch_rate, f.lit(0)) == 0).__and__
               (df_ati.acct_trsn_itm_lcl_curr_cd == "USD"), 1).otherwise(
            df_ati.acct_trsn_itm_lcl_to_grp_curr_exch_rate)
            .alias("acct_trsn_itm_lcl_to_grp_curr_exch_rate"),
        df_ati.acct_trsn_itm_grp_curr_amt,
        f.when((f.coalesce(df_ati.acct_trsn_itm_grp_curr_amt, f.lit(0)) == 0).__and__
               (f.coalesce(df_ati.acct_trsn_itm_lcl_curr_amt, f.lit(0)) == 0).__and__
               (f.coalesce(df_ati.acct_trsn_itm_trsn_curr_amt, f.lit(0)) == 0),
               f.round(df_ati.acct_trsn_itm_trsn_curr_amt * df_ati.acct_trsn_itm_trsn_to_grp_curr_exch_rate,6))
            .when((f.coalesce(df_ati.acct_trsn_itm_grp_curr_amt, f.lit(0)) == 0).__and__
                          (f.coalesce(df_ati.acct_trsn_itm_lcl_curr_amt, f.lit(0)) != 0),
                  f.round(df_ati.acct_trsn_itm_lcl_curr_amt * df_ati.acct_trsn_itm_lcl_to_grp_curr_exch_rate,6)),
        df_ati.acct_trsn_itm_trsn_curr_amt,
        df_ati.acct_trsn_itm_lcl_curr_amt,
        f.when((f.coalesce(df_ati.acct_trsn_itm_lcl_curr_amt, f.lit(0)) == 0).__and__
               (f.coalesce(df_ati.acct_trsn_itm_trsn_curr_amt, f.lit(0) == 0)),
               f.round(df_ati.acct_trsn_itm_trsn_curr_amt * df_ati.acct_trsn_itm_trsn_to_lcl_curr_exch_rate, 6))
            .alias("acct_trsn_itm_drvd_lcl_curr_amt"),
        f.current_timestamp().alias("radar_upd_ts"),
        f.lit("rz_gnrl_ldgr_acct_trsn_itm_fct").alias("radar_upd_by_prs_id"),
        f.lit("N").alias("radar_dlt_ind"),
        f.lit("1051041")
    )

    # insert
    df_gnrl_ldgr_acct_trsn_itm_fct.write.mode("overwrite").insertInto("radar_rz.gnrl_ldgr_acct_trsn_itm_fct", overwrite=True)

    # test Data
    spark.sql("select * from radar_rz.gnrl_ldgr_acct_trsn_itm_fct limit 10").show()


if __name__ == '__main__':
    main(spark)
