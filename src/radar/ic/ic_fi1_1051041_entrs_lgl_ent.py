# Script Name: ic_fi1_1051041_entrs_lgl_ent.py
# Description: Used to load data into IC table entrs_lgl_ent.
# Created By: Bobby
# Created Date: 12/28/2016
# Comments:Initial version
# Changed Date:
# Changed By:
# Changed Date:



from pyspark.sql.types import *
from pyspark.sql import SparkSession




APP_NAME = "entrs_lgl_ent"

spark = SparkSession \
    .builder \
    .appName(APP_NAME) \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.sql.crossJoin.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()
    
spark.sql("set hive.auto.convert.join=false;")


df = spark.sql("SELECT \
'1051041' AS P_SRC_SYS_CD, \
SRC.CTRY_CD, \
SRC.ENTRS_LGL_ENT_NR, \
SRC.ENTRS_LGL_ENT_BSN_ROL_CD, \
SRC.ENTRS_LGL_ENT_NM, \
SRC.ENTRS_LGL_ENT_DN, \
SRC.CHRT_OF_DPRC_CD, \
SRC.ENTRS_LGL_ENT_LVL_NR, \
SRC.ENTRS_LGL_ENT_CO_IND, \
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS RADAR_UPD_TS, \
'ic_fi1_1051041_entrs_lgl_ent.hql' AS RADAR_UPD_BY_PRS_ID, \
'N' AS RADAR_DLT_IND, \
'1051041' AS SRC_SYS_CD \
FROM \
(SELECT  \
DISTINCT \
T001.LAND1 AS CTRY_CD, \
T001.BUKRS AS ENTRS_LGL_ENT_NR, \
T001.BUTXT AS ENTRS_LGL_ENT_NM, \
NULL AS ENTRS_LGL_ENT_DN, \
'2' AS ENTRS_LGL_ENT_LVL_NR, \
'Y' AS ENTRS_LGL_ENT_CO_IND, \
'COMPANY' AS ENTRS_LGL_ENT_BSN_ROL_CD, \
NVL(T093C.AFAPL,'99') AS CHRT_OF_DPRC_CD \
FROM 200836_az_fi1_1051041.T001 T001 \
LEFT JOIN 200836_az_fi1_1051041.T093C T093C ON T093C.BUKRS = T001.BUKRS \
WHERE T001.BUKRS <> 'IN10' AND T001.LAND1 IS NOT NULL AND TRIM(T001.LAND1)<>'' \
UNION ALL \
SELECT \
DISTINCT \
ADDR.CTRY_CD AS CTRY_CD, \
T001W.WERKS AS ENTRS_LGL_ENT_NR, \
T001W.NAME1 AS ENTRS_LGL_ENT_NM, \
T001W.NAME2 AS ENTRS_LGL_ENT_DN, \
NULL AS ENTRS_LGL_ENT_LVL_NR, \
'N' AS ENTRS_LGL_ENT_CO_IND, \
'PLANT' AS ENTRS_LGL_ENT_BSN_ROL_CD, \
'99' AS CHRT_OF_DPRC_CD \
FROM  200836_az_fi1_1051041.T001W \
INNER JOIN radar.ADDR ON ADDR.SRC_SYS_ADDR_ID = T001W.ADRNR AND ADDR.SRC_SYS_CD='1051041' \
UNION ALL \
SELECT \
'US' AS CTRY_CD, \
'999' AS ENTRS_LGL_ENT_NR, \
'SAP FI1' AS ENTRS_LGL_ENT_NM, \
NULL AS ENTRS_LGL_ENT_DN, \
'2' AS ENTRS_LGL_ENT_LVL_NR, \
'Y' AS ENTRS_LGL_ENT_CO_IND, \
'COMPANY' AS ENTRS_LGL_ENT_BSN_ROL_CD, \
'99' AS CHRT_OF_DPRC_CD) src")


df.write.mode('overwrite').insertInto("radar.entrs_lgl_ent",overwrite=True)
