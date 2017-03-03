from redis_hash import RedisOp
from oracle_to_redis import OracleToRedis

sql="SELECT EMP_KY AS EMP_KY,EMAIL_ADDR_NM AS EMAIL_ADDR_NM,EMP_TYPE_TX AS EMP_TYPE_TX,FULL_NM AS FULL_NM,SRC_SYS_KY AS SRC_SYS_KY,SRC_SYS_INSTNC_KY AS SRC_SYS_INSTNC_KY,EMP_STAT_IND AS EMP_STAT_IND,BUS_UNIT_NM AS BUS_UNIT_NM,BUS_ORG_CD AS BUS_ORG_CD,ORG_CHART_GRP_CD AS ORG_CHART_GRP_CD,BUS_SCTR_CD AS BUS_SCTR_CD FROM (SELECT ROW_NUMBER() OVER (PARTITION BY EMAIL_ADDR_NM ORDER BY EMP_STAT_IND, EMP_KY DESC) AS ROW_RANK,HR_PERS_D.EMAIL_ADDR_NM AS EMAIL_ADDR_NM,EMP_TYPE_TX AS EMP_TYPE_TX,FULL_NM AS FULL_NM,SRC_SYS_KY AS SRC_SYS_KY,SRC_SYS_INSTNC_KY AS SRC_SYS_INSTNC_KY,EMP_STAT_IND AS EMP_STAT_IND,BUS_UNIT_NM AS BUS_UNIT_NM,BUS_ORG_CD AS BUS_ORG_CD,ORG_CHART_GRP_CD AS ORG_CHART_GRP_CD,BUS_SCTR_CD AS BUS_SCTR_CD,EMP_KY FROM ITR23.HR_PERS_D) WHERE ROWNUM<=100000 AND ROW_RANK=1"

info={"sql":sql,"username":"INFR","passward":"4.!INFRitr2DEVpw_20160504",
"host":"g1u2115.austin.hp.com","port":"1525","sid":"ITRD"
}

info_re = ["set","HR_PERS_D_1"]

OracleToRedis(info,info_re)