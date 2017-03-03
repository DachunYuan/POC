#!/bin/bash

# make sure $SPARK_HOME is set so spark-submit can be found
# because we are using yarn it must be --master yarn
# there are two deploy-modes (cluster,client)
# cluster means the job will be cast to run on other nodes (which means on that node you need a copy of all files you need) 
# client means the job will run on the node you submitted (and again means on this node you need to have all files you need)




# --executor-memory 2G --driver-memory 512M --total-executor-cores 1 are mandatory and recommened options
# --jars upload the lib to hdfs which is needed by spark make sure $SPARK_HOME is set
# at last it is the path to your code make sure $ITR2_HOME is set

#spark-submit --master yarn --deploy-mode client ../src/ITR2/sparkToOracle/rt_itr2_using_jdbc_3.py read INFR.RT_HPESCLTA3 

spark-submit --master yarn --deploy-mode cluster ../src/ITR2/sparkToOracle/rt_itr2_using_jdbc_3.py read INFR.RT_HPESCLTA3
