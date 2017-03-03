#!/bin/bash

# make sure $SPARK_HOME is set so spark-submit can be found
# because we are using yarn it must be --master yarn
# there are two deploy-modes (cluster,client)
# cluster means the job will be cast to run on other nodes (which means on that node you need a copy of all files you need) 
# client means the job will run on the node you submitted (and again means on this node you need to have all files you need)
# --executor-memory 2G --driver-memory 512M --total-executor-cores 1 are mandatory and recommened options
# --jars upload the lib to hdfs which is needed by spark make sure $SPARK_HOME is set
# at last it is the path to your code make sure $ITR2_HOME is set

#spark-submit --master yarn --deploy-mode cluster --executor-memory 2G --driver-memory 512M --total-executor-cores 1 --py-files $ITR2_HOME/kafka2spark.py \
#--jars $SPARK_HOME/lib/spark-streaming-kafka-assembly_2.11-1.6.3.jar $ITR2_HOME/Kafka_Spark_Mod.py

#spark-submit --master yarn --deploy-mode client --executor-memory 2G --driver-memory 512M --total-executor-cores 1 \
#--py-files $ITR2_HOME/kafka2spark.py,$ITR2_HOME/ITR2_config.ini \
#--jars $SPARK_HOME/lib/spark-streaming-kafka-assembly_2.11-1.6.3.jar \
#$ITR2_HOME/Kafka_Spark_Mod.py

spark-submit --master yarn --deploy-mode cluster --executor-memory 512M --driver-memory 512M --total-executor-cores 1 \
--py-files /home/wuhua/NewTechPOC/lib/ITR2/kafka2spark.py,/home/wuhua/NewTechPOC/conf/ITR2/ITR2_config.ini,/home/wuhua/NewTechPOC/src/ITR2/redis/redis_hash.py,/home/wuhua/NewTechPOC/src/ITR2/generateSql/generateSql.py,/home/wuhua/NewTechPOC/src/ITR2/generateSql/RLT_ESCLT_ACTN_D.conf  \
--jars $SPARK_HOME/lib/spark-streaming-kafka-assembly_2.11-1.6.3.jar \
/home/wuhua/NewTechPOC/src/ITR2/test.py
#>>>>>>> Stashed changes

# no spark streaming jars and no module error
#spark-submit --master yarn --deploy-mode cluster --executor-memory 2G --driver-memory 512M --total-executor-cores 1 $ITR2_HOME/Kafka_Spark.py

# can't get containerID
#spark-submit --master yarn --deploy-mode cluster --executor-memory 2G --driver-memory 512M --total-executor-cores 1 --py-files $ITR2_HOME/kafka2spark.py $ITR2_HOME/Kafka_Spark_Mod.py

# not working
#spark-submit --master yarn --deploy-mode cluster --executor-memory 2G --driver-memory 512M --total-executor-cores 1 $ITR2_HOME/Kafka_Spark_Mod.py --py-files $ITR2_HOME/kafka2spark.py
