#!/bin/bash
spark-submit --master yarn --deploy-mode client --driver-memory 512m --executor-memory 1g --executor-cores 1 /opt/app/NewTechPOC/examples/pysparksql_example.py -s radar -t curr -l 1051041

# If you need to include any class like Hive UDF function, you might specify --jars option
# In below example, we have to add jar to use Hive UDF function
spark-submit --master yarn --deploy-mode client --jars /opt/app/NewTechPOC/lib/hive-contrib-1.2.1.jar --driver-memory 512m --executor-memory 1g --executor-cores 1 /opt/app/NewTechPOC/examples/pysparksql_example.py -s radar -t curr -l 1051041
