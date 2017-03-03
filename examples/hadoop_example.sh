#!/bin/bash

# use the client in the jar to run the `date` command
$HADOOP_PREFIX/bin/hadoop jar $HADOOP_PREFIX/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-2.7.3.jar \
org.apache.hadoop.yarn.applications.distributedshell.Client \
--jar $HADOOP_PREFIX/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-2.7.3.jar \
--shell_command date --num_containers 2 --master_memory 3072
