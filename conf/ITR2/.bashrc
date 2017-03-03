# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

# User specific aliases and functions
# github home
export GIT_HOME="/home/${USER}/NewTechPOC"

# redis home configuration
export REDIS_HOME="/opt/mount1/app/redis-3.2"

# Home sweet home
export ITR2_HOME="/opt/mount1/app"

# config java
export JAVA_HOME="/opt/mount1/app/jdk1.8.0_72"

# added by Anaconda2 4.2.0 installer
#export HIVE_HOME="/opt/mount1/app/apache-hive-1.2.1-bin"
export HIVE_HOME="/opt/mount1/app/apache-hive-2.1.1-bin"

# python configuration
export PYTHON_HOME="/opt/mount1/app/anaconda2"
# scala configuration
export SCALA_HOME="/opt/mount1/app/scala-2.11.0"
 
# cx_Oracle configuration
export ORACLE_VERSION="12.1"
export ORACLE_HOME="/usr/lib/oracle/$ORACLE_VERSION/client64"
export PATH="$PATH:$ORACLE_HOME/bin"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$ORACLE_HOME/lib"
export TNS_ADMIN="$ORACLE_HOME/network/admin"
 
# hadoop configuration
export HADOOP_PREFIX="/opt/mount1/app/hadoop-2.7.3"
export HADOOP_HOME=$HADOOP_PREFIX
export HADOOP_COMMON_HOME=$HADOOP_PREFIX
export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
export HADOOP_HDFS_HOME=$HADOOP_PREFIX
export HADOOP_MAPRED_HOME=$HADOOP_PREFIX
export HADOOP_YARN_HOME=$HADOOP_PREFIX

# spark configuration
#export SPARK_HOME="/opt/mount1/app/spark-2.0.2-bin-hadoop2.7"
#export SPARK_HOME="/opt/mount1/app/spark-1.6.1-bin-hadoop2.6"
export SPARK_HOME="/opt/mount1/app/spark-2.1.0-bin-hadoop2.7"
export SPARK_CONF_DIR=$SPARK_HOME/conf

# zookeeper configuration
export ZK_HOME="/opt/mount1/app/zookeeper-3.4.9"

# kafka configuration
export KAFKA_HOME="/opt/mount1/app/kafka_2.11-0.10.1.0"

# maven configuration
export MVN_HOME="/opt/mount1/app/apache-maven-3.3.9"
 
# export path
export PATH=${JAVA_HOME}/bin:$PATH
export PATH=${HADOOP_PREFIX}/bin:$PATH
export PATH=${HADOOP_PREFIX}/sbin:$PATH
export PATH=${HIVE_HOME}/bin:$PATH
export PATH=${ZK_HOME}/bin:$PATH
export PATH=${KAFKA_HOME}/bin:$PATH
export PATH=${PYTHON_HOME}/bin:$PATH
export PATH=${SPARK_HOME}/bin:$PATH
export PATH=${SPARK_HOME}/sbin:$PATH
export PATH=${SCALA_HOME}/bin:$PATH
export PATH=${MVN_HOME}/bin:$PATH
