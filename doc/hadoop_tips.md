
#Hadoop Tips:

##1.
###Diagnostics: java.io.IOException: No space left on device 
###No logs available for container container_1484291937537_0020_02_000001 
###1/1 local-dirs are bad: /tmp/hadoop-hadoop/nm-local-dir;
if you have the above error please delete error-logs in /tmp

>sudo rm -rf /tmp/hadoop-hadoop

use the command below to check whether you have successfully deleted the tmp files

>df -h

##2.
###Task stops at Application report for application_1484730110349_0008 (state: ACCEPTED)
###Kill previously running jobs, consult spark web UI for application id

>yarn application -kill application_1484730110349_0008

##3.
###Caused by: org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.AccessControlException): Permission denied: user=jizho, access=WRITE, inode="/user/jizho/.sparkStaging/application_1485314444128_0008":hadoop:supergroup:drwxr-xr-x 
This is because you don't have a folder in hdfs create one and chown to you
>bash hdfs dfs -mkdir /user/yinlin

>bash hdfs dfs -chown yinlin:supergroup /user/yinlin

##Common Hadoop Command Help
go to $HADOOP_PREFIX/bin

list directory
>bash hdfs dfs -ls /user

make directory
>bash hdfs dfs -mkdir /user/yinlin

copy a file from /home/yinlin to hdfs /user/yinlin
>bash hdfs dfs -copyFromLocal /home/yinlin/XXX.py hdfs:///user/yinlin

copy a file from hdfs to your local unix directory
>bash hdfs dfs -copyToLocal hdfs:///user/yinlin /home/yinlin/XXX.py 

all other commands are just like what it is under unix with the prefix

in hdfs `cd` is not allowed there is no notion of directory in hdfs


