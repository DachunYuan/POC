# ITR2 Zone

---
<a name="Table of Contents"></a>
### Table of Contents

* <a href="#Goal">Goal</a>
* <a href="#Database Connection">Database Connection</a>
  * <a href="#Source Database">Source Database</a>
  * <a href="#Destination Database">Destination Database</a>
* <a href="#ITR2 Lab Environment">RADAR Lab Environment</a>
  * <a href="#Cluster Information">Cluster Information</a>
    * <a href="#New Node Installation">New Node Isntallation</a>
  * <a href="#Service Web UIs">Service Web UIs</a>
  * <a href="#Version Information">Version Information</a>
  * <a href="#Environment Variables">Environment Variables</a>
    * <a href="#Python">Python</a>
    * <a href="#Hadoop">Hadoop</a>
    * <a href="#Hive">Hive</a>
    * <a href="#Kafka">Kafka</a>
    * <a href="#Spark">Spark</a>
    * <a href="#Redis">Redis</a>
    * <a href="#Zookeeper">Zookeeper</a>
  * <a href="#Configuration File">Configuration File</a>
  * <a href="#Shared User Account">Shared User Account</a>
* <a href="#Command Help & Tips">Command Help & TIPS</a>
* <a href="#Cluster Status Check">Cluster Status Check</a>
* <a href="#Github Tutorial">Github Tutorial</a>
* <a href="#Environment Configuration">Environment Configuration</a>
* <a href="#Packages">Packages</a>

---

<a name=Goal></a>                                               


### Goal
Faster Real-time data refresh!

<a name="Database Connection"></a>
###Database Connection
Download toad data point from file://15.107.21.4/Tools/TOAD/

the key and site message are in the doc

<a name="Source Database"></a>
####Source Database
Item|Info
----|----
Address1|gcu12022.austin.hp.com:1526
Address2|gcu21401.austin.hp.com:1526
Database|ITSMP
Username|HPSM_ODS_RO
Password|oprpt!Usr894Feb@ITSMP

<a name="Destination Database"></a>
####Destination Database
Item|Info
----|----
Address|g1u2115.austin.hp.com:1525
Database|ITRD
Username|INFR
Password|4.!INFRitr2DEVpw_20160504

<a name="ITR2 Lab Environment"></a>
###ITR2 Lab Environment

<a name="Cluster Information"></a>
###Cluster Information
|Server name		|Memory (GB)|Disk Space (GB)*|Server IP		|OS|
|----------------|-----------|---------------|---------------|---------|
|c9t26359.itcs.hpecorp.net	|8			|2			|16.250.37.241	|RedHat 6.8|
|c9t26360.itcs.hpecorp.net	|8			|2			|16.250.45.52	|RedHat 6.8|
|c9t26361.itcs.hpecorp.net	|8			|2			|16.250.45.90	|RedHat 6.8|
|c9t26318.itcs.hpecorp.net	|8			|2			|16.250.1.189	|RedHat 6.8|
*Disk Space for individual user (i.e. /home/yinlin)

*Disk Space for $ITR2_HOME (/opt/mount1) is 148GB

<a name="New Node Installation"></a>
### New Node Installation
To install a new node, copy all the needed packages and config files, then  
For Hadoop:  
change etc/hadoop/slaves (add IP address)   

For Spark:  
change conf/spark-env.sh (change SPARK_LOCAL_IP to IP address)  
change slaves (add IP address)  

For Zookeeper:  
change conf/zoo.cfg (add server address and port)  
change data/myid (give a number)   

For Kafka:  
change config/server.properties (change [broker.id] [listeners] [advertised.listeners] [zookeeper.connect])   

For Redis:  
please read [Redis doc](./doc/Redis-install_and_config.md) for details  

For cx_Oracle:  
pip install -U cx_Oracle --proxy=http://web-proxy.cup.hp.com:8080  

For kafka-python:  
pip install -U kafka-python --proxy=http://web-proxy.cup.hp.com:8080  

make sure you restart all the services and set the environment variables after that

<a name="Service Web UIs"></a>
### Service Web UIs
|Service Name | Server name | URLs|
|-------------|----------------|-----------|
|Hadoop HDFS| c9t26359.itcs.hpecorp.net |[HDFS](http://16.250.37.241:50070/dfshealth.html)|
|Hadoop YARN| c9t26359.itcs.hpecorp.net |[YARN](http://16.250.37.241:8088/)|
|Spark Master| c9t26359.itcs.hpecorp.net |[Spark](http:/16.250.37.241:8080/)|

<a href="#Table of Contents">Go Back</a>

<a name="Version Information"></a>
###Version Information
|Component name		|Version Info|
|----------------|-----------|
|Python(Anaconda2)	|2.7.12|
|Java|1.8.0_72|
|Hadoop	|2.7.3|
|Spark	|2.1.0 - Hadoop2.7|
|Spark Streaming|Scala2.11|
|Kafka|2.11-0.10.1.0|
|Zookeeper|3.4.9|
|Scala|2.11|
|Maven|3.3.9|
|Redis|3.2.6|

<a name="Environment Variables"></a>
###Environment Variables
Check out [.bashrc](./conf/ITR2/.bashrc) and copy this file to your home folder (i.e. /home/yinlin) and then source it


<a name=Python></a>
###Python
To use Python please update your [.bashrc](./conf/ITR2/.bashrc) and source it

To build a Python source use the command below when in a Python source code directory

>python setup.py install

<a name=Hadoop></a>
###Hadoop
To use hadoop please update your [.bashrc](./conf/ITR2/.bashrc) and source it

Hadoop has been started on three servers, to start hadoop

run the command below
>./start-dfs.sh

>./start-yarn.sh

After it is done, you shall have started 
NameNode,SecondaryNameNode,DataNode for hdfs

and ResourceManager,NodeManager for yarn

<a href="#Table of Contents">Go Back</a>

<a name=Hive></a>
###Hive
To modify hive configuration,

consult hive-site.xml in $HIVE_HOME/conf

consult hive-env.sh in $HIVE_HOME/conf
To modify hive log setting,

consult hive-log4j2.properties

mysql root password is root/Shambella!

mysql database user is hadoop/hadoop

###Hive on Spark
To use Hive on Spark put

hive-site.xml($HIVE_HOMe/conf)

hdfs-site.xml($HADOOP_PREFIX/etc/hadoop)

core-site.xml($HADOOP_PREFIX/etc/hadoop)

in $SPAKR_HOME/conf

<a name="Zookeeper"></a>
###Zookeeper
Zookeeper has been started, you just need to use it.  
To start zookeeper go to $ZK_HOME of each node and start zookeeper on each node one by one
>bin/zkServer.sh start 

To check zookeeper status on each node use
>bin/zkServer.sh status

<a name=Kafka></a>
###Kafka
Kafka has been started on three servers, you just need to use it.  
To start Kafka go to $KAFKA_HOME of each node and start kafka on each node one by one  
>bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &  

Consult [kafka tips](doc/kafka_tips.md) for more details
kafka inherent zookeeper is not used

Kafka broker id is 100,101,102

Kafka topics used are

|Topics|
|----------|
|HPESCLTA1|
|HPESCLTA2|
|HPESCLTA3|
|HPESCLTM1|
|HPESCLTDVCM1|
|test|
|ESCLT_ACTN_D|

<a name=Spark></a>
###Spark
spark can be started using $SPARK_HOME/sbin/start-all.sh  
most code needs to be submitted using spark-submit  
for spark-submit there are few options:

(path to spark-submit)

--master yarn

--deploy-mode client

or

--deploy-mode cluster

--jars (path to each jar file, separated by comma if there are multiple jars)

--py-files (path to each file, separated by comma if there are multiple files)

(path to your code)

consult [Kafka_Spark_ITR2.sh](./bin/Kafka_Spark_ITR2.sh) for what a typical spark-submit command should look like

<a name=Redis></a>
###Redis
Redis has been installed on c9t26359, and it can be accessed from c9t26360 or c9t26361

Consult [Redis doc](./doc/Redis-install_and_config.md) for details

<a name="Configuration File"></a>
###Configuration File
The configuration file can be found at [ITR2_config.ini](./conf/ITR2/ITR2_config.ini)

<a name="Shared User Account"></a>
###Shared User Account
Username|Password
----|-----
hadoop|Shambella!

<a name="Command Help & Tips">
###Command Help & Tips
>gzip -d XXX.tar.gz

>tar xvf XXX.tar

>chown -R hadoop:ldap XXX

Consult [hadoop tips](./doc/hadoop_tips.md) for useful details

Consult [Kafka tips](./doc/kafka_tips.md) for useful details

Consult [python tips](./doc/python_tips.md) for useful details

Consult [redis tips](./doc/redis_tips.md) for useful details  

<a name="Cluster Status Check"></a>
###Cluster Status Check
When on c9t26359 use jps to check service status using user hadoop you shall see

|c9t26359|Service|
|----------|-----|
|Namenode|HDFS|
|Master|Spark|
|Kafka|Kafka|
|Jps|Jps|
|QuorumPeerMain|Zookeeper|
|Secondary Namenode|HDFS|
|RunJar|HiveMetaStore|
|RsourceManager|YARN|

When on c9t26360/c9t26361 use jps to check service status using user hadoop you shall see

|c9t26360,c9t26361|Service
|----------|----|
|Jps|Jps
|QuorumPeerMain|Zookeeper|
|Worker|Spark|
|DataNode|HDFS|
|Kafka|Kafka|
|NodeManager|YARN|

<a href="#Table of Contents">Go Back</a>

<a name="Github Tutorial"></a>
###Github Tutorial
When installing packages using pip, please add the proxy below to the option    
--proxy=http://web-proxy.cup.hp.com:8080  
To use github, please consult [Github Tutorial](./doc/github_on_linux.md) for details.

<a href="#Table of Contents">Go Back</a>

<a name="Environment Configuration"></a>
###Environment Configuration
Config files for all the components could be found in [/conf/ITR2](./conf/ITR2)

<a name="Packages"></a>
###Packages
Packages are found under [/lib/ITR2](./lib/ITR2)

####How to use Packages
To use a package named XXX.py, put it in the same directory as your code in hdfs,

and then in your code put the line below

>from pkg_name import func_name

pkg_name and func_name need to be subsitituted to the exact package and exact function you are using

####How to submit job with packages
To sumbit the job you need to use __--py-files__ option in spark-submit,

use comma to separate if you need to upload multiple python files

consult [Kafka_Spark_ITR2.sh](./bin/Kafka_Spark_ITR2.sh) for details

For more and more details, please consult [python tips](./doc/python_tips.md)

---------------------------------------------------------------------------------------------------------
Package name|Function name
----|-----
[OBSOLETE]kafka2spark|kafka2spark(ssc,topics,kafka_broker_list)*
kafka2spark|kafka2spark(ssc,topic)*
*__Input:__

*ssc is a sparkcontext or try sparksession

*topics are kafka topics separated by comma

>[OBSOLETE]topics = "HPESCLTA1,HPESCLTA2,HPESCLTA3"  
>topic = 'HPESCLTA3'  

*kafka_broker_list are kafka brokers of the form hostname:port separated by comma

>kafka_broker_list = "c9t26359.itcs.hpecorp.net:9092,c9t26360.itcs.hpecorp.net:9092,c9t26361.itcs.hpecorp.net:9092"

*__Return:__

*it will return kafka_stream, an object of type rdd composed of message from kafka bus transformed by spark into rdd

-------------------------------------------------------------------------------------------------------------

<a href="#Table of Contents">Go Back</a>
