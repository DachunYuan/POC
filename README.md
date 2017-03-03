# POC Playground
This repository is used to manage and track Spark POC stuff. Two teams from RADAR and ITR2 will get involved in this POC.
RADAR and ITR2 supply their own business logic and data to test and implement the solution in own environment, but the virtual team share ideas and solution.

---

### Table of Contents

* <a href="#Goal">Goal</a>
* <a href="#Architecture">Architecture</a>
* <a href="#Data Flow">Data Flow</a>
    * <a href="#Batch Processing">Batch Processing</a>
    * <a href="#Streaming Processing">Streaming Processing</a>
* <a href="#RADAR Scope">RADAR Scope</a>
* <a href="#RDD Operation">RDD Operation</a>
* <a href="#RADAR Lab Environment">RADAR Lab Environment</a>
   * <a href="#Cluser Information">Cluser Information</a>
   * <a href="#Cluster Service Matrix">Cluster Service Matrix</a>
   * <a href="#Service Web UIs">Service Web UIs</a>
* <a href="#Team members">Team members</a>

---

<a name="Goal"></a>


## Goal
Goal Phase I (Duration: Start from 12/12/2016 to 1/13/2017):
* Work out a stable and efficient solution for batch load based on SparkSQL.
* Implement one typical fact table using SparkSQL, and test performance

Goal Phase II (Duration: plan it later. It’s should be after China new year break):
* Create a real time computing architecture based on Hadoop ecosystem

<a name="Architecture"></a>
## Architecture
### Batch Processing
![Batch Processing](./images/batching_architecture.jpg)

<a name="Data Flow"></a>
## Data Flow
In this section, we will explain overall data flow for both batch and streaming processing.
<a name="Batch Processing"></a>
### Batch Processing
The following diagram illustrates overall data flow of data ingesting and processing. First, data will be derived from external RDBMS or file systems by Apache Sqoop or customed program, and then loaded to HDFS (Step1). Hive external table points to these HDFS files loaded from external systems. Second, user invokes Spark job. Spark job will be initialized Spark driver which interacts with Yarn resource manager and allocates Spark executors and schedules Spark tasks (Step 2) and retrieves Hive meta store (Step 3). Spark tasks will be executed over Yarn nodes, and they read data from Hive staging/archiving table (Step 4) and process and loaded back to Hive integrated table (Step 5). Similarly with procesing Hive exteranl table, data will be read (Step 8), processed and loaded into (Step 9). In the end, data in Hive reporting table will be copied to Vertica table through HDFS connector.

![Batching Data Flow](./images/batching_data_flow.jpg)

Regarding incremental loading, we might consider below approach. Firstly, we need to analyze incremental data to determine which values in partitioning columns like source system code in this figure are related (Step 1). If there are some records in this incremental data which needs to update existing data in Hive, we need to read ALL data in corresponding partition in existing Hive table (Step 2). For instance, there is one record (partitioning value is "AER") in incremental data which should update existing records in Hive table. We should read ALL data in partitioning value "AER" except corresponding records in incremental data and combine rest of data in incremental data together (Step 3.1), and they are processed as required (Step 3.2). Then, processed data is loaded to Hive temporary table same as Hive partitioning table (Step 4). In the end, we exchanged partitons with the two Hive tables (Step 5). 

![incremental Loading](./images/incremental_load.jpg)




<a name="RADAR Scope"></a>
## RADAR Scope
FI1-ORD_ITM_FACT W3/W6 from AZ to IC and RZ. Ensure data can be retrieve from BOE side and complete the result comparison with current reports. It’s should support increment load.
Highlight Items:
1. List AZ tables and create in LAB Hive
2. Move AZ data to LAB.
3. List IC/RZ table and create in LAB hive
4. List IC/RZ scripts.
5. RADAR Tasks arrangement.
6. BOE report result comparison.

<a name="RADAR Lab Environment"></a>
## RADAR Lab Environment
<a name="Cluser Information"></a>
### Cluser Information

Server name		|Memory (GB)|Disk Space (GB)|Server IP		|OS
----------------|-----------|---------------|---------------|---------
hadoopmaster	|12			|454			|16.152.119.10	|Centos 6.6 64bit
hadoopslave1	|12			|454			|16.152.119.8	|Centos 6.6 64bit
hadoopslave3	|12			|432			|16.152.119.13	|Centos 6.6 64bit
hadoopvertica	|4			|125			|16.152.119.9	|Centos 6.6 64bit
hadoopslave5	|13			|175			|16.152.119.15	|Centos 6.6 32bit
hadoopslave5VM	|13			|				|16.152.119.11	|VM
ssit-grc-public	|16			|454			|16.152.119.11	|Win7 64it
hadoopslave7	|8			|200			|16.152.119.16	|Centos 6.6 64bit

<a name="Cluster Service Matrix"></a>
### Cluster Service Matrix
![Cluster Service Matrix](./images/service_metrics.jpg)

<a name="Service Web UIs"></a>
### Service Web UIs
Service Name | Server name		| URLs
-------------|----------------|-----------
Hadoop HFDS| hadoopmaster	|[HDFS](http://16.152.119.10:50070/dfshealth.html)
Hadoop Yarn| hadoopmaster	| [Yarn](http://16.152.119.10:8088/)
Apache Hue| hadoopmaster	|[Apache Hue](http://16.152.119.10:8000/beeswax)
Apache Oozie| hadoopslave3	|[Apache Oozie](http://16.152.119.13:11000/oozie/)
Spark History Service| hadoopmaster	|[Apache Spark History Service](http://16.152.119.10:18080/)
Apache Airflow| hadoopslave1	|[Apache Airflow](http://16.152.119.8:4080/)
Apache Nifi| hadoopslave3	|[Apache Nifi](http://16.152.119.13:8880/nifi/)


<a name="Team members"></a>
## Team members
ITR2:
* Lin, Yin yin.lin@hpe.com
* Zeng, Chun Xia (Billie,SSIT-China-CQ) czeng@hpe.com
* Chen, Xiu-Hong (Sakina, HPIT-SSIT BI Practice-CQ) xiu-hong.chen@hpe.com
* Cai, Cheng-Xin (Chengxin Cai, SSIT) cheng-xin.cai@hpe.com
* Zhu, Xia-Jun (Chris, SSIT) xia-jun.zhu@hpe.com  -- Tech leader
* Wu, Hua-Jun (Damon,SSIT-MDM-CQ) hua-jun.wu@hpe.com  -- Java skill
* Wang, Xiao-Yan (Irene, SSIT) wang.xiaoyan@hpe.com -- Coordinator

RADAR:
* Huang, Lang (Bobby,GD-GRC) <lang.huang@hpe.com>
* Yang, Zhen-Peng (Arvin, GD-GRC) <zhen-peng.yang@hpe.com>
* Yuan, Da-Chun (Echo,GD-GRC) <da-chun.yuan@hpe.com>
* Tan, Jian-Ming (Jimmy,GD-GRC) <jian-ming.tan@hpe.com>
* Peng, Feng (Fred, GD-GRC) <feng.peng@hpe.com>
* Qian, Hong (Anho, GD-GRC) <hong.qian@hpe.com>
* He, Chen-Gang (GD-GRC Shanghai) <chen-gangh@hpe.com>
* Ma, Li-Na (Li-Na, GD-GRC) <lina.ma@hpe.com>
