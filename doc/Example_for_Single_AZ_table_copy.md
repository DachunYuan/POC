	Ø 单个表的AZ table Copy：
	1,g4t7491_[radamgr@g4t7491 200836_az_fi1_1051041]$ scp -r t001 hadoop@16.152.119.10:/opt/userdata
	hadoop@16.152.119.10's password:
	000000_0                  
	
	2,[hadoop@hadoopmaster ~]$ hadoop fs -put /opt/userdata/t001 /RADAR/hive/200836_az_fi1_1051041/
	[hadoop@hadoopmaster ~]$ hadoop fs -ls /RADAR/hive/200836_az_fi1_1051041
	Found 1 items
	drwxr-xr-x   - hadoop supergroup          0 2016-12-22 13:45 /RADAR/hive/200836_az_fi1_1051041/t001
	3,on g4t7491_[radamgr@g4t7491:
	hive> show create table 200836_az_fi1_1051041.t001;
	OK
	CREATE EXTERNAL TABLE `200836_az_fi1_1051041.t001`(
	  `mandt` varchar(3),
	  `bukrs` varchar(4),
	  `butxt` varchar(25),
	  `ort01` varchar(25),
	  `land1` varchar(3),
	  `waers` varchar(5),
	  `spras` varchar(1),
	  `ktopl` varchar(4),
	  `buvar` varchar(1),
	  `fdbuk` varchar(4),
	  `xfdis` varchar(1),
	  `waabw` decimal(2,0),
	  `xvalv` varchar(1),
	  `xskfn` varchar(1),
	  `kkber` varchar(4),
	  `xmwsn` varchar(1),
	  `periv` varchar(2),
	  `kokfi` varchar(1),
	  `mregl` varchar(4),
	  `xgsbe` varchar(1),
	  `xgjrv` varchar(1),
	  `xkdft` varchar(1),
	  `rcomp` varchar(6),
	  `xprod` varchar(1),
	  `adrnr` varchar(10),
	  `xeink` varchar(1),
	  `stceg` varchar(20),
	  `xjvaa` varchar(1),
	  `xvvwa` varchar(1),
	  `xslta` varchar(1),
	  `fikrs` varchar(4),
	  `xfdmm` varchar(1),
	  `xfdsd` varchar(1),
	  `xextb` varchar(1),
	  `ebukr` varchar(4),
	  `ktop2` varchar(4),
	  `umkrs` varchar(4),
	  `bukrs_glob` varchar(6),
	  `fstva` varchar(4),
	  `opvar` varchar(4),
	  `xfmco` varchar(1),
	  `xcovr` varchar(1),
	  `txkrs` varchar(1),
	  `wfvar` varchar(4),
	  `xfmcb` varchar(1),
	  `xfmca` varchar(1),
	  `xbbbf` varchar(1),
	  `xbbbe` varchar(1),
	  `xbbba` varchar(1),
	  `xbbko` varchar(1),
	  `xstdt` varchar(1),
	  `mwskv` varchar(2),
	  `mwska` varchar(2),
	  `impda` varchar(1),
	  `txjcd` varchar(15),
	  `fmhrdate` date,
	  `xnegp` varchar(1),
	  `xkkbi` varchar(1),
	  `wt_newwt` varchar(1),
	  `pp_pdate` varchar(1),
	  `infmt` varchar(4),
	  `fstvare` varchar(4),
	  `kopim` varchar(1),
	  `dkweg` varchar(1),
	  `offsacct` decimal(1,0))
	ROW FORMAT DELIMITED
	  FIELDS TERMINATED BY '\u0001'
	STORED AS INPUTFORMAT
	  'org.apache.hadoop.mapred.TextInputFormat'
	OUTPUTFORMAT
	  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
	LOCATION
	  'maprfs:/RADAR/hive/200836_az_fi1_1051041/t001'
	TBLPROPERTIES (
	  'COLUMN_STATS_ACCURATE'='true',
	  'numFiles'='1',
	  'totalSize'='35736',
	  'transient_lastDdlTime'='1466045487')
	Time taken: 0.133 seconds, Fetched: 79 row(s)
	hive>
	4,on hadoop@hadoopmaster:
	转换成以下DDL:
	CREATE EXTERNAL TABLE 200836_az_fi1_1051041.t001(
	  mandt varchar(3),
	  bukrs varchar(4),
	  butxt varchar(25),
	  ort01 varchar(25),
	  land1 varchar(3),
	  waers varchar(5),
	  spras varchar(1),
	  ktopl varchar(4),
	  buvar varchar(1),
	  fdbuk varchar(4),
	  xfdis varchar(1),
	  waabw decimal(2,0),
	  xvalv varchar(1),
	  xskfn varchar(1),
	  kkber varchar(4),
	  xmwsn varchar(1),
	  periv varchar(2),
	  kokfi varchar(1),
	  mregl varchar(4),
	  xgsbe varchar(1),
	  xgjrv varchar(1),
	  xkdft varchar(1),
	  rcomp varchar(6),
	  xprod varchar(1),
	  adrnr varchar(10),
	  xeink varchar(1),
	  stceg varchar(20),
	  xjvaa varchar(1),
	  xvvwa varchar(1),
	  xslta varchar(1),
	  fikrs varchar(4),
	  xfdmm varchar(1),
	  xfdsd varchar(1),
	  xextb varchar(1),
	  ebukr varchar(4),
	  ktop2 varchar(4),
	  umkrs varchar(4),
	  bukrs_glob varchar(6),
	  fstva varchar(4),
	  opvar varchar(4),
	  xfmco varchar(1),
	  xcovr varchar(1),
	  txkrs varchar(1),
	  wfvar varchar(4),
	  xfmcb varchar(1),
	  xfmca varchar(1),
	  xbbbf varchar(1),
	  xbbbe varchar(1),
	  xbbba varchar(1),
	  xbbko varchar(1),
	  xstdt varchar(1),
	  mwskv varchar(2),
	  mwska varchar(2),
	  impda varchar(1),
	  txjcd varchar(15),
	  fmhrdate date,
	  xnegp varchar(1),
	  xkkbi varchar(1),
	  wt_newwt varchar(1),
	  pp_pdate varchar(1),
	  infmt varchar(4),
	  fstvare varchar(4),
	  kopim varchar(1),
	  dkweg varchar(1),
	  offsacct decimal(1,0))
	ROW FORMAT DELIMITED
	  FIELDS TERMINATED BY '\u0001'
	STORED AS INPUTFORMAT
	  'org.apache.hadoop.mapred.TextInputFormat'
	OUTPUTFORMAT
	  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
	LOCATION
	  '/RADAR/hive/200836_az_fi1_1051041/t001';
	  
	5，on hadoop@hadoopmaster:
	hive> select * from t001 limit 3;
	OK
	100     AE00    HP Middle East FZ-LLC   Dubai   AE      AED     E       AE00                            0       X       HP       2                       X                               0050020803                                      X       AE00     WW00    WW00                    1                                                                       X0      1900-01-01                                                                       0
	100     AE10    HP ISE GmBH Abu Dhabi   Abu Dhabi       AE      AED     E       AE00                            0       XHP      2                       X                               0050037616                                      X       AE10     WW00    WW00                    1                                                                               1900-01-01                                                                       0
	100     AN10    Perlina Corporation NV  Willemstad      AN      USD     E       NLK6                            0       HP       2                                                       0050023265                                              AN10     WW00                                                                                                            1900-01-01                                                                       0
	Time taken: 0.132 seconds, Fetched: 3 row(s)
	hive>
