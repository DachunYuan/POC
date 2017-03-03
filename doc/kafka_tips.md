#Kafka Command Help

You first need to be in kafka home directory

>cd $KAFKA_HOME/bin

##Start kafka Consumer Client (Show messages received by topic)
>kafka-console-consumer.sh --zookeeper c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181 --topic test  
>kafka-console-consumer.sh --zookeeper c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181 --topic HPESCLTDVCM1  
>bin/kafka-console-consumer.sh --zookeeper c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181 --topic APP_ESCLT
>bin/kafka-console-consumer.sh --zookeeper c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181 --topic HPESCLTM1

##Start kafka on three servers
do this on 59/60/61 one by one and close using X upper right corner
>bin/kafka-server-start.sh config/server.properties &

##List kafka topics
>kafka-topics.sh --list --zookeeper c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181

##Delete kafka topics
>bin/kafka-topics.sh --zookeeper c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181 --delete --topic HPESCLTA1

>bin/kafka-topics.sh --zookeeper c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181 --delete --topic HPESCLTA2

>bin/kafka-topics.sh --zookeeper c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181 --delete --topic HPESCLTA3

##Create kafka topics
>bin/kafka-topics.sh --create --zookeeper c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181 --partitions 1 --replication-factor 1 --topic HPESCLTA1

>bin/kafka-topics.sh --create --zookeeper c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181 --partitions 1 --replication-factor 1 --topic HPESCLTA2

>bin/kafka-topics.sh --create --zookeeper c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181 --partitions 1 --replication-factor 1 --topic HPESCLTA3

##Describe kafka topics
>bin/kafka-topics.sh --describe --zookeeper c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181 --topic HPESCLTA1

>bin/kafka-topics.sh --describe --zookeeper c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181 --topic HPESCLTA2

>bin/kafka-topics.sh --describe --zookeeper c9t26359.itcs.hpecorp.net:2181,c9t26360.itcs.hpecorp.net:2181,c9t26361.itcs.hpecorp.net:2181 --topic HPESCLTA3


