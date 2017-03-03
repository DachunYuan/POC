#Redis Tips
To stop redis use  
>killall redis-server

To start a redis client to see the data in redis  
>redis-cli -h c9t26359.itcs.hpecorp.net -p 6379  
>redis-cli -h c9t26360.itcs.hpecorp.net -p 6379  
>redis-cli -h c9t26361.itcs.hpecorp.net -p 6379

then type the below command to show the keys
>keys *

You can see which key is installed on which server, then access the keys

##Use Redis on multiple servers
import redis_hash package
>from redis_hash import RedisOp
