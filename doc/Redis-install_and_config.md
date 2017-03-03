#Redis install location
server IP:16.250.37.241

install dir:$ITR2_HOME


#Redis install process

>mkdir redis-3.2

>cd redis-3.2

>tar -zxvf redis-3.2.6.tar.gz

>ln -s redis-2.6.14 redis

>cd redis

>make PREFIX=/opt/mount1/app/redis-3.2 install 

>cd bin

>cp ../redis-3.2.6/redis.conf .


#verify that the redis is successful installation
>cd /opt/mount1/app/redis-3.2/bin

>redis-server redis.conf

Open a new window to open the redis-cli

>cd /opt/mount1/app/redis-3.2/bin

>redis-cli

>set key value

Return OK indicates sucess

#set redis config
>vim redis.conf

Find the keyword "bind" ,then add a line,input "bind 16.250.37.241"

Only by binding server IP can other servers have the access to redis.
	
Setting "daemonize" to yes,the redis can run in the background

>daemonize yes

When the redis runs for a period of time,we operate the redis,it gives a "MISCONF Redis is configured to save RDB snapshots,........" error.

There are two methods to solve the problem.

	One:

		In /etc/sysctl.conf file to add a line,the content of the increase is "vm.overcommit_memory = 1".

		Then,input "sysctl vm.overcommit_memory=1" in the cmd and restart the redis.

	Two:

		Running order in /opt/mount1/app/redis-3.2/bin director.

		Then,input "config set stop-writes-on-bgsave-error no" and restart the redis.

