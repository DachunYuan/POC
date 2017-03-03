#unix tips

##add user
>useradd hadoop

##add group
>groupadd ldap

##change user password
>sudo su
>passwd hadoop

##add existing user to existing group
>usermod -a -G ldap hadoop

##show user group information
>id hadoop

##Add existing user into sudoer
>vim /etc/sudoers

add this line below root ALL=(ALL)  ALL
>hadoop ALL=(ALL) NOPASSWD: ALL

use the line below to save to a READONLY file
>:w!  
>:q
