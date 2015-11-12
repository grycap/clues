
Special configurations
----------------------------

This document explains some special configurations for CLUES

Using IPMI via ssh
------------------------

Under some circumstances, you will have your CLUES server in other server than the one that has access to the IPMI network. E.g. You have CLUES running in host1, but the one who has access to the IPMI network is host2

In such case, you can easily modify the IPMI configuration to issue the IPMI commands through an additional server. Just follow the next steps:

Configure ssh access without password
-------------------------------------------------

In host1 issue

  # ssh-keygen 

Copy the file $HOME/.ssh/id_rsa.pub to host2 and concat it to the /root/.ssh/authorized_keys file (you can make it by using copy-paste to the content of the file).

  # cat id_rsa.pub >> /root/.ssh/authorized_keys

Test it by ssh-ing from host1 to host2

  # ssh -o StrictHostKeyChecking=no root@host2

