# CLUES

CLUES is an **energy management system for High Performance Computing (HPC) Clusters and Cloud infrastructures**. The main function of the system is to **power off internal cluster nodes when they are not being used**, and conversely to power them on when they are needed. CLUES system **integrates with the cluster management middleware**, such as a batch-queuing system or a cloud infrastructure management system, by means of different connectors.

CLUES also **integrates with the physical infrastructure by means of different plug-ins**, so that nodes can be powered on/off using the techniques which best suit each particular infrastructure (e.g. using wake-on-LAN, Intelligent Platform Management Interface (IPMI) or Power Device Units, PDU).

Although there exist some batch-queuing systems that provide energy saving mechanisms, **some of the most popular choices, such as Torque/PBS, lack this possibility**. As far as cloud infrastructure management middleware is concerned, **none of the most usual options for scientific environments provide similar features**. The additional advantage of the approach taken by **CLUES is that it can be integrated with virtually any resource manager**, whether or not the manager provides energy saving features.

[Installing][]
[Installing](#Installing)

# Installing

In order to install CLUES you can follow the next steps:

## Prerrequisites

You need a python interpreter and the easy_install commandline tool. In ubuntu, you can install these things as

```
$ apt-get -y install python python-setuptools
```

Git is also needed, in order to get the source code

```
$ apt-get -y install git
```

Now you need to install the cpyutils

```
$ git clone https://github.com/grycap/cpyutils
$ mv cpyutils /opt
$ cd /opt/cpyutils
$ python setup.py install --record installed-files.txt
```

In case that you want, you can safely remove the ```/opt/cpyutils``` folder. But it is recommended to keep the ```installed-files.txt``` file just in order to be able to uninstall the cpyutils.

Finally you need to install two python modules from pip:

```
$ easy_install ply web.py
```

## Installing CLUES

Firt of all, you need to get the CLUES source code and then install it.

```
$ git clone https://github.com/grycap/clues
$ mv clues /opt
$ cd /opt/clues
$ python setup.py install --record installed-files.txt
```

In case that you want, you can safely remove the ```/opt/clues``` folder. But it is recommended to keep the ```installed-files.txt``` file just in order to be able to uninstall CLUES.

Now you ***must config*** CLUES, as it won't work unless you have a valid configuration.

## Configuring CLUES

You need a /etc/clues2/clues2.cfg file. So you can get the template and use it for your convenience.

```
$ cd /etc/clues2
$ cp clues2.cfg-example clues2.cfg
``` 

Now you can edit the ```/etc/clues2/clues2.cfg``` and adjust its parameters for your specific deployment.

The most important parameters that you MUST adjust are ```LRMS_CLASS```, ```POWERMANAGER_CLASS``` and ```SCHEDULER_CLASSES```. 

For the ```LRMS_CLASS``` you have different options available (you *MUST* state one and only one of them):
* cluesplugins.one that is designed to work in an OpenNebula deployment.
* cluesplugins.pbs that is designed to work in a Torque/PBS environment.
* cluesplugins.sge that is designed to work in a SGE-like environment
* cluesplugins.slurm that is designed to work in a SLURM environment

For the ```POWERMANAGER_CLASS``` you have different options available (you *MUST* state one and only one of them):
* cluesplugins.ipmi to power on or off working nodes in an physical infrastructure using IPMI calls
* cluesplugins.wol to power on working nodes in an physical infrastructure using Wake-on-Lan calls, and powering them off using password-less SSH connections.
* cluesplugins.one to create and destoy virtual machines as working nodes in a OpenNebula IaaS.
* cluesplugins.onetemplate to create and destoy virtual machines as working nodes in a in a OpenNebula IaaS (creating the template inline instead of using existing templates).
* cluesplugins.im that is designed to work in an multi-IaaS environment managed by the Infrastructure Manager [link pending].

Finally, you should state the CLUES schedulers that you want to use. It is a comma-separated ordered list where the schedulers are being called in the same order that they are stated.

For the ```SCHEDULER_CLASSES``` parameter you have the following options available:
* clueslib.schedulers.CLUES_Scheduler_PowOn_Requests that will react up on the requests for resources from the underlying middleware. It will take into account the requests for resources and will power on some nodes if needed.
* clueslib.schedulers.CLUES_Scheduler_Reconsider_Jobs, that will monitor the jobs in the LRMS and will power on some resources if the jobs are in the queue for too long.
* clueslib.schedulers.CLUES_Scheduler_PowOff_IDLE, that will power off the nodes that are IDLE after a period of time.
* clueslib.schedulers.CLUES_Scheduler_PowOn_Free, that will keep extra empty slots or nodes.

Each of the LRMS, POWERMANAGER or SCHEDULER has its own options that should be properly configured.

### Example configuration with ONE

In this example we are integrating CLUES in a OpenNebula 4.8 deployment, which is prepared to power on or off the working nodes using IPMI. In the next steps we are configuring CLUES to monitor the ONE deployment and to intercept the requests for new VMs.

On the one side, we must set the proper values in /etc/clues2/clues2.cfg. The most important values are:

```
[general]
CONFIG_DIR=conf.d
LRMS_CLASS=cluesplugins.one
POWERMANAGER_CLASS=cluesplugins.ipmi
MAX_WAIT_POWERON=300
...
[monitoring]
COOLDOWN_SERVED_REQUESTS=300
...
[scheduling]
SCHEDULER_CLASSES=clueslib.schedulers.CLUES_Scheduler_PowOn_Requests, clueslib.schedulers.CLUES_Scheduler_Reconsider_Jobs, clueslib.schedulers.CLUES_Scheduler_PowOff_IDLE, clueslib.schedulers.CLUES_Scheduler_PowOn_Free
IDLE_TIME=600
RECONSIDER_JOB_TIME=600
EXTRA_SLOTS_FREE=0
EXTRA_NODES_PERIOD=60
```

* CONFIG_DIR is the folder (relative to the CLUES configuration folder: /etc/clues2), where the *.cfg files will be considered as part of the configuration (e.g. for the configuration of the plugins).
* LRMS_CLASS is set to use the ONE plugin to monitor the deployment.
* POWERMANAGER_CLASS is set to use IPMI to power on or off the working nodes.
* MAX_WAIT_POWERON is set to an upper bound of the time that a working node lasts to be power on and ready from the IPMI order to power on (in our case 5 minutes). If this time passes, CLUES will consider that the working node has failed to be powered on.
* COOLDOWN_SERVED_REQUESTS is the time during which the requested resources for a VM will be booked by CLUES, once it has been attended (e.g. some working nodes have been powered on). It is needed to take into account the time that passes from when a VM is released to ONE to when the VM is finally deployed into a working node. In case of ONE, when the VM is finally hosted in a host, this time is aborted (it does not happen in other LRMS).
* SCHEDULER_CLASSES are the power-on features that we want for the deployment. In this case, we are reacting up on requests, and we will also consider the requests for resources of jobs that are in the queue for too long. Then, we will power off the working nodes that have been idle for too long, but we will keep some slots free.
* IDLE_TIME is related to the CLUES_Scheduler_PowOff_IDLE and is the time during which a working node has to be idle to be considered to be powered off.
* RECONSIDER_JOB_TIME is related to the CLUES_Scheduler_Reconsider_Jobs scheduler, and states the frequency (in seconds) that a job has to be in the queue before its resources are reconsidered.
* EXTRA_SLOTS_FREE is related to the CLUES_Scheduler_PowOn_Free scheduler and states how many slots should be free in the platform.
* EXTRA_NODES_PERIOD=60 is also related to CLUES_Scheduler_PowOn_Free and states the frequency of the scheduler. It is not executed all the time to try to avoid transient allocations.
 
Once this file is configured, we can use the templates in the /etc/clues2/conf.d folder to configure the ONE and IPMI plugins. So we are creating the proper files:

```
$ cd /etc/clues2/conf.d/
$ cp plugin-one.cfg-example plugin-one.cfg         
$ cp plugin-ipmi.cfg-example plugin-ipmi.cfg         
```

In the ```/etc/clues2/conf.d/plugin-one.cfg``` we should check the variables ```ONE_XMLRPC``` and ```ONE_AUTH```, and set them to the proper values of your deployment. The credentials in the ```ONE_AUTH``` variable should be of a user in the ```oneadmin``` group (you can use the oneadmin user or create a new one in ONE).

```
[ONE LRMS]
ONE_XMLRPC=http://localhost:2633/RPC2
ONE_AUTH=clues:cluespass
```

In the ```/etc/clues2/conf.d/plugin-ipmi.cfg``` we should check the variables ```IPMI_HOSTS_FILE``` and ```IPMI_CMDLINE_POWON```  and ```IPMI_CMDLINE_POWOF```, and set them to the proper values of your deployment. 

```
[IPMI]
IPMI_HOSTS_FILE=ipmi.hosts
IPMI_CMDLINE_POWON=/usr/bin/ipmitool -I lan -H %%a -P "" power on
IPMI_CMDLINE_POWOFF=/usr/bin/ipmitool -I lan -H %%a -P "" power off
```

The ```ipmi.hosts``` should be located in the folder ```/etc/clues2/``` and contains the correspondences of the IPMI IP addresses and the names of the hosts that appear in ONE, using the well known ```/etc/hosts``` file format. An example for this file is, where the first column is the IPMI IP address and the second column is the name of the host as appears in ONE.

```
192.168.1.100   niebla01
192.168.1.102   niebla02
192.168.1.103   niebla03
192.168.1.104   niebla04
```

The you should adjust the commandline for powering on and off the working nodes, using IPMI. In the default configuration we use the common ```ipmitool``` app and we use a passwordless connection to the IPMI interface. To adjust the commandline you can use %%a to substitute the IP address and %%h to substitute the hostname

## Troubleshooting

You can get information in the CLUES log file (i.e. ```/var/log/clues2/clues2.log```). But you can also set the  ```LOG_FILE``` to a empty value in the ```/etc/clues2/clues2.cfg``` file and execute CLUES as 

```
$ /usr/bin/python /usr/local/bin/cluesserver
```

In the logging information you can find useful messages to debug what is happening. Here we highlight some common issues.

### Wrong ONE configuration

Some messages like

```
[DEBUG] 2015-06-18 09:41:57,551 could not contact to the ONE server
[WARNING] 2015-06-18 09:41:57,551 an error occurred when monitoring hosts (could not get information from ONE; please check ONE_XMLRPC and ONE_AUTH vars)
```

usually mean that either the URL that is pointed by ONE_XMLRPC is wrong (or not reachable) or the ONE_AUTH information has not enough privileges. 

In a distributed configuration, maybe the ONE server is not reachable from outside the localhost.
