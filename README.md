# CLUES

CLUES is an **energy management system for High Performance Computing (HPC) Clusters and Cloud infrastructures**. The main function of the system is to **power off internal cluster nodes when they are not being used**, and conversely to power them on when they are needed. CLUES system **integrates with the cluster management middleware**, such as a batch-queuing system or a cloud infrastructure management system, by means of different connectors.

CLUES also **integrates with the physical infrastructure by means of different plug-ins**, so that nodes can be powered on/off using the techniques which best suit each particular infrastructure (e.g. using wake-on-LAN, Intelligent Platform Management Interface (IPMI) or Power Device Units, PDU).

Although there exist some batch-queuing systems that provide energy saving mechanisms, **some of the most popular choices, such as Torque/PBS, lack this possibility**. As far as cloud infrastructure management middleware is concerned, **none of the most usual options for scientific environments provide similar features**. The additional advantage of the approach taken by **CLUES is that it can be integrated with virtually any resource manager**, whether or not the manager provides energy saving features.

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
$ cd /etc/clues2/clues2
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
