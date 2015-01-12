# CLUES

CLUES is an **energy management system for High Performance Computing (HPC) Clusters and Cloud infrastructures**. The main function of the system is to **power off internal cluster nodes when they are not being used**, and conversely to power them on when they are needed. CLUES system **integrates with the cluster management middleware**, such as a batch-queuing system or a cloud infrastructure management system, by means of different connectors.

CLUES also **integrates with the physical infrastructure by means of different plug-ins**, so that nodes can be powered on/off using the techniques which best suit each particular infrastructure (e.g. using wake-on-LAN, Intelligent Platform Management Interface (IPMI) or Power Device Units, PDU).

Although there exist some batch-queuing systems that provide energy saving mechanisms, **some of the most popular choices, such as Torque/PBS, lack this possibility**. As far as cloud infrastructure management middleware is concerned, **none of the most usual options for scientific environments provide similar features**. The additional advantage of the approach taken by **CLUES is that it can be integrated with virtually any resource manager**, whether or not the manager provides energy saving features.
