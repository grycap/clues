# CLUES simulator

This is a simulator to test the CLUES core. It implements a syntetic LRMS FIFO and a emulated platform. It is possible to program the launch of jobs that, once launched, will request resources to CLUES and then will put the job in the queue (with a duration and a specific amount of resources to be allocated). The job will allocate some of the nodes in the platform for the whole duration of the _job_ and then these resources will be freed. 

The LRMS FIFO features any operation needed by CLUES (i.e. monitorization of jobs, monitorization of nodes, etc.), so almost any scheduler can be tested using this synthetic LRMS. The platform implements any operation needed by CLUES (i.e. power on nodes, power off, etc.).

The simulator needs a file that defines the simulation. It consists of a definition of the platform (i.e. definition of nodes), and a set of jobs (defined by the amount of resources and the duration), that will be executed in a specific time.

## Example 1

An example is included in file `job.sim`

```
# Example of node creation ()
# blank;node operation;number of cores;amount of memory;number of nodes;
;node;node;1;4096;4

# Example of job
# t in which the job is launched; job operation; numer of cores (default: random); amount of memory (default: random); duration of the job (default: random); number of nodes needed with the resources specified (default: 1)
10;job;1;512;10
12;job;1;1024;20
```

In this example, we create a platform of 4 nodes with 4096 Mb. and 1 core each. Then, at time 10s, we launch a job that needs 1 core and 512 Mb. RAM, and will be running for 10 seconds. At time 12s, we launch a job that needs 1 core and 1024 Mb. RAM, and will be running for 20 seconds.

The execution of the simulation will be like the next command:

```
$ python readmin.py -f job.sim -d job.db -s -t
```

The simulatior (readmin) will execute the workload in file `job.sim`, and the results will remain in database `job.db` (it is a standard CLUES database, and so it can be used to obtain reports). It will run in simulated time (`-s`) (i.e. not in real time, but going forward to the next event), and will truncate the database (`-t`).

## Example 2

Another example is included in file `platform.sim`

```
# Example of node creation ()
;node;node;1;4096;4
```

This is a simple platform that can be run with the simulator, just to test the runtime of CLUES (i.e. monitorization of the LRMS, power on or off the nodes via commandline, etc.).

The execution of the simulation will be like the next command:

```
$ python readmin.py -f platform.sim -d platform.db -t -n
```

The CLUES daemon will be started and the it is possible to use command `clues` to test the process by issuing command like

```
$ clues status
```
