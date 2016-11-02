#!/usr/bin/env python
#
# CLUES - Cluster Energy Saving System
# Copyright (C) 2015 - GRyCAP - Universitat Politecnica de Valencia
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import clueslib.configlib
import logging
import clueslib.helpers
import cpyutils.runcommand
import subprocess

from clueslib.node import NodeInfo
from cpyutils.evaluate import TypedClass, TypedList


import cpyutils.log
_LOGGER = cpyutils.log.Log("PLUGIN-SLURM")

def _translate_mem_value(memval):
    memval = memval.lower().rstrip(".").strip()
    
    multiplier = 1
    if len(memval) > 0:
        qualifier = memval[-2:]
        if qualifier == 'kb':
            multiplier = 1024
        elif qualifier == 'mb':
            multiplier = 1024*1024
        elif qualifier == 'gb':
            multiplier = 1024*1024*1024
        elif qualifier == 'tb':
            multiplier = 1024*1024*1024*1024
        elif qualifier == 'pb':
            multiplier = 1024*1024*1024*1024*1024
        
    if multiplier > 1:
        value_str = memval[:-2]
    else:
        value_str = memval
    
    try:
        value = int(value_str)
    except:
        try:
            value = float(value_str)
        except:
            value = -1
            
    return value * multiplier

def run_command(command):
    try:
        p=subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (out, err) = p.communicate()
        if p.returncode != 0:
            raise Exception("return code: %d\nError output: %s" % (p.returncode, err))
        return out
    except Exception as e:
        raise Exception("Error executing '%s': %s" % (" ".join(command), str(e)))

# This function facilitates the parsing of the scontrol command exit
def parse_scontrol(out):
    if out.find("=") < 0: return []
    r = []
    for line in out.split("\n"):
        line = line.strip()
        if not line: continue
        d = {}; r.append(d); s = False
        for k in [ j for i in line.split("=") for j in i.rsplit(" ", 1) ]:
            if s: d[f] = k
            else: f = k
            s = not s
    return r

# TODO: consider states in the second line of slurm
# Function that translates the slurm node state into a valid clues2 node state
def infer_clues_node_state(self, state):
    # SLURM node states: "NoResp", "ALLOC", "ALLOCATED", "COMPLETING", "DOWN", "DRAIN", "ERROR, "FAIL", "FAILING", "FUTURE" "IDLE", 
    #                "MAINT", "MIXED", "PERFCTRS/NPC", "RESERVED", "POWER_DOWN", "POWER_UP", "RESUME" or "UNDRAIN".
    # CLUES2 node states: ERROR, UNKNOWN, IDLE, USED, OFF
    res_state = ""

    if state == 'IDLE':
        res_state = NodeInfo.IDLE
    elif state == 'FAIL' or state == 'FAILING' or state == 'ERROR' or state == 'NoResp':
        res_state = NodeInfo.ERROR
    elif state == 'DOWN' or state == 'DRAIN' or state == 'MAINT':
        res_state = NodeInfo.OFF
    elif state == 'ALLOCATED' or state == 'ALLOC' or state == 'COMPLETING':
        res_state = NodeInfo.USED
    else:
        res_state = NodeInfo.OFF
        #res_state = NodeInfo.UNKNOWN

    return res_state

# Function that translates the slurm job state into a valid clues2 job state
def infer_clues_job_state(state):
    # a job can be in several states
    # SLURM job states: CANCELLED, COMPLETED, CONFIGURING, COMPLETING, FAILED, NODE_FAIL, PENDING, PREEMPTED, RUNNING, SUSPENDED, TIMEOUT
    # CLUES2 job states: ATTENDED o PENDING
    res_state = ""

    if state == 'PENDING':
        res_state = clueslib.request.Request.PENDING
    else:
        res_state = clueslib.request.Request.ATTENDED

    return res_state

# Function that recovers the partitions of a node
# A node can be in several queues: SLURM has supported configuring nodes in more than one partition since version 0.7.0
def get_partition(self, node_name):

    '''Exit example of scontrol show partitions: 
    PartitionName=wn
    AllowGroups=ALL AllowAccounts=ALL AllowQos=ALL
    AllocNodes=ALL Default=NO
    DefaultTime=NONE DisableRootJobs=NO GraceTime=0 Hidden=NO
    MaxNodes=UNLIMITED MaxTime=UNLIMITED MinNodes=1 LLN=NO MaxCPUsPerNode=UNLIMITED
    Nodes=wn[0-4]
    Priority=1 RootOnly=NO ReqResv=NO Shared=NO PreemptMode=OFF
    State=UP TotalCPUs=5 TotalNodes=5 SelectTypeParameters=N/A
    DefMemPerNode=UNLIMITED MaxMemPerNode=UNLIMITED'''
    
    res_queue = []
    exit = ""

    try:
        exit = parse_scontrol(run_command(self._partition))
    except:
        _LOGGER.error("could not obtain information about SLURM partitions %s (%s)" % (self._server_ip, exit))
        return None
    
    if exit:
        for key in exit:
            queue = key
            nodes = str(key["Nodes"])
            #nodes is like wnone-[0-1]
            pos1 = nodes.find("[")
            pos2 = nodes.find("]")
            if pos1 > -1 and pos2 > -1:
                num1 = int(nodes[pos1+1])
                num2 = int(nodes[pos2-1])
                name = nodes[:pos1]
                while num1 <= num2:
                    nodename = name + str(num1)
                    if nodename == node_name:
                        #res_queue.append(queue)
                        res_queue.append(key["PartitionName"])
                        break;
                    num1 = num1 + 1

    return res_queue

class lrms(clueslib.platform.LRMS):

    def __init__(self, SLURM_SERVER = None, SLURM_PARTITION_COMMAND = None, SLURM_NODES_COMMAND = None, SLURM_JOBS_COMMAND = None): 
        import cpyutils.config
        config_slurm = cpyutils.config.Configuration(
            "SLURM",
            {
                "SLURM_SERVER": "slurmserverpublic", 
                "SLURM_PARTITION_COMMAND": "/usr/local/bin/scontrol -o show partitions",
                "SLURM_NODES_COMMAND": "/usr/local/bin/scontrol -o show nodes",
                "SLURM_JOBS_COMMAND": "/usr/local/bin/scontrol -o show jobs"
            }
        )
        
        self._server_ip = clueslib.helpers.val_default(SLURM_SERVER, config_slurm.SLURM_SERVER)
        _partition_cmd = clueslib.helpers.val_default(SLURM_PARTITION_COMMAND, config_slurm.SLURM_PARTITION_COMMAND)
        self._partition = _partition_cmd.split(" ")
        _nodes_cmd = clueslib.helpers.val_default(SLURM_NODES_COMMAND, config_slurm.SLURM_NODES_COMMAND)
        self._nodes = _nodes_cmd.split(" ")
        _jobs_cmd = clueslib.helpers.val_default(SLURM_JOBS_COMMAND, config_slurm.SLURM_JOBS_COMMAND)
        self._jobs = _jobs_cmd.split(" ")
        clueslib.platform.LRMS.__init__(self, "SLURM_%s" % self._server_ip)

    def get_nodeinfolist(self):      
        nodeinfolist = {}
        
        '''Exit example of scontrol show nodes
        NodeName=wn0 Arch=x86_64 CoresPerSocket=1
        CPUAlloc=0 CPUErr=0 CPUTot=1 CPULoad=0.02 Features=(null)
        Gres=(null)
        NodeAddr=wn0 NodeHostName=wn0 Version=14.11
        OS=Linux RealMemory=1 AllocMem=0 Sockets=1 Boards=1
        State=IDLE ThreadsPerCore=1 TmpDisk=0 Weight=1
        BootTime=2015-04-28T13:12:21 SlurmdStartTime=2015-04-28T13:16:32
        CurrentWatts=0 LowestJoules=0 ConsumedJoules=0
        ExtSensorsJoules=n/s ExtSensorsWatts=0 ExtSensorsTemp=n/s'''

        exit = " "
        try:
            exit = parse_scontrol(run_command(self._nodes))
        except:
            _LOGGER.error("could not obtain information about SLURM nodes %s (%s)" % (self._server_ip, exit))
            return None

        if exit:
            for key in exit:
                name = str(key["NodeName"])
                slots_count = int(key["CPUTot"])
                slots_free = int(key["CPUTot"]) - int(key["CPUAlloc"])
                #NOTE: memory is in GB
                memory_total = _translate_mem_value(key["RealMemory"] + ".GB")
                memory_free = _translate_mem_value(key["RealMemory"] + ".GB") - _translate_mem_value(key["AllocMem"] + ".GB")
                state = infer_clues_node_state(self, str(key["State"]))
                keywords = {}
                queues = get_partition(self, name)
                keywords['hostname'] = TypedClass.auto(name)
                if queues:
                    keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])
                    
                nodeinfolist[name] = NodeInfo(name, slots_count, slots_free, memory_total, memory_free, keywords)
                nodeinfolist[name].state = state

        return nodeinfolist

    # Method in charge of monitoring the job queue of SLURM
    def get_jobinfolist(self):

        '''Exit example of scontrol -o show jobs:
        JobId=3 JobName=pr.sh
        UserId=ubuntu(1000) GroupId=ubuntu(1000)
        Priority=4294901758 Nice=0 Account=(null) QOS=(null)
        JobState=RUNNING Reason=None Dependency=(null)
        Requeue=0 Restarts=0 BatchFlag=1 Reboot=0 ExitCode=0:0
        RunTime=00:00:00 TimeLimit=UNLIMITED TimeMin=N/A
        SubmitTime=2015-05-13T12:34:57 EligibleTime=2015-05-13T12:34:57
        StartTime=2015-05-13T12:34:58 EndTime=Unknown
        PreemptTime=None SuspendTime=None SecsPreSuspend=0
        Partition=wn AllocNode:Sid=slurmserverpublic:1135
        ReqNodeList=(null) ExcNodeList=(null)
        NodeList=wn2
        BatchHost=wn2
        NumNodes=1 NumCPUs=1 CPUs/Task=1 ReqB:S:C:T=0:0:*:*
        Socks/Node=* NtasksPerN:B:S:C=0:0:*:* CoreSpec=*
        MinCPUsNode=1 MinMemoryNode=0 MinTmpDiskNode=0
        Features=(null) Gres=(null) Reservation=(null)
        Shared=0 Contiguous=0 Licenses=(null) Network=(null)
        Command=/home/ubuntu/pr.sh
        WorkDir=/home/ubuntu
        StdErr=/home/ubuntu/slurm-3.out
        StdIn=/dev/null
        StdOut=/home/ubuntu/slurm-3.out'''

        exit = " "
        jobinfolist = []

        try:
            exit = parse_scontrol(run_command(self._jobs))
        except:
            _LOGGER.error("could not obtain information about SLURM jobs %s (%s)" % (self._server_ip, exit))
            return None

        if exit:
            for job in exit:
                job_id = str(job["JobId"])
                state = infer_clues_job_state(str(job["JobState"]))
                nodes = []
                # ReqNodeList is also available
                if str(job["NodeList"]) != "(null)":
                    nodes.append(str(job["NodeList"]))
                if len(job["NumNodes"]) > 1:
                    numnodes = int(job["NumNodes"][:1])
                else:
                    numnodes = int(job["NumNodes"])
                memory = _translate_mem_value(job["MinMemoryNode"] + ".MB")
                cpus_per_task = int(job["CPUs/Task"])
                partition = '"' + str(job["Partition"]) + '" in queues'

                resources = clueslib.request.ResourcesNeeded(cpus_per_task, memory, [partition], numnodes)
                j = clueslib.request.JobInfo(resources, job_id, nodes)
                j.set_state(state)
                jobinfolist.append(j)
        
        return jobinfolist
        
if __name__ == '__main__':
    pass
