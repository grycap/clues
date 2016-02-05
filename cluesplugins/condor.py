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

import logging
import cpyutils.eventloop
import cpyutils.oneconnect
import cpyutils.config
import clueslib.node
import clueslib.helpers
import clueslib.request
import clueslib.platform
from clueslib.node import Node, NodeInfo, NodeList
from cpyutils.evaluate import TypedClass, TypedList

import subprocess

import htcondor
import classad

_LOGGER = logging.getLogger("[PLUGIN-HTCondor]")

# Executes external commands
def run_command(command): 
    try:
        p=subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (out, err) = p.communicate()
        if p.returncode != 0:
            raise Exception("return code: %d\nError output: %s" % (p.returncode, err))
        return out
    except Exception as e:
        raise Exception("Error executing '%s': %s" % (" ".join(command), str(e)))


# Determines the equivalent state between Condor jobs and Clues2 possible job states
def infer_clues_job_state(state):
    # Condor job states: 1-Idle, 2-Running, 3-Removed, 4-Completed, 5-Held, 6-Transferring Output, 7-Suspended
    # CLUES2 job states: ATTENDED o PENDING
    if state == 1:
        return clueslib.request.Request.PENDING
    else:
        return clueslib.request.Request.ATTENDED


class lrms(clueslib.platform.LRMS):
    
    def __init__(self, HTCONDOR_SERVER = None):
        config_htcondor = cpyutils.config.Configuration("HTCONDOR", {"HTCONDOR_SERVER": "htcondoreserver"})
        self._server_ip = clueslib.helpers.val_default(HTCONDOR_SERVER, config_htcondor.HTCONDOR_SERVER)
        clueslib.platform.LRMS.__init__(self, "HTCONDOR_%s" % self._server_ip)
         
    def get_nodeinfolist(self):
        nodeinfolist = {}
        worker_nodes = []
        collector = htcondor.Collector()
        try: 
            worker_nodes = collector.locateAll(htcondor.DaemonTypes.Startd)
        except: 
            worker_nodes = []
        if len(worker_nodes) > 0:
            for worker_node in worker_nodes:
                activity = ""
                name = ""
                slots = 0
                slots_free = 0
                memory = 0
                memory_free = 0
                keywords = {}
                queues = []
                try:
                    activity = worker_node["Activity"]
                except:
                    activity = "undefined"
                if activity == "Idle":
                    try:
                        name = worker_node["Name"]
                    except:
                        name = ""
                    try:
                        slots = worker_node["TotalSlots"]
                    except:
                        slots = 0
                    slots_free = slots
                    try:
                        memory =  worker_node["Memory"]
                    except:
                        memory =  0
                    memory_free = memory
                    keywords['hostname'] = TypedClass.auto(name)
                    queues = ["default"]
                    keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])
                    nodeinfolist[name] = NodeInfo(name, slots, slots_free, memory, memory_free, keywords)
                    nodeinfolist[name].state = NodeInfo.IDLE
                elif activity != "undefined":
                    try:
                        name = worker_node["Name"]
                    except:
                        name = ""
                    try:
                        slots = worker_node["TotalSlots"]
                    except:
                        slots = 0
                    slots_free = slots
                    try:
                        memory =  worker_node["Memory"]
                    except:
                        memory =  0
                    memory_free = memory
                    keywords['hostname'] = TypedClass.auto(name)
                    try:
                        schedulers = collector.locateAll(htcondor.DaemonTypes.Schedd)
                    except: 
                        schedulers = []
                    if len(schedulers) > 0:
                        for scheduler in schedulers:
                            jobs_scheduled = htcondor.Schedd(scheduler)
                            jobs_scheduled_attributes = jobs_scheduled.query()
                            if len(jobs_scheduled_attributes) > 0:
                                for job_scheduled_attributes in jobs_scheduled_attributes:
                                    nodes = []
                                    try:
                                        nodes = job_scheduled_attributes["AllRemoteHosts"].split(",")
                                    except:
                                        try: 
                                            nodes = [job_scheduled_attributes["RemoteHost"]]
                                        except:
                                            nodes = []
                                    if name in nodes:
                                        cpus = 0
                                        try:
                                            cpus = job_scheduled_attributes["RequestCpus"]
                                        except: 
                                            cpus =  0
                                        slots_free -= cpus
                                        mem = 0
                                        try:
                                            mem =  (job_scheduled_attributes["ImageSize"] + 1023)/1024
                                        except: 
                                            mem =  0
                                        memory_free -= mem
                    queues = ["default"]
                    if len(queues) > 0:
                        keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])
                    if slots_free < 0:
                        slots_free = 0
                    if memory_free < 0:
                        memory_free = 0
                    nodeinfolist[name] = NodeInfo(name , slots , slots_free , memory , memory_free, keywords)
                    nodeinfolist[name].state = NodeInfo.USED
                else:
                    _LOGGER.warning("could not obtain information about nodes.")
                    return None                    
        else:
            try:
                infile = open('/etc/clues2/condor_vnodes.info', 'r')
                for line in infile:
                    name = line.rstrip('\n')
                    # Illustrative values for Clues, since the node is not running, we cannot know the real values
                    slots_count = 1
                    slots_free = 1
                    memory_total = 1572864000
                    memory_free = 1572864000
                    # Create a fake queue
                    keywords = {}
                    keywords['hostname'] = TypedClass.auto(name)
                    queues = ["default"]
                    keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])
                    nodeinfolist[name] = NodeInfo(name, slots_count, slots_free, memory_total, memory_free, keywords)
                    nodeinfolist[name].state = NodeInfo.OFF
                infile.close()
            except:
                _LOGGER.warning("could not obtain information about nodes.")
                return None
        return nodeinfolist
    
    def get_jobinfolist(self):
        jobinfolist = []
        schedulers = []
        collector = htcondor.Collector()
        try: 
            schedulers = collector.locateAll(htcondor.DaemonTypes.Schedd)
        except: 
            schedulers = []
        if len(schedulers) > 0:
            for scheduler in schedulers:
                jobs_scheduled = htcondor.Schedd(scheduler)
                jobs_scheduled_attributes = jobs_scheduled.query()
                if len(jobs_scheduled_attributes) > 0:
                    for job_scheduled_attributes in jobs_scheduled_attributes:
                        cpus_per_task = 0.0
                        try:
                            cpus_per_task = float(job_scheduled_attributes["RequestCpus"])
                        except: 
                            cpus_per_task =  0.0
                        memory = 0
                        try:
                            memory =  (job_scheduled_attributes["ImageSize"] + 1023)/1024
                        except: 
                            memory =  0
                        queue = '"default" in queues'
                        nodes = []
                        numnodes = 0
                        try:
                            nodes = job_scheduled_attributes["AllRemoteHosts"].split(",")
                            numnodes = len(nodes)
                        except:
                            try: 
                                nodes = [job_scheduled_attributes["RemoteHost"]]
                                numnodes = 1
                            except:
                                nodes = []
                                numnodes = job_scheduled_attributes["MinHosts"]    
                        job_id = ""
                        try:
                            cluster_id = job_scheduled_attributes["ClusterId"]
                            proc_id = job_scheduled_attributes["ProcId"]
                            job_id = str(cluster_id) + "." + str(proc_id)
                        except: 
                            job_id = ""
                        state = "" 
                        try: 
                            job_st = job_scheduled_attributes["JobStatus"]
                            state = infer_clues_job_state(job_st)
                        except:
                            state = clueslib.request.Request.PENDING
                        resources = clueslib.request.ResourcesNeeded(cpus_per_task, memory, [queue], numnodes)
                        j = clueslib.request.JobInfo(resources, job_id, nodes)
                        j.set_state(state) 
                        jobinfolist.append(j)
        else:
            _LOGGER.warning("could not obtain information about jobs.")
            return None
        return jobinfolist

if __name__ == '__main__':
    pass
