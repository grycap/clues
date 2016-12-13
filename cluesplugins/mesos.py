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
import json

from clueslib.node import NodeInfo
from cpyutils.evaluate import TypedClass, TypedList


import cpyutils.log
_LOGGER = cpyutils.log.Log("PLUGIN-MESOS")

'''def _translate_mem_value(memval):
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
            
    return value * multiplier'''

def run_command(command):
    try:
        p=subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (out, err) = p.communicate()
        if p.returncode != 0:
            raise Exception("return code: %d\nError output: %s" % (p.returncode, err))
        return out
    except Exception as e:
        raise Exception("Error executing '%s': %s" % (" ".join(command), str(e)))


class lrms(clueslib.platform.LRMS):

    # Obtains the list of jobs and identifies the nodes that are in "USED" state (jobs in state "TASK_RUNNING")
    def _obtain_used_nodes(self):
        used_nodes = []
        output = ""
        try:
            output = run_command(self._jobs)
            json_data = json.loads(output)
        except:
            _LOGGER.error("could not obtain information about MESOS jobs (%s)" % (output))
            return []
    
        if json_data:
            for _, details in json_data.items():
                for element in details:
                    state = str(element['state'])
                    if state == "TASK_RUNNING" or state == "TASK_STAGING":
                        used_nodes.append(str(element['slave_id']))
        return used_nodes
    
    # Obtains the mem and cpu used by the possible job that is in execution in the node "node_id"
    def _obtain_cpu_mem_used(self, node_id):
        used_cpu = 0
        used_mem = 0
        output = ""
        try:
            output = run_command(self._jobs)
            json_data = json.loads(output)
        except:
            _LOGGER.error("could not obtain information about MESOS jobs (%s)" % (output))
            return None, None
    
        if json_data:
            for _, details in json_data.items():
                for element in details:
                    if str(element['slave_id']) == node_id and str(element['state']) == "TASK_RUNNING":
                        used_cpu = float(element['resources']['cpus'])
                        used_mem = element['resources']['mem'] * 1048576
        
        return used_cpu, used_mem
    
            
    # Determines the equivalent state between Mesos slaves and Clues2 possible node states
    @staticmethod
    def _infer_clues_node_state(id, state, used_nodes):
        # MESOS node states: active=true || active=false
        # CLUES2 node states: ERROR, UNKNOWN, IDLE, USED, OFF
    
        res_state = ""
    
        if state and id not in used_nodes:
            res_state = NodeInfo.IDLE
        elif state and id in used_nodes:
            res_state = NodeInfo.USED
        elif not state:
            res_state = NodeInfo.OFF
        else:
            res_state = NodeInfo.UNKNOWN
        
        return res_state
    
    # Determines the equivalent state between Mesos tasks and Clues2 possible job states
    @staticmethod
    def _infer_clues_job_state(state):
        # MESOS job states: TASK_RUNNING, TASK_PENDING, TASK_KILLED, TASK_FINISHED (TODO: check if there are more possible states)
        # CLUES2 job states: ATTENDED o PENDING
        res_state = ""
    
        if state == 'TASK_PENDING':
            res_state = clueslib.request.Request.PENDING
        else:
            res_state = clueslib.request.Request.ATTENDED
    
        return res_state
    
    # Determines the equivalent state between Chronos jobs and Clues2 possible job states
    @staticmethod
    def _infer_chronos_state(state):
        # CHRONOS job states: idle,running,queued,failed,started,finished,disabled,skipped,expired,removed (TODO: check if there are more possible states)
        # CLUES2 job states: ATTENDED o PENDING
        res_state = ""
        if state == 'queued': # or state == 'idle':
            res_state = clueslib.request.Request.PENDING
        else:
            res_state = clueslib.request.Request.ATTENDED
        return res_state
    
    # Method to obtain the slave where the chronos job is executed
    def _obtain_chronosjob_node(self, job_id):
        output = " "
        nodes = []
        try:
            output = run_command(self._jobs)
            json_data = json.loads(output)
        except:
            _LOGGER.warning("could not obtain information about Mesos tasks (%s)" % (output))
            return []
    
        job = "ChronosTask:" + job_id
        if json_data:
            for job, details in json_data.items():
                for element in details:
                    jobname = str(element['name'])
                    if jobname == job:
                        node_id = str(element['slave_id'])
                        try:
                            output = run_command(self._nodes)
                            json_data2 = json.loads(output)
                        except:
                            _LOGGER.warning("could not obtain information about MESOS nodes (%s)" % (output))
                        return None
                        if json_data2:
                            for _, det in json_data2.items():
                                for element in det:
                                    if element['id'] == node_id:
                                        nodes.append(element['hostname'])
                        
        return nodes

    # Method in charge of monitoring the job queue of Chronos
    def _get_chronos_jobinfolist(self):
        output = " "
        jobinfolist = []
        try:
            output = run_command(self._chronos)
            json_data = json.loads(output)
        except:
            _LOGGER.warning("could not obtain information about Chronos %s (%s)" % (self._server_ip, output))
            return []

        # process the exit of the chronos command 
        if json_data:
            for job in json_data:
                job_id = job['name']
                # When the job is running, the name received in mesos is "ChronosTask:<chronosJobName>"
                # TODO: call Mesos: hacer un metodo que para un nombre de trabajo devuelva el nodo en el que se ejecuta
                nodes = self._obtain_chronosjob_node(job_id)
                #nodes = []
                state = ""
                numnodes = 1;
                memory = job['mem']* 1048576
                if memory <= 0:
                    memory = 536870912
                cpus_per_task = float(job['cpus'])
                # Use the fake queue
                queue = '"default" in queues'
                # Ask chronos the current state of the job <name>
                # We obtain something like "type,jobName,lastRunStatus,currentState" for each job
                try:
                    exit2 = run_command(self._chronos_state)
                    exit2 = exit2.split("\n")
                    for e in exit2:
                        if e != '':
                            exit2_split = e.split(",")
                            if exit2_split[1] == job_id:
                                state = self._infer_chronos_state(exit2_split[3])
                except:
                    _LOGGER.warning("could not obtain information about CHRONOS job state %s (%s)" % (self._server_ip, exit))
                    return None
                    
                resources = clueslib.request.ResourcesNeeded(cpus_per_task, memory, [queue], numnodes)
                j = clueslib.request.JobInfo(resources, job_id, nodes)
                j.set_state(state)
                jobinfolist.append(j)
            
        return jobinfolist

    # Method in charge of monitoring the job queue of Marathon
    def _get_marathon_jobinfolist(self):
        exit = " "
        jobinfolist = []
        try:
            exit = run_command(self._marathon)
            json_data = json.loads(exit)
        except:
            _LOGGER.warning("could not obtain information about Marathon status %s (%s)" % (self._server_ip, exit))
            return []
                    
        # process the exit of the marathon command
        if json_data:
            for details in json_data.items():
                if details[0] == "apps":
                    apps = details[1]
                    if len(apps) != 0:
                        for a in apps:
                            job_id = a['id']
                            memory = a['mem'] * 1048576
                            if memory <= 0:
                                memory = 536870912
                            cpus_per_task = float(a['cpus'])
                            if cpus_per_task <= 0:
                                cpus_per_task = 1
                            nodes = []
                            numnodes = a['instances'];
                            tasks = a['tasks']
                            if(a['tasksRunning'] > 0 and len(tasks) != 0):
                                state = clueslib.request.Request.ATTENDED
                            else:
                                state = clueslib.request.Request.PENDING
                            if len(tasks) != 0:
                                for t in tasks:
                                    nodes.append(t['host'])
                                                
                            # Use the fake queue
                            queue = '"default" in queues'

                            resources = clueslib.request.ResourcesNeeded(cpus_per_task, memory, [queue], numnodes)
                            j = clueslib.request.JobInfo(resources, job_id, nodes)
                            j.set_state(state)
                            jobinfolist.append(j)
        
        return jobinfolist

    def __init__(self, MESOS_SERVER = None, MESOS_NODES_COMMAND = None,  MESOS_STATE_COMMAND = None, MESOS_JOBS_COMMAND = None, MESOS_MARATHON_COMMAND = None, MESOS_CHRONOS_COMMAND = None, MESOS_CHRONOS_STATE_COMMAND = None): 

        import cpyutils.config
        config_mesos = cpyutils.config.Configuration(
            "MESOS",
            {
                "MESOS_SERVER": "mesosserverpublic", 
                "MESOS_NODES_COMMAND": "/usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/slaves",
                "MESOS_STATE_COMMAND": "/usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/state.json",
                "MESOS_JOBS_COMMAND": "/usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/tasks.json",
                "MESOS_MARATHON_COMMAND": "/usr/bin/curl -L -X GET http://mesosserverpublic:8080/v2/apps?embed=tasks",
                "MESOS_CHRONOS_COMMAND":"/usr/bin/curl -L -X GET http://mesosserverpublic:4400/scheduler/jobs",
                "MESOS_CHRONOS_STATE_COMMAND":"/usr/bin/curl -L -X GET http://mesosserverpublic:4400/scheduler/graph/csv"
            }
        )
        
        self._server_ip = clueslib.helpers.val_default(MESOS_SERVER, config_mesos.MESOS_SERVER)
        _nodes_cmd = clueslib.helpers.val_default(MESOS_NODES_COMMAND, config_mesos.MESOS_NODES_COMMAND)
        self._nodes = _nodes_cmd.split(" ")
        _state_cmd = clueslib.helpers.val_default(MESOS_STATE_COMMAND, config_mesos.MESOS_STATE_COMMAND)
        self._state = _state_cmd.split(" ")
        _jobs_cmd = clueslib.helpers.val_default(MESOS_JOBS_COMMAND, config_mesos.MESOS_JOBS_COMMAND)
        self._jobs = _jobs_cmd.split(" ")
        _marathon_cmd = clueslib.helpers.val_default(MESOS_MARATHON_COMMAND, config_mesos.MESOS_MARATHON_COMMAND)
        self._marathon = _marathon_cmd.split(" ")
        _chronos_cmd = clueslib.helpers.val_default(MESOS_CHRONOS_COMMAND, config_mesos.MESOS_CHRONOS_COMMAND)
        self._chronos = _chronos_cmd.split(" ")
        _chronos_state_cmd = clueslib.helpers.val_default(MESOS_CHRONOS_STATE_COMMAND, config_mesos.MESOS_CHRONOS_STATE_COMMAND)
        self._chronos_state = _chronos_state_cmd.split(" ")
        clueslib.platform.LRMS.__init__(self, "MESOS_%s" % self._server_ip)

    def get_nodeinfolist(self): 
        nodeinfolist = {}
        
        '''Exit example of /usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/slaves
        {
            "slaves": [
                {
                    "active": true,
                    "attributes": {},
                    "hostname": "10.0.0.84",
                    "id": "20150716-115932-1063856798-5050-14165-S0",
                    "pid": "slave(1)@10.0.0.84:5051",
                    "registered_time": 1437487335.75923,
                    "reregistered_time": 1437487335.75927,
                    "resources": {
                        "cpus": 1,
                        "disk": 13438,
                        "mem": 623,
                        "ports": "[31000-32000]"
                    }
                }
            ]
        }'''

        output = " "
        try:
            output = run_command(self._nodes)
            json_data = json.loads(output)
            infile = open('/etc/clues2/mesos_vnodes.info', 'r')
        except:
            _LOGGER.error("could not obtain information about MESOS nodes %s (%s)" % (self._server_ip, output))
            return None

        for line in infile:
            #name = line[:-1]
            name = line.rstrip('\n')
            #name = line
            state = NodeInfo.OFF
            # Illustrative values for Clues, since the node is not running, we cannot know the real values
            slots_count = 1
            memory_total = 1572864000
            slots_free = 1
            memory_free = 1572864000

            # Create a fake queue
            keywords = {}
            queues = ["default"]
            keywords['hostname'] = TypedClass.auto(name)
            if len(queues) > 0:
                keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])
                    
            nodeinfolist[name] = NodeInfo(name, slots_count, slots_free, memory_total, memory_free, keywords)
            nodeinfolist[name].state = state
        infile.close()

        if json_data:
            for node, details in json_data.items():
                used_nodes = self._obtain_used_nodes()
                for element in details:
                    name = element['hostname']
                    for node in nodeinfolist:
                        if name == nodeinfolist[node].name:
                            state = self._infer_clues_node_state(element["id"], element["active"], used_nodes)
                            slots_count = float(element['resources']['cpus'])
                            memory_total = element['resources']['mem'] * 1048576
                            
                            used_cpu, used_mem = self._obtain_cpu_mem_used(element["id"])
                            slots_free = slots_count - used_cpu
                            memory_free = memory_total - used_mem

                            # Create a fake queue
                            keywords = {}
                            queues = ["default"]
                            keywords['hostname'] = TypedClass.auto(name)
                            if len(queues) > 0:
                                keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])
                            
                            nodeinfolist[name] = NodeInfo(name, slots_count, slots_free, memory_total, memory_free, keywords)
                            nodeinfolist[name].state = state


        return nodeinfolist

    # Method in charge of monitoring the job queue of Mesos plus Marathon
    # The Mesos info about jobs has to be obtained from frameworks and not from tasks, 
    # because if there are not available resources to execute new tasks, Mesos do not create them but frameworks are created
    def get_jobinfolist(self):

        '''Exit example of /usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/tasks.json:
        {
            "tasks": [
                {
                    "executor_id": "",
                    "framework_id": "20150716-115932-1063856798-5050-14165-0000",
                    "id": "ct:1437487440000:0:dockerjob:",
                    "labels": [],
                    "name": "ChronosTask:dockerjob",
                    "resources": {
                        "cpus": 0.5,
                        "disk": 256,
                        "mem": 512
                    },
                    "slave_id": "20150716-115932-1063856798-5050-14165-S0",
                    "state": "TASK_FINISHED",
                    "statuses": [
                        {
                            "state": "TASK_RUNNING",
                            "timestamp": 1437487453.65378
                        },
                        {
                            "state": "TASK_FINISHED",
                            "timestamp": 1437487463.59986
                        }
                    ]
                }
            ]
        }'''

        exit = " "
        jobinfolist = []

        try:
            #exit = run_command(self._chronos)
            #exit = run_command(self._jobs)
            exit = run_command(self._state)
            json_data = json.loads(exit)
        except:
            _LOGGER.error("could not obtain information about MESOS status %s (%s)" % (self._server_ip, exit))
            return None
        
        # process the exit of the mesos job command 
        '''if json_data:
            for job, details in json_data.items():
                for element in details:
                    job_id = element['id']
                    state = self._infer_clues_job_state(str(element['state']))
                    nodes = []
                    nodes.append(str(element['slave_id']))
                    numnodes = 1;
                    memory = _translate_mem_value(str(element['resources']['mem']) + ".MB")
                    cpus_per_task = float(element['resources']['cpus'])
                    # Usamos la cola ficticia
                    queue = '"default" in queues'

                    resources = clueslib.request.ResourcesNeeded(cpus_per_task, memory, [queue], numnodes)
                    j = clueslib.request.JobInfo(resources, job_id, nodes)
                    j.set_state(state)
                    jobinfolist.append(j)'''
                    
        # process the exit of the mesos state of the system command 
        if json_data:
            for details in json_data.items():
                if details[0] == "frameworks":
                    frameworks = details[1]
                    if len(frameworks) != 0:
                        for f in frameworks:
                            if(f['name'] != "chronos-2.4.0" and f['name'] != "marathon"):
                                job_id = f['id']
                                nodes = []
                                numnodes = 1;
                                tasks = f['tasks']
                                state = clueslib.request.Request.PENDING
                                memory = f['resources']['mem'] * 1048576
                                if memory <= 0:
                                    memory = 536870912
                                cpus_per_task = float(f['resources']['cpus'])
                                if cpus_per_task <= 0:
                                    cpus_per_task = 1
                                if len(tasks) != 0:
                                    for t in tasks:
                                        state = self._infer_clues_job_state(str(t['state']))
                                        node_id = str(t['slave_id'])
                                        try:
                                            exit2 = run_command(self._nodes)
                                            json_data2 = json.loads(exit2)
                                        except:
                                            _LOGGER.error("could not obtain information about MESOS jobs %s (%s)" % (self._server_ip, exit))
                                        return None
                                        
                                        if json_data2:
                                            for node, det in json_data2.items():
                                                for element in det:
                                                    if element['id'] == node_id:
                                                        nodes.append(element['hostname'])
                                                    
                                # Use the fake queue
                                queue = '"default" in queues'

                                resources = clueslib.request.ResourcesNeeded(cpus_per_task, memory, [queue], numnodes)
                                j = clueslib.request.JobInfo(resources, job_id, nodes)
                                j.set_state(state)
                                jobinfolist.append(j)

        # Obtain Marathon jobs and add to the jobinfolist of Mesos
        jobinfolist2 = self._get_marathon_jobinfolist();
        if(jobinfolist2 != None and len(jobinfolist2) > 0):
            jobinfolist = list(set(jobinfolist + jobinfolist2))

        #Obtain chronos jobs and add them to jobinfolist
        jobinfolist3 = self._get_chronos_jobinfolist();   
        if(jobinfolist3 != None and len(jobinfolist3) > 0):
            jobinfolist = list(set(jobinfolist + jobinfolist3))
        
        return jobinfolist
        
if __name__ == '__main__':
    pass
