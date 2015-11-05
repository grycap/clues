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


_LOGGER = logging.getLogger("[PLUGIN-MESOS]")

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

        
#Funcion para obtener la lista de trabajos e identificar los nodos que estan USED (trabajos en estado TASK_RUNNING)
def obtain_used_nodes():
    exit = " "
    used_nodes = []
    try:
        exit = run_command("/usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/tasks.json".split(" "))
        json_data = json.loads(exit)
    except:
        _LOGGER.error("could not obtain information about MESOS jobs (%s)" % (exit))
        return None

    if json_data:
        for job, details in json_data.items():
            for element in details:
                state = str(element['state'])
                if state == "TASK_RUNNING":
                    used_nodes.append(str(element['slave_id']))
    return used_nodes

#Funcion para obtener la memoria y la cpu usadas por el posible trabajo en ejecucion en el nodo
def obtain_cpu_mem_used(node_id):
    used_cpu = 0
    used_mem = 0
    try:
        exit = run_command("/usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/tasks.json".split(" "))
        json_data = json.loads(exit)
    except:
        _LOGGER.error("could not obtain information about MESOS jobs (%s)" % (exit))
        return None

    if json_data:
        for job, details in json_data.items():
            for element in details:
                if str(element['slave_id']) == node_id and str(element['state']) == "TASK_RUNNING":
                    used_cpu = float(element['resources']['cpus'])
                    used_mem = _translate_mem_value(str(element['resources']['mem']) + ".MB")
    
    return used_cpu, used_mem

        
#Funcion que pasa del estado de los slaves MESOS al equivalente estado CLUES2
def infer_clues_node_state(id, state, used_nodes):
    # Estados MESOS: active=true || active=false
    # Estados CLUES2: ERROR, UNKNOWN, IDLE, USED, OFF
    #TODO: como se si esta idle or used? tendre que mirar si hay procesos running en ellos
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

#Funcion que pasa del estado de los trabajos MESOS al equivalente estado CLUES2
def infer_clues_job_state(state):
    # Estados MESOS: TASK_RUNNING, TASK_PENDING, TASK_KILLED, TASK_FINISHED (no se si hay mas)
    # Estados CLUES: ATTENDED o PENDING
    res_state = ""

    if state == 'TASK_PENDING':
        res_state = clueslib.request.Request.PENDING
    else:
        res_state = clueslib.request.Request.ATTENDED

    return res_state

class lrms(clueslib.platform.LRMS):

    def __init__(self, MESOS_SERVER = None, MESOS_NODES_COMMAND = None,  MESOS_STATE_COMMAND = None, MESOS_JOBS_COMMAND = None): 

        import cpyutils.config
        config_mesos = cpyutils.config.Configuration(
            "MESOS",
            {
                "MESOS_SERVER": "mesosserverpublic", 
                "MESOS_NODES_COMMAND": "/usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/slaves",
                "MESOS_STATE_COMMAND": "/usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/state.json",
                "MESOS_JOBS_COMMAND": "/usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/tasks.json"
                #"MESOS_CHRONOS_COMMAND":"/usr/bin/curl -L -X GET http://mesosserverpublic:4400/scheduler/jobs"
            }
        )
        
        self._server_ip = clueslib.helpers.val_default(MESOS_SERVER, config_mesos.MESOS_SERVER)
        _nodes_cmd = clueslib.helpers.val_default(MESOS_NODES_COMMAND, config_mesos.MESOS_NODES_COMMAND)
        self._nodes = _nodes_cmd.split(" ")
        _state_cmd = clueslib.helpers.val_default(MESOS_STATE_COMMAND, config_mesos.MESOS_STATE_COMMAND)
        self._state = _state_cmd.split(" ")
        _jobs_cmd = clueslib.helpers.val_default(MESOS_JOBS_COMMAND, config_mesos.MESOS_JOBS_COMMAND)
        self._jobs = _jobs_cmd.split(" ")
        #_chronos_cmd = clueslib.helpers.val_default(MESOS_CHRONOS_COMMAND, config_mesos.MESOS_CHRONOS_COMMAND)
        #self._chronos = _chronos_cmd.split(" ")
        clueslib.platform.LRMS.__init__(self, "SLURM_%s" % self._server_ip)

    def get_nodeinfolist(self): 
        nodeinfolist = {}
        
        '''Ejemplo de salida /usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/slaves
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

        exit = " "
        try:
            exit = run_command(self._nodes)
            json_data = json.loads(exit)
            infile = open('/tmp/vnodes.info', 'r')
        except:
            _LOGGER.error("could not obtain information about MESOS nodes %s (%s)" % (self._server_ip, exit))
            return None

        for line in infile:
            #name = line[:-1]
            name = line.rstrip('\n')
            #name = line
            state = NodeInfo.OFF
            #Valores orientativos para CLUES, hasta que no este el nodo encendido, no se conocen los valores exactos
            slots_count = 1
            memory_total = 1572864000
            slots_free = 1
            memory_free = 1572864000

            # Creamos una cola ficticia
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
                used_nodes = obtain_used_nodes()
                for element in details:
                    name = element['hostname']
                    for node in nodeinfolist:
                        if name == nodeinfolist[node].name:
                            state = infer_clues_node_state(name, element["active"], used_nodes)
                            slots_count = float(element['resources']['cpus'])
                            memory_total = _translate_mem_value(str(element['resources']['mem']) + ".MB")
                            
                            used_cpu, used_mem = obtain_cpu_mem_used(name)
                            slots_free = slots_count - used_cpu
                            memory_free = memory_total - used_mem

                            # Creamos una cola ficticia
                            keywords = {}
                            queues = ["default"]
                            keywords['hostname'] = TypedClass.auto(name)
                            if len(queues) > 0:
                                keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])
                            
                            nodeinfolist[name] = NodeInfo(name, slots_count, slots_free, memory_total, memory_free, keywords)
                            nodeinfolist[name].state = state


        return nodeinfolist

    # Metodo encargado de monitorizar la cola de trabajos
    # La info hay que obtenerla desde los frameworks y no desde los tasks, 
    # porque si no hay recursos disponibles los task no se crean pero si los frameworks
    def get_jobinfolist(self):

        '''Ejemplo de salida con /usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/tasks.json:
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

        # procesado de la salida del comando de chronos
        '''if json_data:
            for job, details in json_data.items():
                    job_id = details['name']
                    state = clueslib.request.Request.PENDING
                    nodes = []
                    numnodes = 1;
                    memory = _translate_mem_value(str(details['mem']) + ".MB")
                    cpus_per_task = float(details['cpus'])
                    # Usamos la cola ficticia
                    queue = '"default" in queues'
                    
                    resources = clueslib.request.ResourcesNeeded(cpus_per_task, memory, [queue], numnodes)
                    j = clueslib.request.JobInfo(resources, job_id, nodes)
                    j.set_state(state)
                    jobinfolist.append(j)'''
        
        # procesado de la salida del comando de trabajos de mesos
        '''if json_data:
            for job, details in json_data.items():
                for element in details:
                    job_id = element['id']
                    state = infer_clues_job_state(str(element['state']))
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
                    
        # procesado de la salida del comando de estado del sistema de mesos 
        if json_data:
            for details in json_data.items():
                if details[0] == "frameworks":
                    frameworks = details[1]
                    if len(frameworks) != 0:
                        for f in frameworks:
                            if(f[name] != "chronos-2.4.0"):
                                job_id = f['id']
                                nodes = []
                                numnodes = 1;
                                tasks = f['tasks']
                                state = clueslib.request.Request.PENDING
                                memory = _translate_mem_value(str(f['resources']['mem']) + ".MB")
                                cpus_per_task = float(f['resources']['cpus'])
                                if len(tasks) != 0:
                                    for t in tasks:
                                        state = infer_clues_job_state(str(t['state']))
                                        #para el nodo, aqui solo tenemos info del id, nos hace falta el hostname (CREO)
                                        #nodes.append(str(t['slave_id']))
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
                                                    
                        # Usamos la cola ficticia
                        queue = '"default" in queues'

                        resources = clueslib.request.ResourcesNeeded(cpus_per_task, memory, [queue], numnodes)
                        j = clueslib.request.JobInfo(resources, job_id, nodes)
                        j.set_state(state)
                        jobinfolist.append(j)
        
        return jobinfolist
        
if __name__ == '__main__':
    pass
