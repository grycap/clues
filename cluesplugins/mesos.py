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

import socket
import subprocess
import json
import requests
import cpyutils.config
import clueslib.helpers as Helpers

from cpyutils.evaluate import TypedClass, TypedList
from cpyutils.log import Log
from cpyutils.runcommand import runcommand
from clueslib.node import NodeInfo
from clueslib.platform import LRMS
from clueslib.request import Request, ResourcesNeeded, JobInfo
import collections

_LOGGER = Log("PLUGIN-MESOS")


def curl_command(command, server_ip, error_message, is_json=True, timeout=10):
    result = None
    try:
        success, result = runcommand(command, True, timeout)
        if success:
            if result:
                if is_json:
                    return json.loads(result)
                else:
                    return result
        else:
            raise Exception("Command return code is not 0.")
    except Exception as exception:
        message = str(exception) + ';ERROR=' + error_message.rstrip('\n') + ';SERVER_IP=' + server_ip.rstrip('\n')
        if result:
            message += ';COMMAND_OUTPUT=' + result.rstrip('\n') + ''
        _LOGGER.error(message)


def infer_mesos_job_state(job_state):
    ''' Determines the equivalent node_state between Mesos tasks and Clues2 possible job states
    MESOS job states: TASK_RUNNING, TASK_PENDING, TASK_KILLED, TASK_FINISHED
    (TODO: check if there are more possible MESOS job states)
    CLUES2 job states: ATTENDED o PENDING
    '''
    return Request.PENDING if job_state == 'TASK_PENDING' else Request.ATTENDED


def infer_chronos_job_state(job_state):
    ''' Determines the equivalent job_state between Chronos jobs and Clues2 possible job states
    CHRONOS job states: idle,running,queued,failed,started,finished,disabled,skipped,expired,removed
    (TODO: check if there are more possible CHRONOS job states)
    CLUES2 job states: ATTENDED o PENDING
    '''
    return Request.PENDING if job_state == 'queued' else Request.ATTENDED


def infer_marathon_job_state(numnodes, jobs, jobs_running):
    ''' Determines the equivalent state between Marathon jobs and Clues2 possible job states'''
    return Request.ATTENDED if jobs and jobs_running >= numnodes else Request.PENDING


def calculate_memory_bytes(memory):
    return memory * 1048576

def get_memory_in_bytes(str_memory):
    if str_memory.strip()[-2:] in ['Mi', 'Gi', 'Ki', 'Ti']:
        unit = str_memory.strip()[-2:][1]
        memory = int(str_memory.strip()[:-2])
    elif str_memory.strip()[-1:] in ['M', 'G', 'K', 'T']:
        unit = str_memory.strip()[-1:]
        memory = int(str_memory.strip()[:-1])
    else:
        return int(str_memory)

    if unit == 'K':
        memory *= 1024
    elif unit == 'M':
        memory *= 1024 * 1024
    elif unit == 'G':
        memory *= 1024 * 1024 * 1024
    elif unit == 'T':
        memory *= 1024 * 1024 * 1024 * 1024
    return memory


def open_file(file_path):
    try:
        file_read = open(file_path, 'r')
    except:
        message = "Could not open file with path '%s'" % file_path
        _LOGGER.error(message)
        raise Exception(message)
    return file_read


class lrms(LRMS):

    def _obtain_mesos_jobs(self):
        '''Obtains the list of jobs in Mesos'''
        return curl_command(self._jobs, self._server_ip, "Could not obtain information about MESOS jobs")

    def _obtain_mesos_nodes(self):
        '''Obtains the list of nodes in Mesos'''
        return curl_command(self._nodes, self._server_ip, "Could not obtain information about MESOS nodes")

    def _obtain_chronos_jobs(self):
        '''Obtains the list of jobs in Chronos'''
        return curl_command(self._chronos, self._server_ip, "Could not obtain information about Chronos jobs")

    def _obtain_chronos_jobs_state(self):
        '''Obtains the list of states for the jobs in Chronos'''
        return curl_command(self._chronos_state, self._server_ip,
                            "Could not obtain information about the state of the Chronos jobs", False)

    def _obtain_marathon_jobs(self):
        '''Obtains the list of jobs in Marathon'''
        return curl_command(self._marathon, self._server_ip, "Could not obtain information about Marathon jobs")

    def _obtain_mesos_state(self):
        '''Obtains the state of the Mesos server'''
        return curl_command(self._state, self._server_ip, "Could not obtain information about MESOS state")

    def _obtain_chronos_jobs_nodes(self, job_id):
        '''Method to obtain the slaves' hostnames that are executing chronos jobs'''
        mesos_jobs = self._obtain_mesos_jobs()
        chronos_nodes_hostname = []

        chronos_job_name = "ChronosTask:" + job_id
        if mesos_jobs:
            for mesos_job in mesos_jobs['tasks']:
                if chronos_job_name == mesos_job['name']:
                    mesos_nodes = self._obtain_mesos_nodes()
                    if mesos_nodes:
                        for mesos_node in mesos_nodes['slaves']:
                            if mesos_node['id'] == mesos_job['slave_id']:
                                chronos_nodes_hostname.append(mesos_node['hostname'])

        return chronos_nodes_hostname

    def _obtain_chronos_job_state(self, job_id):
        '''Given a job id, calls Chronos to know the state of that job'''
        chronos_jobs = self._obtain_chronos_jobs_state()
        if chronos_jobs:
            parsed_chronos_jobs = chronos_jobs.split("\n")
            for chronos_job in parsed_chronos_jobs:
                if chronos_job != '':
                    properties = chronos_job.split(",")
                    # properties[1] --> Job name
                    if job_id == properties[1]:
                        # properties[3] --> Job state
                        return infer_chronos_job_state(properties[3])

    def _update_job_info_list(self, jobinfolist, cpus_per_task, memory, numnodes, job_id, nodes, state):
        # Use the fake queue
        #queue = '"default" in queues'
        queue = ""
        resources = ResourcesNeeded(cpus_per_task, memory, [queue], numnodes)
        job_info = JobInfo(resources, job_id, nodes)
        job_info.set_state(state)
        jobinfolist.append(job_info)
        return jobinfolist

    def _get_chronos_jobinfolist(self):
        '''Method in charge of monitoring the chronos_job queue of Chronos'''
        jobinfolist = []
        chronos_jobs = self._obtain_chronos_jobs()
        if chronos_jobs:
            for chronos_job in chronos_jobs:
                job_id = chronos_job['name']
                # When the chronos_job is running, the name received in mesos is "ChronosTask:<chronosJobName>"
                nodes = self._obtain_chronos_jobs_nodes(job_id)
                numnodes = 1
                memory = calculate_memory_bytes(chronos_job['mem'])
                if memory <= 0:
                    memory = 536870912
                cpus_per_task = float(chronos_job['cpus'])
                # Ask chronos the current chronos_job_state of the chronos_job <name>
                # We obtain something like "type,jobName,lastRunStatus,currentState" for each chronos_job
                chronos_job_state = self._obtain_chronos_job_state(job_id)
                jobinfolist = self._update_job_info_list(jobinfolist,
                                                         cpus_per_task, memory, numnodes,
                                                         job_id, nodes, chronos_job_state)
        return jobinfolist

    def _get_marathon_jobinfolist(self):
        ''' Method in charge of monitoring the chronos_job queue of Marathon '''
        jobinfolist = []
        marathon_jobs = self._obtain_marathon_jobs()

        if marathon_jobs:
            if marathon_jobs['apps']:
                for job_attributes in marathon_jobs['apps']:
                    job_id = job_attributes['id']
                    memory = calculate_memory_bytes(job_attributes['mem'])
                    if memory <= 0:
                        memory = 536870912
                    cpus_per_task = float(job_attributes['cpus'])
                    if cpus_per_task <= 0:
                        cpus_per_task = 1.0
                    tasks = job_attributes['tasks']
                    nodes = []
                    if tasks:
                        for task in tasks:
                            nodes.append(task['host'])
                    numnodes = job_attributes['instances']
                    marathon_job_state = infer_marathon_job_state(numnodes, tasks, job_attributes['tasksRunning'])
                    jobinfolist = self._update_job_info_list(jobinfolist,
                                                             cpus_per_task, memory, numnodes,
                                                             job_id, nodes, marathon_job_state)
        return jobinfolist

    @staticmethod
    def _unit_to_value(unit):
        """Return the value of an unit."""

        if not unit:
            return 1
        unit = unit[0].upper()
        if unit == "K":
            return 1024
        if unit == "M":
            return 1024 * 1024
        if unit == "G":
            return 1024 * 1024 * 1024
        return 1

    def _parse_web_ui_env(self, html_code):
        """
        Parse the HTML code of the env page to the get the spark cpus
        """
        # Find <td>spark.cores.max</td><td>2</td>
        cpus = 0
        try:
            ini = html_code.find("<td>spark.cores.max</td><td>")
            if ini >= 0:
                ini += 28
                end = html_code.find("</td>", ini)
                cpus = int(html_code[ini:end])
        except Exception as ex:
            _LOGGER.error("Error getting Spark cpus: %s" % str(ex))

        # Find <td>spark.executor.memory</td><td>512M</td>
        memory = 0
        try:
            ini = html_code.find("<td>spark.executor.memory</td><td>")
            if ini >= 0:
                ini += 34
                end = html_code.find("</td>", ini)
                memory = int(html_code[ini:end-1])
                memory_unit = html_code[ini:end][-1]
                memory *= self._unit_to_value(memory_unit)
        except Exception as ex:
            _LOGGER.error("Error getting Spark memory: %s" % str(ex))

        return cpus, memory

    def _get_spark_resources(self, webui_url):
        """
        Get the number of CPUs of the Spark job from the Web UI
        """
        try:
            # TODO: check api 2.2.1 /api/v1//applications/[app-id]/environment
            resp = requests.get("%s/environment" % webui_url, verify=False)
            if resp.status_code == 200:
                return self._parse_web_ui_env(resp.text)
            else:
                _LOGGER.error("Error querying the Spark Web UI: %s" % resp.reason)
                return 0, 0
        except Exception as ex:
            _LOGGER.error("Error getting Spark Resources: %s" % str(ex))
            return 0, 0

    def __init__(self, MESOS_SERVER=None, MESOS_NODES_COMMAND=None, MESOS_STATE_COMMAND=None, MESOS_JOBS_COMMAND=None,
                 MESOS_MARATHON_COMMAND=None, MESOS_CHRONOS_COMMAND=None, MESOS_CHRONOS_STATE_COMMAND=None,
                 MESOS_NODE_MEMORY=None, MESOS_NODE_SLOTS=None):

        config_mesos = cpyutils.config.Configuration(
            "MESOS",
            {
                "MESOS_SERVER":
                "mesosserverpublic",
                "MESOS_NODES_COMMAND":
                "/usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/slaves",
                "MESOS_STATE_COMMAND":
                "/usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/state.json",
                "MESOS_JOBS_COMMAND":
                "/usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/tasks.json?limit=1000",
                "MESOS_MARATHON_COMMAND":
                "/usr/bin/curl -L -X GET http://mesosserverpublic:8080/v2/apps?embed=tasks",
                "MESOS_CHRONOS_COMMAND":
                "/usr/bin/curl -L -X GET http://mesosserverpublic:4400/scheduler/jobs",
                "MESOS_CHRONOS_STATE_COMMAND":
                "/usr/bin/curl -L -X GET http://mesosserverpublic:4400/scheduler/graph/csv",
                "MESOS_NODE_MEMORY": 1572864000,
                "MESOS_NODE_SLOTS": 1,
            }
        )

        self._server_ip = Helpers.val_default(MESOS_SERVER, config_mesos.MESOS_SERVER)
        self._nodes = Helpers.val_default(MESOS_NODES_COMMAND, config_mesos.MESOS_NODES_COMMAND)
        self._state = Helpers.val_default(MESOS_STATE_COMMAND, config_mesos.MESOS_STATE_COMMAND)
        self._jobs = Helpers.val_default(MESOS_JOBS_COMMAND, config_mesos.MESOS_JOBS_COMMAND)
        self._marathon = Helpers.val_default(MESOS_MARATHON_COMMAND, config_mesos.MESOS_MARATHON_COMMAND)
        self._chronos = Helpers.val_default(MESOS_CHRONOS_COMMAND, config_mesos.MESOS_CHRONOS_COMMAND)
        self._chronos_state = Helpers.val_default(MESOS_CHRONOS_STATE_COMMAND, config_mesos.MESOS_CHRONOS_STATE_COMMAND)
        self._node_memory = Helpers.val_default(MESOS_NODE_MEMORY, config_mesos.MESOS_NODE_MEMORY)
        self._node_slots = Helpers.val_default(MESOS_NODE_SLOTS, config_mesos.MESOS_NODE_SLOTS)
        LRMS.__init__(self, "MESOS_%s" % self._server_ip)

    def get_nodeinfolist(self):
        nodeinfolist = collections.OrderedDict()
        try:
            vnodes = json.load(open('/etc/clues2/mesos_vnodes.info', 'r'))
            for vnode in vnodes:
                name = vnode["name"]
                if name not in nodeinfolist:
                    keywords = {'hostname': TypedClass(name, TypedClass.STRING)}
                    state = NodeInfo.OFF
                    slots_count = self._node_slots
                    slots_free = self._node_slots
                    if "cpu" in vnode:
                        slots_count = int(vnode["cpu"])
                        slots_free = int(vnode["cpu"])

                    memory_total = self._node_memory
                    memory_free = self._node_memory
                    if "memory" in vnode:
                        memory_total = get_memory_in_bytes(vnode["memory"])
                        memory_free = get_memory_in_bytes(vnode["memory"])
                    #queues = ["default"]
                    #if "queues" in vnode:
                    #    queues = vnode["queues"].split(",")
                    #    if queues:
                    #        keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])

                    if "keywords" in vnode:
                        for keypair in vnode["keywords"].split(','):
                            parts = keypair.split('=')
                            keywords[parts[0].strip()] = TypedClass(parts[1].strip(), TypedClass.STRING)

                    nodeinfolist[name] = NodeInfo(name, slots_count, slots_free, memory_total, memory_free, keywords)
                    nodeinfolist[name].state = state
        except Exception as ex:
            _LOGGER.error("Error processing file /etc/clues2/mesos_vnodes.info: %s" % str(ex))

        mesos_slaves = self._obtain_mesos_nodes()
        if mesos_slaves:
            for mesos_slave in mesos_slaves['slaves']:
                name = mesos_slave['hostname']
                if nodeinfolist:
                    for node in nodeinfolist:
                        nodeinfolist_node_ip = None
                        try:
                            nodeinfolist_node_ip = socket.gethostbyname(nodeinfolist[node].name)
                        except:
                            _LOGGER.warning("Error resolving node ip %s" % nodeinfolist[node].name)
                        if name == nodeinfolist[node].name or name == nodeinfolist_node_ip:
                            name = nodeinfolist[node].name
                            slots_count = float(mesos_slave['resources']['cpus'])
                            memory_total = calculate_memory_bytes(mesos_slave['resources']['mem'])
                            used_cpu = float(mesos_slave['used_resources']['cpus'])
                            used_mem = calculate_memory_bytes(mesos_slave['used_resources']['mem'])

                            state = NodeInfo.UNKNOWN
                            if mesos_slave["active"]:
                                if used_cpu > 0 or used_mem > 0:
                                    state = NodeInfo.USED
                                else:
                                    state = NodeInfo.IDLE
                            else:
                                state = NodeInfo.OFF

                            slots_free = slots_count - used_cpu
                            memory_free = memory_total - used_mem

                            # Create a fake queue
                            keywords = {}
                            keywords['hostname'] = TypedClass.auto(name)
                            #queues = ["default"]
                            #if queues:
                            #    keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])

                            nodeinfolist[name] = NodeInfo(
                                name, slots_count, slots_free, memory_total, memory_free, keywords)
                            nodeinfolist[name].state = state

        return nodeinfolist

    def get_jobinfolist(self):
        '''Method in charge of monitoring the job queue of Mesos plus Marathon
        The Mesos info about jobs has to be obtained from frameworks and not from tasks,
        because if there are not available resources to execute new tasks, Mesos
        do not create them but frameworks are created
        '''
        jobinfolist = []
        mesos_state = self._obtain_mesos_state()

        # process the result of the mesos mesos_job_state of the system command
        if mesos_state:
            frameworks = mesos_state['frameworks']
            if frameworks:
                for framework in frameworks:
                    if framework['name'] not in ["chronos", "chronos-2.4.0", "marathon"]:
                        job_id = framework['id']
                        nodes = []
                        mesos_job_state = Request.PENDING
                        memory = calculate_memory_bytes(framework['resources']['mem'])
                        cpus_per_task = float(framework['resources']['cpus'])

                        if 'webui_url' in framework:
                            # this is a spark job get the requested resources
                            spark_cpus, spark_mem = self._get_spark_resources(framework['webui_url'])
                            _LOGGER.debug("Spark resources detected: CPUs: %d, MEM: %d" % (spark_cpus, spark_mem))
                            if spark_cpus > 0:
                                cpus_per_task = spark_cpus
                            if spark_mem > 0:
                                memory = spark_mem

                        if memory <= 0:
                            memory = 536870912
                        if cpus_per_task <= 0:
                            cpus_per_task = 1

                        tasks = framework['tasks']
                        if tasks:
                            for task in tasks:
                                mesos_job_state = infer_mesos_job_state(task['state'])
                                node_id = task['slave_id']

                                mesos_nodes = self._obtain_mesos_nodes()
                                if mesos_nodes:
                                    for mesos_node in mesos_nodes['slaves']:
                                        if node_id == mesos_node['id']:
                                            nodes.append(mesos_node['hostname'])

                        jobinfolist = self._update_job_info_list(jobinfolist,
                                                                 1, memory, cpus_per_task,
                                                                 job_id, nodes, mesos_job_state)

        # Obtain Marathon jobs and add to the jobinfolist of Mesos
        marathon_jobinfolist = self._get_marathon_jobinfolist()
        if marathon_jobinfolist:
            jobinfolist = list(set(jobinfolist + marathon_jobinfolist))

        # Obtain chronos jobs and add them to jobinfolist
        chronos_jobinfolist = self._get_chronos_jobinfolist()
        if chronos_jobinfolist:
            jobinfolist = list(set(jobinfolist + chronos_jobinfolist))

        return jobinfolist

if __name__ == '__main__':
    pass
