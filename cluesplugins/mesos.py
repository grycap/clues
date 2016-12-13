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

import subprocess
import json
import cpyutils.config
import clueslib.helpers as Helpers

from cpyutils.evaluate import TypedClass, TypedList
from cpyutils.log import Log
from clueslib.node import NodeInfo
from clueslib.platform import LRMS
from clueslib.request import Request, ResourcesNeeded, JobInfo

_LOGGER = Log("PLUGIN-MESOS")


def run_command(command):
    if command:
        # _LOGGER.debug("Executing command: '" + str(" ".join(command)) + "'")
        try:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if process:
                (out, err) = process.communicate()
                if process.returncode != 0:
                    message = "RETURN_CODE=" + str(process.returncode)
                    if err:
                        message += ";ERROR_OUTPUT=" + str(err)
                    _LOGGER.error(message)
                    raise Exception(message)
                return out
        except Exception as excp:
            message = "ERROR_EXECUTING_COMMAND=" + str(" ".join(command)) + ";" + str(excp)
            _LOGGER.error(message)
            raise Exception(message)


def curl_command(command, server_ip, error_message, is_json=True):
    try:
        result = run_command(command.split(" "))
        if result:
            if is_json:
                return json.loads(result)
            else:
                return result
    except Exception as exception:
        message = str(exception) + ';ERROR=' + error_message.rstrip('\n') + ';SERVER_IP=' + \
                server_ip.rstrip('\n')
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


def infer_marathon_job_state(jobs, jobs_running):
    ''' Determines the equivalent state between Marathon jobs and Clues2 possible job states'''
    return Request.ATTENDED if jobs and jobs_running > 0 else Request.PENDING


def infer_clues_node_state(node_id, node_state, used_nodes):
    ''' Determines the equivalent node_state between Mesos slaves and Clues2 possible node states
    MESOS node states: active=true || active=false
    CLUES2 node states: ERROR, UNKNOWN, IDLE, USED, OFF
    '''
    clues_node_state = ""
    if node_state and node_id not in used_nodes:
        clues_node_state = NodeInfo.IDLE
    elif node_state and node_id in used_nodes:
        clues_node_state = NodeInfo.USED
    elif not node_state:
        clues_node_state = NodeInfo.OFF
    else:
        clues_node_state = NodeInfo.UNKNOWN
    return clues_node_state


def calculate_memory_bytes(memory):
    return memory * 1048576


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
        _LOGGER.debug("Obtaining marathon jobs")
        return curl_command(self._marathon, self._server_ip, "Could not obtain information about Marathon jobs")

    def _obtain_mesos_state(self):
        '''Obtains the state of the Mesos server'''
        return curl_command(self._state, self._server_ip, "Could not obtain information about MESOS state")

    def _obtain_mesos_used_nodes(self):
        '''Identifies the nodes that are in "USED" state (jobs in state "TASK_RUNNING")'''
        mesos_jobs = self._obtain_mesos_jobs()
        used_nodes = []

        if mesos_jobs:
            for mesos_job in mesos_jobs['tasks']:
                state = mesos_job['state']
                if state == "TASK_RUNNING" or state == "TASK_STAGING":
                    used_nodes.append(mesos_job['slave_id'])

        return used_nodes

    def _obtain_cpu_mem_used_in_mesos_node(self, slave_id):
        ''' Obtains the mem and cpu used by the mesos_job that is in execution in the node with id 'slave_id' '''
        used_cpu = 0
        used_mem = 0
        mesos_jobs = self._obtain_mesos_jobs()

        if mesos_jobs:
            for mesos_job in mesos_jobs['tasks']:
                if mesos_job['slave_id'] == slave_id and mesos_job['state'] == "TASK_RUNNING":
                    used_cpu = float(mesos_job['resources']['cpus'])
                    used_mem = calculate_memory_bytes(mesos_job['resources']['mem'])

        return used_cpu, used_mem

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
        queue = '"default" in queues'
        resources = ResourcesNeeded(cpus_per_task, memory, [queue], numnodes)
        job_info = JobInfo(resources, job_id, nodes)
        job_info.set_state(state)
        jobinfolist.append(job_info)
        return jobinfolist

    def _get_chronos_jobinfolist(self):
        '''Method in charge of monitoring the chronos_job queue of Chronos'''
        jobinfolist = []
        chronos_jobs = self._obtain_chronos_jobs()

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
                    marathon_job_state = infer_marathon_job_state(tasks, job_attributes['tasksRunning'])
                    jobinfolist = self._update_job_info_list(jobinfolist,
                                                             cpus_per_task, memory, numnodes,
                                                             job_id, nodes, marathon_job_state)
        return jobinfolist

    def __init__(self, MESOS_SERVER=None, MESOS_NODES_COMMAND=None, MESOS_STATE_COMMAND=None, MESOS_JOBS_COMMAND=None,
                 MESOS_MARATHON_COMMAND=None, MESOS_CHRONOS_COMMAND=None, MESOS_CHRONOS_STATE_COMMAND=None):

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
                "/usr/bin/curl -L -X GET http://mesosserverpublic:5050/master/tasks.json",
                "MESOS_MARATHON_COMMAND":
                "/usr/bin/curl -L -X GET http://mesosserverpublic:8080/v2/apps?embed=tasks",
                "MESOS_CHRONOS_COMMAND":
                "/usr/bin/curl -L -X GET http://mesosserverpublic:4400/scheduler/jobs",
                "MESOS_CHRONOS_STATE_COMMAND":
                "/usr/bin/curl -L -X GET http://mesosserverpublic:4400/scheduler/graph/csv"
            }
        )

        self._server_ip = Helpers.val_default(MESOS_SERVER, config_mesos.MESOS_SERVER)
        self._nodes = Helpers.val_default(MESOS_NODES_COMMAND, config_mesos.MESOS_NODES_COMMAND)
        self._state = Helpers.val_default(MESOS_STATE_COMMAND, config_mesos.MESOS_STATE_COMMAND)
        self._jobs = Helpers.val_default(MESOS_JOBS_COMMAND, config_mesos.MESOS_JOBS_COMMAND)
        self._marathon = Helpers.val_default(MESOS_MARATHON_COMMAND, config_mesos.MESOS_MARATHON_COMMAND)
        self._chronos = Helpers.val_default(MESOS_CHRONOS_COMMAND, config_mesos.MESOS_CHRONOS_COMMAND)
        self._chronos_state = Helpers.val_default(MESOS_CHRONOS_STATE_COMMAND, config_mesos.MESOS_CHRONOS_STATE_COMMAND)
        LRMS.__init__(self, "MESOS_%s" % self._server_ip)

    def get_nodeinfolist(self):
        nodeinfolist = {}
        infile = open_file('/etc/clues2/mesos_vnodes.info')
        if infile:
            for line in infile:
                name = line.rstrip('\n')
                state = NodeInfo.OFF
                # Illustrative values for Clues, since the node is not running, we
                # cannot know the real values
                slots_count = 1
                memory_total = 1572864000
                slots_free = 1
                memory_free = 1572864000
                # Create a fake queue
                keywords = {}
                keywords['hostname'] = TypedClass.auto(name)
                queues = ["default"]
                if queues:
                    keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])

                nodeinfolist[name] = NodeInfo(name, slots_count, slots_free, memory_total, memory_free, keywords)
                nodeinfolist[name].state = state
            infile.close()

        mesos_slaves = self._obtain_mesos_nodes()
        if mesos_slaves:
            used_nodes = self._obtain_mesos_used_nodes()
            for mesos_slave in mesos_slaves['slaves']:
                name = mesos_slave['hostname']
                if nodeinfolist:
                    for node in nodeinfolist:
                        if name == nodeinfolist[node].name:
                            state = infer_clues_node_state(mesos_slave["id"], mesos_slave["active"], used_nodes)
                            slots_count = float(mesos_slave['resources']['cpus'])
                            memory_total = calculate_memory_bytes(mesos_slave['resources']['mem'])

                            used_cpu, used_mem = self._obtain_cpu_mem_used_in_mesos_node(mesos_slave["id"])
                            slots_free = slots_count - used_cpu
                            memory_free = memory_total - used_mem

                            # Create a fake queue
                            keywords = {}
                            keywords['hostname'] = TypedClass.auto(name)
                            queues = ["default"]
                            if queues:
                                keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])

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
                    if(framework['name'] != "chronos-2.4.0" and framework['name'] != "marathon"):
                        job_id = framework['id']
                        nodes = []
                        numnodes = 1
                        mesos_job_state = Request.PENDING
                        memory = calculate_memory_bytes(framework['resources']['mem'])
                        if memory <= 0:
                            memory = 536870912
                        cpus_per_task = float(framework['resources']['cpus'])
                        if cpus_per_task <= 0:
                            cpus_per_task = 1

                        tasks = framework['tasks']
                        if tasks:
                            for task in tasks:
                                mesos_job_state = infer_mesos_job_state(task['state'])
                                node_id = task['slave_id']

                                mesos_nodes = self._obtain_mesos_nodes()
                                for mesos_node in mesos_nodes['slaves']:
                                    if node_id == mesos_node['id']:
                                        nodes.append(mesos_node['hostname'])

                        jobinfolist = self._update_job_info_list(jobinfolist,
                                                                 cpus_per_task, memory, numnodes,
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
