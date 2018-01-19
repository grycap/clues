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
import collections
import requests
import base64
import cpyutils.config
import clueslib.helpers as Helpers

from cpyutils.evaluate import TypedClass, TypedList
from cpyutils.log import Log
from clueslib.node import NodeInfo
from clueslib.platform import LRMS
from clueslib.request import Request, ResourcesNeeded, JobInfo


_LOGGER = Log("PLUGIN-KUBERNETES")


class lrms(LRMS):

    def _get_auth_header(self, auth):
        """
        Generate the auth header needed to contact with the Kubernetes API server.
        """
        auth_header = {}
        if 'username' in auth and 'password' in auth:
            passwd = auth['password']
            user = auth['username']
            auth_header = {'Authorization': 'Basic ' +
                           (base64.encodestring((user + ':' + passwd).encode('utf-8'))).strip().decode('utf-8')}
        elif 'token' in auth:
            token = auth['token']
            auth_header = {'Authorization': 'Bearer ' + token}

        return auth_header

    def _create_request(self, method, url, auth_data, headers=None, body=None):
        try:
            if headers is None:
                headers = {}
            auth_header = self._get_auth_header(auth_data)
            if auth_header:
                headers.update(auth_header)
    
            url = "%s%s" % (self._server_url, url)
            resp = requests.request(method, url, verify=False, headers=headers, data=body)
    
            return resp
        except Exception as ex:
            _LOGGER.error("Error contanctinf Kubernetes API: %s" % str(ex))
            return None

    def __init__(self, KUBERNETES_SERVER=None, KUBERNETES_PODS_API_URL_PATH=None,
                 KUBERNETES_NODES_API_URL_PATH=None, KUBERNETES_TOKEN=None, KUBERNETES_NODE_MEMORY=None,
                 KUBERNETES_NODE_SLOTS=None):

        config_kube = cpyutils.config.Configuration(
            "KUBERNETES",
            {
                "KUBERNETES_SERVER": "http://localhost:8080",
                "KUBERNETES_PODS_API_URL_PATH": "/api/v1/pods",
                "KUBERNETES_NODES_API_URL_PATH": "/api/v1/nodes",
                "KUBERNETES_TOKEN": None,
                "KUBERNETES_NODE_MEMORY": 1073741824,
                "KUBERNETES_NODE_SLOTS": 1,
            }
        )

        self._server_url = Helpers.val_default(KUBERNETES_SERVER, config_kube.KUBERNETES_SERVER)
        self._pods_api_url_path = Helpers.val_default(KUBERNETES_PODS_API_URL_PATH,
                                                      config_kube.KUBERNETES_PODS_API_URL_PATH)
        self._nodes_api_url_path = Helpers.val_default(KUBERNETES_NODES_API_URL_PATH,
                                                       config_kube.KUBERNETES_NODES_API_URL_PATH)
        token = Helpers.val_default(KUBERNETES_TOKEN, config_kube.KUBERNETES_TOKEN)
        self._node_memory = Helpers.val_default(KUBERNETES_NODE_MEMORY, config_kube.KUBERNETES_NODE_MEMORY)
        self._node_slots = Helpers.val_default(KUBERNETES_NODE_SLOTS, config_kube.KUBERNETES_NODE_SLOTS)

        if token:
            self.auth_data = {"token": token}
        else:
            self.auth_data = {}
        LRMS.__init__(self, "KUBERNETES_%s" % self._server_url)

    def _get_memory_in_bytes(self, str_memory):
        if str_memory.strip()[-2:] in ['Mi', 'Gi', 'Ki']:
            unit = str_memory.strip()[-2:]
            memory = int(str_memory.strip()[:-2])
            if unit == 'Ki':
                memory *= 1024
            elif unit == 'Mi':
                memory *= 1024 * 1024
            elif unit == 'Gi':
                memory *= 1024 * 1024 * 1024
            elif unit == 'Ti':
                memory *= 1024 * 1024 * 1024 * 1024
            return memory
        else:
            return int(str_memory)

    def get_nodeinfolist(self):
        nodeinfolist = collections.OrderedDict()

        resp = self._create_request('GET', self._nodes_api_url_path, self.auth_data)
        if resp and resp.status_code == 200:
            nodes_data = resp.json()

            for node in nodes_data["items"]:
                # not add master node
                if "node-role.kubernetes.io/master" not in node["metadata"]["labels"]:
                    name = node["metadata"]["name"]
                    memory_total = self._get_memory_in_bytes(node["status"]["capacity"]["memory"])
                    slots_count = int(node["status"]["capacity"]["cpu"])
                    pods_total = node["status"]["capacity"]["pods"]
                    memory_free = self._get_memory_in_bytes(node["status"]["allocatable"]["memory"])
                    slots_free = int(node["status"]["allocatable"]["cpu"])
                    pods_free = node["status"]["allocatable"]["pods"]
    
                    is_ready = True
                    for conditions in node["status"]["conditions"]:
                        if conditions['type'] == "Ready":
                            if conditions['status'] != "True":
                                is_ready = False
    
                    keywords = {}
                    # Create a fake queue
                    queues = ["default"]
                    if queues:
                        keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])
                    nodeinfolist[name] = NodeInfo(name, slots_count, slots_free, memory_total, memory_free, keywords)
                    if is_ready:
                        nodeinfolist[name].state = NodeInfo.IDLE
                        if memory_free <= 0 or slots_free <= 0 or pods_free <= 0:
                            nodeinfolist[name].state = NodeInfo.USED
                    else:
                        nodeinfolist[name].state = NodeInfo.OFF
        else:
            _LOGGER.error("Error getting Kubernetes node list: %s: %s" % (resp.status_code, resp.text))

        # Add the "virtual" nodes
        try:
            for line in open('/etc/clues2/kubernetes_vnodes.info', 'r'):
                name = line.rstrip('\n')
                if name not in nodeinfolist:
                    keywords = {}
                    queues = ["default"]
                    if queues:
                        keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])
                    nodeinfolist[name] = NodeInfo(name, self._node_slots, self._node_slots,
                                                  self._node_memory, self._node_memory, keywords)
                    nodeinfolist[name].state = NodeInfo.OFF
        except Exception as ex:
            _LOGGER.error("Error processing file /etc/clues2/kubernetes_vnodes.info: %s" % str(ex))

        return nodeinfolist

    def _get_cpu_float(self, cpu_info):
        if cpu_info.strip()[-1:] == "m":
            return float(cpu_info.strip()[:-1]) / 1000.0
        else:
            return float(cpu_info)

    def _get_pod_cpus_and_memory(self, pod):
        cpus = memory = 0
        for cont in pod["spec"]["containers"]:
            if "resources" in cont:
                # we give precedence requests to limits
                # TODO: Use the highest value?
                if "requests" in cont["resources"]:
                    if "cpu" in cont["resources"]["requests"]:
                        cpus += self._get_cpu_float(cont["resources"]["requests"]["cpu"])
                    if "memory" in cont["resources"]["requests"]:
                        memory += self._get_memory_in_bytes(cont["resources"]["requests"]["memory"])
                elif "limits" in cont["resources"]:
                    if "cpu" in cont["resources"]["limits"]:
                        cpus += self._get_cpu_float(cont["resources"]["limits"]["cpu"])
                    if "memory" in cont["resources"]["limits"]:
                        memory += self._get_memory_in_bytes(cont["resources"]["limits"]["memory"])

        return cpus, memory

    def get_jobinfolist(self):
        '''Method in charge of monitoring the job queue of Mesos plus Marathon
        The Mesos info about jobs has to be obtained from frameworks and not from tasks,
        because if there are not available resources to execute new tasks, Mesos
        do not create them but frameworks are created
        '''
        jobinfolist = []

        resp = self._create_request('GET', self._pods_api_url_path, self.auth_data)
        if resp and resp.status_code == 200:
            pods_data = resp.json()
            for pod in pods_data["items"]:
                job_id = pod["metadata"]["uid"]
                state = pod["status"]["phase"]  # Pending, Running, Succeeded, Failed or Unknown

                job_state = Request.UNKNOWN
                if state == "Pending":
                    job_state = Request.PENDING
                elif state in ["Running", "Succeeded", "Failed"]:
                    job_state = Request.SERVED

                cpus, memory = self._get_pod_cpus_and_memory(pod)

                queue = '"default" in queues'
                resources = ResourcesNeeded(cpus, memory, [queue], 1)
                job_info = JobInfo(resources, job_id, 1)
                job_info.set_state(job_state)
                jobinfolist.append(job_info)
        else:
            _LOGGER.error("Error getting Kubernetes pod list: %s: %s" % (resp.status_code, resp.text))

        return jobinfolist

if __name__ == '__main__':
    pass
