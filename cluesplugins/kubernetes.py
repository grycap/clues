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

import collections
import requests
import base64
import json
import cpyutils.config
import clueslib.helpers as Helpers

from cpyutils.evaluate import TypedNumber, TypedClass, TypedList
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
            if resp.status_code == 200:
                return resp.json()
            else:
                _LOGGER.error("Error contacting Kubernetes API: %s - %s" % (resp.status_code, resp.text))
                return None
        except Exception as ex:
            _LOGGER.error("Error contacting Kubernetes API: %s" % str(ex))
            return None

    def __init__(self, KUBERNETES_SERVER=None, KUBERNETES_PODS_API_URL_PATH=None,
                 KUBERNETES_NODES_API_URL_PATH=None, KUBERNETES_TOKEN=None, KUBERNETES_NODE_MEMORY=None,
                 KUBERNETES_NODE_SLOTS=None, KUBERNETES_NODE_PODS=None):

        config_kube = cpyutils.config.Configuration(
            "KUBERNETES",
            {
                "KUBERNETES_SERVER": "http://localhost:8080",
                "KUBERNETES_PODS_API_URL_PATH": "/api/v1/pods",
                "KUBERNETES_NODES_API_URL_PATH": "/api/v1/nodes",
                "KUBERNETES_TOKEN": None,
                "KUBERNETES_NODE_MEMORY": 1073741824,
                "KUBERNETES_NODE_SLOTS": 1,
                "KUBERNETES_NODE_PODS": 110,
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
        self._node_pods = Helpers.val_default(KUBERNETES_NODE_PODS, config_kube.KUBERNETES_NODE_PODS)

        if token:
            self.auth_data = {"token": token}
        else:
            self.auth_data = {}
        LRMS.__init__(self, "KUBERNETES_%s" % self._server_url)

    def _get_memory_in_bytes(self, str_memory):
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

    def _get_node_used_resources(self, nodename, pods_data):
        used_mem = 0
        used_cpus = 0.0
        used_pods = 0

        if pods_data:
            for pod in pods_data["items"]:
                if "nodeName" in pod["spec"] and nodename == pod["spec"]["nodeName"]:
                    used_pods += 1
                    cpus, memory = self._get_pod_cpus_and_memory(pod)
                    used_mem += memory
                    used_cpus += cpus

        return used_mem, used_cpus, used_pods

    def get_nodeinfolist(self):
        nodeinfolist = collections.OrderedDict()

        nodes_data = self._create_request('GET', self._nodes_api_url_path, self.auth_data)
        if nodes_data:
            pods_data = self._create_request('GET', self._pods_api_url_path, self.auth_data)
            if not pods_data:
                _LOGGER.error("Error getting Kubernetes pod list. Node usage will not be obtained.")

            for node in nodes_data["items"]:
                name = node["metadata"]["name"]
                memory_total = self._get_memory_in_bytes(node["status"]["allocatable"]["memory"])
                slots_total = int(node["status"]["allocatable"]["cpu"])
                pods_total = int(node["status"]["allocatable"]["pods"])

                skip_node = False
                # Get Taints
                if 'taints' in node["spec"] and node["spec"]['taints']:
                    for taint in node["spec"]['taints']:
                        if taint['effect'] in ["NoSchedule", "PreferNoSchedule", "NoExecute"]:
                            skip_node = True
                            _LOGGER.debug("Node %s is tainted with %s, skiping." % (name, taint['effect']))

                if not skip_node:
                    used_mem, used_cpus, used_pods = self._get_node_used_resources(name, pods_data)

                    memory_free = memory_total - used_mem
                    slots_free = slots_total - used_cpus
                    pods_free = pods_total - used_pods

                    is_ready = True
                    for conditions in node["status"]["conditions"]:
                        if conditions['type'] == "Ready":
                            if conditions['status'] != "True":
                                is_ready = False

                    keywords = {'pods_free': TypedNumber(pods_free),
                                'nodeName': TypedClass(name, TypedClass.STRING)}
                    # Add labels as keywords
                    for key, value in node["metadata"]["labels"].items():
                        keywords[key] = TypedClass(value, TypedClass.STRING)

                    nodeinfolist[name] = NodeInfo(name, slots_total, slots_free, memory_total, memory_free, keywords)
                    if is_ready:
                        nodeinfolist[name].state = NodeInfo.IDLE
                        if used_pods > 0:
                            nodeinfolist[name].state = NodeInfo.USED
                    else:
                        nodeinfolist[name].state = NodeInfo.OFF
        else:
            _LOGGER.error("Error getting Kubernetes node list.")

        # Add the "virtual" nodes
        try:
            vnodes = json.load(open('/etc/clues2/kubernetes_vnodes.info', 'r'))
            for vnode in vnodes:
                name = vnode["name"]
                if name not in nodeinfolist:
                    keywords = {'pods_free': TypedNumber(self._node_pods),
                                'nodeName': TypedClass(name, TypedClass.STRING)}

                    cpus = self._node_slots
                    if "cpus" in vnode:
                        cpus = int(vnode["cpus"])

                    memory = self._node_memory
                    if "memory" in vnode:
                        memory = self._get_memory_in_bytes(vnode["memory"])

                    if "queues" in vnode:
                        queues = vnode["queues"].split(",")
                        if queues:
                            keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])

                    if "keywords" in vnode:
                        for keypair in vnode["keywords"].split(','):
                            parts = keypair.split('=')
                            keywords[parts[0].strip()] = TypedClass(parts[1].strip(), TypedClass.STRING)

                    nodeinfolist[name] = NodeInfo(name, cpus, cpus, memory, memory, keywords)
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
        cpus = 0.0
        memory = 0
        for cont in pod["spec"]["containers"]:
            if "resources" in cont:
                if "requests" in cont["resources"]:
                    if "cpu" in cont["resources"]["requests"]:
                        cpus += self._get_cpu_float(cont["resources"]["requests"]["cpu"])
                    if "memory" in cont["resources"]["requests"]:
                        memory += self._get_memory_in_bytes(cont["resources"]["requests"]["memory"])

        return cpus, memory

    def get_jobinfolist(self):
        '''Method in charge of monitoring the job queue of Mesos plus Marathon
        The Mesos info about jobs has to be obtained from frameworks and not from tasks,
        because if there are not available resources to execute new tasks, Mesos
        do not create them but frameworks are created
        '''
        jobinfolist = []

        pods_data = self._create_request('GET', self._pods_api_url_path, self.auth_data)
        if pods_data:
            for pod in pods_data["items"]:
                job_id = pod["metadata"]["uid"]
                state = pod["status"]["phase"]  # Pending, Running, Succeeded, Failed or Unknown

                job_state = Request.UNKNOWN
                if state == "Pending":
                    job_state = Request.PENDING
                elif state in ["Running", "Succeeded", "Failed"]:
                    job_state = Request.SERVED

                cpus, memory = self._get_pod_cpus_and_memory(pod)

                req_str = '(pods_free > 0)'
                if 'nodeName' in pod["spec"] and pod["spec"]["nodeName"]:
                    req_str += ' && (nodeName = "%s")' % pod["spec"]["nodeName"]

                # Add node selector labels
                if 'nodeSelector' in pod['spec'] and pod['spec']['nodeSelector']:
                    for key, value in pod['spec']['nodeSelector'].items():
                        req_str += ' && (%s == "%s")' % (key, value)
                resources = ResourcesNeeded(cpus, memory, [req_str], 1)
                job_info = JobInfo(resources, job_id, 1)
                job_info.set_state(job_state)
                jobinfolist.append(job_info)
        else:
            _LOGGER.error("Error getting Kubernetes pod list")

        return jobinfolist

if __name__ == '__main__':
    pass
