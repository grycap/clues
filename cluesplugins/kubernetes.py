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

from cmath import isnan
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

    VNODE_FILE = '/etc/clues2/kubernetes_vnodes.info'

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
                "KUBERNETES_NODE_MEMORY": "1 GB",
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
        self._node_memory = self._get_memory_in_bytes(Helpers.val_default(KUBERNETES_NODE_MEMORY,
                                                                          config_kube.KUBERNETES_NODE_MEMORY))
        self._node_slots = Helpers.val_default(KUBERNETES_NODE_SLOTS, config_kube.KUBERNETES_NODE_SLOTS)
        self._node_pods = Helpers.val_default(KUBERNETES_NODE_PODS, config_kube.KUBERNETES_NODE_PODS)

        if token:
            self.auth_data = {"token": token}
        else:
            self.auth_data = {}
        LRMS.__init__(self, "KUBERNETES_%s" % self._server_url)

    def _get_memory_in_bytes(self, str_memory):
        if isinstance(str_memory, (int, float)):
            return str_memory
        str_memory = str_memory.lower()
        if str_memory.strip()[-2:] in ['mi', 'mb', 'gi', 'gb', 'ki', 'kb', 'ti', 'tb']:
            unit = str_memory.strip()[-2:][0]
            memory = int(str_memory.strip()[:-2])
        elif str_memory.strip()[-1:] in ['m', 'g', 'k', 't']:
            unit = str_memory.strip()[-1:]
            memory = int(str_memory.strip()[:-1])
        else:
            return int(str_memory)

        if unit == 'k':
            memory *= 1024
        elif unit == 'm':
            memory *= 1024 * 1024
        elif unit == 'g':
            memory *= 1024 * 1024 * 1024
        elif unit == 't':
            memory *= 1024 * 1024 * 1024 * 1024
        return memory

    def _get_node_used_resources(self, nodename, pods_data):
        used_mem = 0
        used_cpus = 0.0
        used_pods = 0
        system_pods = 0
        used_agpus = 0
        used_ngpus = 0
        used_sgx = 0
        if pods_data:
            for pod in pods_data["items"]:
                if "nodeName" in pod["spec"] and nodename == pod["spec"]["nodeName"]:
                    # do not count the number of pods in case finished jobs
                    if pod["status"]["phase"] not in ["Succeeded", "Failed"]:
                        # do not count the number of pods in case of system ones
                        # nor in case of DaemonSets
                        if (pod["metadata"]["namespace"] in ["kube-system", "kube-flannel"] or
                                "ownerReferences" in pod["metadata"] and pod["metadata"]["ownerReferences"] and pod["metadata"]["ownerReferences"][0]["kind"] == "DaemonSet"):
                            system_pods += 1
                        used_pods += 1
                        cpus, memory, ngpus, agpus, sgx = self._get_pod_cpus_and_memory(pod)
                        used_mem += memory
                        used_cpus += cpus
                        used_agpus += agpus
                        used_ngpus += ngpus
                        used_sgx += sgx

        return used_mem, used_cpus, used_agpus, used_ngpus, used_sgx, used_pods, system_pods

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

                ngpus_total = 0
                agpus_total = 0
                if 'nvidia.com/gpu' in node["status"]["allocatable"]:
                    ngpus_total = int(node["status"]["allocatable"]["nvidia.com/gpu"])
                elif 'amd.com/gpu' in node["status"]["allocatable"]:
                    agpus_total = int(node["status"]["allocatable"]["amd.com/gpu"])

                sgx = 0
                if 'sgx.k8s.io/sgx' in node["status"]["allocatable"]:
                    sgx = int(node["status"]["allocatable"]["sgx.k8s.io/sgx"])

                # Skip master node
                if ("node-role.kubernetes.io/master" in node["metadata"]["labels"] or
                        "node-role.kubernetes.io/control-plane" in node["metadata"]["labels"]):
                    _LOGGER.debug("Node %s seems to be master node, skiping." % name)
                else:
                    used_mem, used_cpus, used_agpus, used_ngpus, used_sgx, used_pods, system_pods = \
                        self._get_node_used_resources(name, pods_data)

                    memory_free = memory_total - used_mem
                    slots_free = slots_total - used_cpus
                    pods_free = pods_total - used_pods
                    ngpus_free = ngpus_total - used_ngpus
                    agpus_free = agpus_total - used_agpus
                    sgx_free = sgx - used_sgx

                    is_ready = True
                    for conditions in node["status"]["conditions"]:
                        if conditions['type'] == "Ready":
                            if conditions['status'] != "True":
                                is_ready = False

                    keywords = {'pods_free': TypedNumber(pods_free),
                                'nodeName': TypedClass(name, TypedClass.STRING),
                                'schedule': TypedNumber(1)}

                    # Get Taints
                    if 'taints' in node["spec"] and node["spec"]['taints']:
                        for taint in node["spec"]['taints']:
                            if taint['effect'] in ["NoSchedule", "NoExecute"]:
                                keywords['schedule'] = TypedNumber(0)

                    if agpus_free:
                        keywords['amd_gpu'] = TypedNumber(agpus_free)
                    if ngpus_free:
                        keywords['nvidia_gpu'] = TypedNumber(ngpus_free)
                    if sgx_free > 0:
                        keywords['sgx'] = TypedNumber(1)

                    # Add labels as keywords
                    for key, value in list(node["metadata"]["labels"].items()):
                        keywords[key] = TypedClass(value, TypedClass.STRING)

                    nodeinfolist[name] = NodeInfo(name, slots_total, slots_free, memory_total, memory_free, keywords)
                    if is_ready:
                        nodeinfolist[name].state = NodeInfo.IDLE
                        if (used_pods - system_pods) > 0:
                            nodeinfolist[name].state = NodeInfo.USED
                    else:
                        nodeinfolist[name].state = NodeInfo.OFF
        else:
            _LOGGER.error("Error getting Kubernetes node list.")

        # Add the "virtual" nodes
        try:
            vnodes = json.load(open(self.VNODE_FILE, 'r'))
            for vnode in vnodes:
                name = vnode["name"]
                if name not in nodeinfolist:
                    keywords = {'pods_free': TypedNumber(self._node_pods),
                                'nodeName': TypedClass(name, TypedClass.STRING),
                                'schedule': TypedNumber(1)}

                    cpus = self._node_slots
                    if "cpu" in vnode:
                        cpus = int(vnode["cpu"])

                    memory = self._node_memory
                    if "memory" in vnode:
                        memory = self._get_memory_in_bytes(vnode["memory"])

                    gpu_vendor = "nvidia"
                    if "gpu_vendor" in vnode:
                        gpu_vendor = vnode["gpu_vendor"].lower()

                    if "gpu" in vnode:
                        gpus = int(vnode["gpu"])
                        keywords['%s_gpu' % gpu_vendor] = TypedNumber(gpus)

                    if "queues" in vnode:
                        queues = vnode["queues"].split(",")
                        if queues:
                            keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])

                    if "keywords" in vnode:
                        for keypair in vnode["keywords"].split(','):
                            parts = keypair.split('=')
                            keywords[parts[0].strip()] = TypedClass(parts[1].strip(), TypedClass.STRING)

                    if "sgx" in vnode:
                        sgx = 0
                        if vnode["sgx"] == 'yes':
                            sgx = 1
                        keywords['sgx'] = TypedNumber(sgx)

                    if "sgx_epc_size" in vnode:
                        sgx_epc_size = int(vnode["sgx_epc_size"])
                        keywords['sgx_epc_size'] = TypedNumber(sgx_epc_size)

                    nodeinfolist[name] = NodeInfo(name, cpus, cpus, memory, memory, keywords)
                    nodeinfolist[name].state = NodeInfo.OFF
        except Exception as ex:
            _LOGGER.error("Error processing file %s: %s" % (self.VNODE_FILE, str(ex)))

        return nodeinfolist

    def _get_cpu_float(self, cpu_info):
        if cpu_info.strip()[-1:] == "m":
            return float(cpu_info.strip()[:-1]) / 1000.0
        else:
            return float(cpu_info)

    def _get_pod_cpus_and_memory(self, pod):
        cpus = 0.0
        memory = 0
        ngpus = agpus = 0
        sgx = 0
        for cont in pod["spec"]["containers"]:
            if "resources" in cont:
                if "requests" in cont["resources"]:
                    if "cpu" in cont["resources"]["requests"]:
                        cpus += self._get_cpu_float(cont["resources"]["requests"]["cpu"])
                    if "memory" in cont["resources"]["requests"]:
                        memory += self._get_memory_in_bytes(cont["resources"]["requests"]["memory"])
                    if "amd.com/gpu" in cont["resources"]["requests"]:
                        agpus += int(cont["resources"]["requests"]["amd.com/gpu"])
                    if "nvidia.com/gpu" in cont["resources"]["requests"]:
                        ngpus += int(cont["resources"]["requests"]["nvidia.com/gpu"])
                    if "sgx.k8s.io/sgx" in cont["resources"]["requests"]:
                        sgx += int(cont["resources"]["requests"]["sgx.k8s.io/sgx"])

        return cpus, memory, ngpus, agpus, sgx

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
                if pod["metadata"]["namespace"] not in ["kube-system", "kube-flannel"]:
                    job_id = pod["metadata"]["uid"]
                    state = pod["status"]["phase"]  # Pending, Running, Succeeded, Failed or Unknown
                    hostIP = None
                    if "hostIP" in pod["status"]:
                        hostIP = pod["status"]["hostIP"]

                    job_state = Request.UNKNOWN
                    if state == "Pending":
                        job_state = Request.PENDING
                        if hostIP:
                            job_state = Request.SERVED
                    elif state in ["Running", "Succeeded", "Failed"]:
                        job_state = Request.SERVED

                    cpus, memory, ngpus, agpus, sgx = self._get_pod_cpus_and_memory(pod)

                    req_str = '(pods_free > 0) && (schedule = 1)'
                    if 'nodeName' in pod["spec"] and pod["spec"]["nodeName"]:
                        req_str += ' && (nodeName = "%s")' % pod["spec"]["nodeName"]
                    if ngpus:
                        req_str += ' && (nvidia_gpu >= %d)' % ngpus
                    if agpus:
                        req_str += ' && (amd_gpu >= %d)' % agpus
                    if sgx > 0:
                        req_str += ' && (sgx >= %d)' % sgx

                    # Add node selector labels
                    if 'nodeSelector' in pod['spec'] and pod['spec']['nodeSelector']:
                        for key, value in list(pod['spec']['nodeSelector'].items()):
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
