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
import cpyutils.config
import clueslib.helpers as Helpers
import json, time


from cpyutils.evaluate import TypedClass, TypedList
from cpyutils.log import Log
from clueslib.node import NodeInfo
from clueslib.platform import LRMS
from clueslib.request import Request, ResourcesNeeded, JobInfo



_LOGGER = Log("PLUGIN-NOMAD")

def open_file(file_path):
    try:
        file_read = open(file_path, 'r')
    except:
        message = "Could not open file with path '%s'" % file_path
        _LOGGER.error(message)
        raise Exception(message)
    return file_read

def _get_memory_in_bytes(str_memory):
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

class lrms(LRMS):

    def _create_request(self, method, url, acl_token=None, headers=None, body=None, auth_data=None):    
        if body is None: 
            body = {}
        if headers is None:
            headers = {}
        if acl_token is not None:
            headers.update({ 'X-Nomad-Token': acl_token})
        auth = None
        if auth_data is not None:
            if 'user' in auth_data and 'passwd' in auth_data:
                auth=requests.auth.HTTPBasicAuth( auth_data['user'], auth_data['passwd'])

        response = {}
        retries = 0
        ok = False
        while (self._max_retries > retries) and (not ok) :
            retries += 1
            try: 
                r =  requests.request(method, url, verify=False, headers=headers, data=body, auth=auth)
                response[ 'status_code' ] = r.status_code
                response[ 'text' ] = r.text
                response[ 'json' ] = r.json()
                ok=True
            except requests.exceptions.ConnectionError:
                _LOGGER.error("Cannot connect to %s, waiting 5 seconds..." % (url))
                time.sleep(5)

        if not ok:
            _LOGGER.error("Cannot connect to %s . Retries: %s" % (url, retries))
            response[ 'status_code' ] = -1
            response[ 'text' ] = 'No response text'
            response[ 'json' ] = {} 

        return response

    def __init__(self, NOMAD_SERVER=None, NOMAD_HEADERS=N one, NOMAD_API_VERSION=None, NOMAD_API_URL_GET_ALLOCATIONS=None, NOMAD_API_URL_GET_SERVERS=None, NOMAD_API_URL_GET_CLIENTS=None, NOMAD_API_URL_GET_CLIENT_INFO=None, MAX_RETRIES=None, NOMAD_ACL_TOKEN=None, NOMAD_AUTH_DATA=None, NOMAD_API_URL_GET_CLIENT_STATUS=None, NOMAD_STATE_OFF=None, NOMAD_STATE_ON=None, NOMAD_PRIVATE_HTTP_PORT=None, NOMAD_API_URL_GET_JOBS=None, NOMAD_API_URL_GET_JOBS_INFO=None, NOMAD_API_URL_GET_ALLOCATION_INFO=None, NOMAD_NODES_LIST_CLUES=None, NOMAD_QUEUES=None, NOMAD_QUEUES_OJPN=None, NOMAD_API_URL_GET_CLIENT_ALLOCATIONS=None, NOMAD_DEFAULT_CPUS_PER_NODE=None, NOMAD_DEFAULT_MEMORY_PER_NODE=None, NOMAD_DEFAULT_CPU_GHZ=None):

        config_nomad = cpyutils.config.Configuration(
            "NOMAD",
            {
                "NOMAD_SERVER": "http://localhost:4646",
                "NOMAD_HEADERS": "{}",
                "NOMAD_API_VERSION": "/v1",
                "NOMAD_API_URL_GET_SERVERS": "/agent/members", # Server node
                "NOMAD_API_URL_GET_CLIENTS": "/nodes", # Server node
                "NOMAD_API_URL_GET_CLIENT_INFO": "/node/$CLIENT_ID$", # Server node
                "NOMAD_API_URL_GET_CLIENT_STATUS": "/client/stats", # Client node
                "NOMAD_API_URL_GET_CLIENT_ALLOCATIONS": "/node/$CLIENT_ID$/allocations", # Server node
                "NOMAD_API_URL_GET_ALLOCATIONS": "/allocations", # Server node
                "NOMAD_API_URL_GET_JOBS": "/jobs", # Server node
                "NOMAD_API_URL_GET_JOBS_INFO": "/job/$JOB_ID$", # Server node
                "NOMAD_API_URL_GET_ALLOCATION_INFO": "/allocation", # Server node
                "NOMAD_ACL_TOKEN": None,
                "MAX_RETRIES": 10,
                "NOMAD_AUTH_DATA": None,
                "NOMAD_STATE_OFF": "down",
                "NOMAD_STATE_ON": "ready",
                "NOMAD_PRIVATE_HTTP_PORT": "4646",
                "NOMAD_NODES_LIST_CLUES": "/etc/clues2/nomad_vnodes.info",
                "NOMAD_QUEUES": "default",
                "NOMAD_QUEUES_OJPN": "", # Queues One Job Per Node
                "NOMAD_DEFAULT_CPUS_PER_NODE": 2.0,
                "NOMAD_DEFAULT_MEMORY_PER_NODE": "8Gi",
                "NOMAD_DEFAULT_CPU_GHZ": 2.6 # Nomad use MHz to manage the jobs assigned CPU  
            }
        )

        self._server_url = Helpers.val_default(NOMAD_SERVER, config_nomad.NOMAD_SERVER).replace('"','')
        self._api_version = Helpers.val_default(NOMAD_API_VERSION, config_nomad.NOMAD_API_VERSION).replace('"','')
        self._api_url_get_allocations = Helpers.val_default(NOMAD_API_URL_GET_ALLOCATIONS, config_nomad.NOMAD_API_URL_GET_ALLOCATIONS).replace('"','')
        self._api_url_get_allocation_info = Helpers.val_default(NOMAD_API_URL_GET_ALLOCATION_INFO, config_nomad.NOMAD_API_URL_GET_ALLOCATION_INFO).replace('"','')
        self._api_url_get_jobs = Helpers.val_default(NOMAD_API_URL_GET_JOBS, config_nomad.NOMAD_API_URL_GET_JOBS).replace('"','')
        self._api_url_get_jobs_info = Helpers.val_default(NOMAD_API_URL_GET_JOBS_INFO, config_nomad.NOMAD_API_URL_GET_JOBS_INFO).replace('"','')
        self._api_url_get_servers = Helpers.val_default(NOMAD_API_URL_GET_SERVERS, config_nomad.NOMAD_API_URL_GET_SERVERS).replace('"','')
        self._api_url_get_clients = Helpers.val_default(NOMAD_API_URL_GET_CLIENTS, config_nomad.NOMAD_API_URL_GET_CLIENTS).replace('"','')
        self._api_url_get_clients_info = Helpers.val_default(NOMAD_API_URL_GET_CLIENT_INFO, config_nomad.NOMAD_API_URL_GET_CLIENT_INFO).replace('"','')
        self._api_url_get_clients_status = Helpers.val_default(NOMAD_API_URL_GET_CLIENT_STATUS, config_nomad.NOMAD_API_URL_GET_CLIENT_STATUS).replace('"','')
        self._api_url_get_clients_allocations = Helpers.val_default(NOMAD_API_URL_GET_CLIENT_ALLOCATIONS, config_nomad.NOMAD_API_URL_GET_CLIENT_ALLOCATIONS).replace('"','')
        self._max_retries = Helpers.val_default(MAX_RETRIES, config_nomad.MAX_RETRIES)
        self._acl_token = Helpers.val_default(NOMAD_ACL_TOKEN, config_nomad.NOMAD_ACL_TOKEN)
        self._auth_data = Helpers.val_default(NOMAD_AUTH_DATA, config_nomad.NOMAD_AUTH_DATA)
        self._state_off = Helpers.val_default(NOMAD_STATE_OFF, config_nomad.NOMAD_STATE_OFF).replace('"','')
        self._state_on = Helpers.val_default(NOMAD_STATE_ON, config_nomad.NOMAD_STATE_ON).replace('"','')
        self._http_port = Helpers.val_default(NOMAD_PRIVATE_HTTP_PORT, config_nomad.NOMAD_PRIVATE_HTTP_PORT).replace('"','')
        self._nodes_info_file = Helpers.val_default(NOMAD_NODES_LIST_CLUES, config_nomad.NOMAD_NODES_LIST_CLUES).replace('"','')
        self._queues = Helpers.val_default(NOMAD_QUEUES, config_nomad.NOMAD_QUEUES).replace('"','').split(',')
        self._queues_ojpn = Helpers.val_default(NOMAD_QUEUES_OJPN, config_nomad.NOMAD_QUEUES_OJPN).replace('"','').split(',')
        self._default_cpu_node = Helpers.val_default(NOMAD_DEFAULT_CPUS_PER_NODE, config_nomad.NOMAD_DEFAULT_CPUS_PER_NODE)
        self._default_memory_node = Helpers.val_default(NOMAD_DEFAULT_MEMORY_PER_NODE, config_nomad.NOMAD_DEFAULT_MEMORY_PER_NODE).replace('"','')
        self._queue_constraint_target = '${node.class}'
        self._cpu_mhz_per_core = float( Helpers.val_default(NOMAD_DEFAULT_CPU_GHZ, config_nomad.NOMAD_DEFAULT_CPU_GHZ) ) / 1000.0


        # Check length of queues
        if len(self._queues) <= 0:
            _LOGGER.error("Error reading NOMAD_QUEUES, NOMAD_QUEUES will be %s" % str(config_nomad.NOMAD_QUEUES) )
            self._queues = [ config_nomad.NOMAD_QUEUES ]
        try:
            self._headers = json.loads(Helpers.val_default(NOMAD_HEADERS, config_nomad.NOMAD_HEADERS))
        except ValueError, e:
            self._headers = {}
            _LOGGER.error("Error loading variable NOMAD_HEADERS from config file, NOMAD_HEADERS will be %s" % str(config_nomad.NOMAD_HEADERS) )
        
        LRMS.__init__(self, "TOKEN_%s" % self._server_url)


    # CLUES API
    def get_jobinfolist(self):
        # Obtain server nodes
        server_nodes_info = self._get_server_nodes_info() 

        taskinfolist, jobs_by_server = self._get_jobinfolist(server_nodes_info)
        return taskinfolist

    def get_nodeinfolist(self):
        # Obtain server nodes
        server_nodes_info = self._get_server_nodes_info()

        nodeinfolist = self._get_nodeinfolist(server_nodes_info)
        return nodeinfolist


    # AUX FUNCTIONS    
    def _get_NodeInfo (self, info_node, default_info_node):
                
        # Check queues 
        keywords = default_info_node['keywords'] 
        queues = default_info_node['keywords']['queues'] 
        q = info_node['node_class']
        if not (q in self._queues or q == '') :
            _LOGGER.error(" '%s' (node_class of Nomad Client) is not a valid queue, queue is set to queue of file %s." % (q, self._nodes_info_file))
        if q in self._queues:
            queues = [ q ]  
            keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])  
        
        # Illustrative values for Clues, since the node is not running, we cannot know the real values
        slots_count = default_info_node['cpus'] 
        slots_free = default_info_node['cpus'] 
        memory_total = default_info_node['memory'] 
        memory_free = default_info_node['memory'] 

        # Information obtained from queries
        if 'slots_count' in info_node['resources']: 
            slots_count = info_node['resources']['slots_count']
        if 'memory_total' in info_node['resources']: 
            memory_total = info_node['resources']['memory_total']        
        if 'slots_used' in info_node['resources']: 
            slots_free = float(slots_count) - float(info_node['resources']['slots_used']) 
        if 'memory_used' in info_node['resources']: 
            memory_free = float(memory_total) - float(info_node['resources']['memory_used'])

        # Check state
        state = NodeInfo.UNKNOWN
        if (info_node['status'] == self._state_on and not info_node['any_job_is_running']):
            state = NodeInfo.IDLE
        elif (info_node['status'] == self._state_on and info_node['any_job_is_running']):
            state = NodeInfo.USED
        elif (info_node['status'] == self._state_off):
            state = NodeInfo.OFF           
	
        #_LOGGER.debug(" name= " + info_node['name'] + ", slots_count= " + str(slots_count) + ", slots_free= " + str(slots_free) + ", memory_total= " + str(memory_total) + ", memory_free= " + str(memory_free) + ", keywords= " + str(keywords) + ", memory_used=" + str(info_node['resources']['memory_used'])  + ", slots_used=" + str(info_node['resources']['slots_used'])  )
        node = NodeInfo(info_node['name'], slots_count, slots_free, memory_total, memory_free, keywords)
        node.state = state

        return node

    def _get_server_nodes_info(self):
        server_nodes_info = []
        url = self._server_url + self._api_version + self._api_url_get_servers
        response = self._create_request('GET', url, auth_data=self._auth_data)
        if (response[ 'status_code' ] == 200):
            for node in response['json']['Members']:
                server_nodes_info.append('http://'+node['Addr']+':'+self._http_port)
        else:
            _LOGGER.error("Error getting Nomad Server nodes addresses: %s: %s" % (response['status_code'], response['text']))
        return server_nodes_info

    def _is_Client_runningAJob (self, server_node, client_id):
        url = server_node + self._api_version + self._api_url_get_clients_allocations.replace('$CLIENT_ID$', client_id )
        response = self._create_request('GET', url) 
        if (response['status_code'] == 200):
            for alloc in response['json']:
                if alloc['ClientStatus'] in ['pending', 'running']:
                    #_LOGGER.debug("_is_Client_runningAJob is TRUE")
                    return True
        else:
            _LOGGER.error("Error getting information about allocations of client with ID=%s from Server node with URL=%s: %s: %s" % (client_id, server_node, response['status_code'], response['text']))
        return False

    def _get_Client_resources (self, server_node, client_id):
        client_addr = self._get_Client_address(server_node, client_id) 
        if client_addr == None:
            return {}
            
        resources = {}
        # Querying Client node for getting the slots_count and memory_total
        url = 'http://' + client_addr + self._api_version + self._api_url_get_clients_status
        response = self._create_request('GET', url) 
        if (response['status_code'] == 200):
            resources['slots_count'] = len(response['json']['CPU'])
            resources['memory_total'] = response['json']['Memory']['Total']
        else:
            _LOGGER.error("Error getting client_status from Client_url=%s: %s: %s" % (client_addr, response['status_code'], response['text']))

        # Querying Client node for getting the slots_used and memory_used
        url = server_node + self._api_version + self._api_url_get_clients_allocations.replace('$CLIENT_ID$', client_id )
        response = self._create_request('GET', url) 
        if (response['status_code'] == 200):
            resources['slots_used'] = 0.0
            resources['memory_used'] = 0.0
            for alloc in response['json']:
                if alloc['ClientStatus'] in ['pending', 'running']: # The job is running or will be soon
                    resources['slots_used'] += ( float(alloc['Resources']['CPU']) / 100.0)
                    resources['memory_used'] += float( _get_memory_in_bytes(str(alloc['Resources']['MemoryMB'])+"M"))
        else:
            _LOGGER.error("Error getting information about allocations of client with ID=%s from Server node with URL=%s: %s: %s" % (client_id, server_node, response['status_code'], response['text']))

        return resources

    def _get_Clients_by_Server(self, server_node):
        clients = {}
        url = server_node + self._api_version + self._api_url_get_clients
        response = self._create_request('GET', url) 
        if (response['status_code'] == 200):
            for client in response['json']:
                client_id = client['ID']
                clients[ client_id ] = {}
                clients[ client_id ]['client_id'] = client_id
                clients[ client_id ]['name'] = client['Name'] 
                clients[ client_id ]['status'] = client['Status'] 
                clients[ client_id ]['status_description'] = client['StatusDescription'] 
                clients[ client_id ]['node_class'] = client['NodeClass']
                clients[ client_id ]['any_job_is_running'] = self._is_Client_runningAJob (server_node, client['ID'] )
                clients[ client_id ]['state'] = NodeInfo.OFF
                if (client['Status'] == self._state_on):
                    clients[ client['ID'] ]['state'] = NodeInfo.IDLE
        else:
            _LOGGER.error("Error getting information about the Clients of the Server node with URL=%s: %s: %s" % (server_node, response['status_code'], response['text']))
        return clients

    def _get_Client_address(self, server_node, client_id):
        addr = None
        url = server_node + self._api_version + self._api_url_get_clients_info.replace('$CLIENT_ID$', client_id )
        response = self._create_request('GET', url)
        if (response['status_code'] == 200):
            addr = response['json']['HTTPAddr'] 
        else:
            _LOGGER.error("Error getting client_addr from Server_url=%s and Client_ID=%s: %s: %s" % (server_node, client_id, response['status_code'], response['text']))
        return addr

    def _get_nodeinfolist(self, server_nodes_info):
        ##_LOGGER.info("***** START - get_nodeinfolist ***** ")
        nodeinfolist = collections.OrderedDict()
        default_node_info = collections.OrderedDict()

        # DEFAULT NODE INFO
        try:
            vnodes = json.load(open(self._nodes_info_file, 'r'))
            for vnode in vnodes:
                NODE = {}
                NODE['name'] = vnode["name"] 
                NODE['state'] = NodeInfo.OFF
                NODE['keywords'] = {}
                
                NODE['cpus'] = float(self._default_cpu_node)
                if "cpu" in vnode:
                    NODE['cpus'] = int(vnode["cpu"])

                NODE['memory'] = _get_memory_in_bytes(self._default_memory_node)   
                if "memory" in vnode:
                    NODE['memory'] = _get_memory_in_bytes(vnode["memory"])

                if "keywords" in vnode:
                    for keypair in vnode["keywords"].split(','):
                        parts = keypair.split('=')
                        NODE['keywords'][parts[0].strip()] = TypedClass(parts[1].strip(), TypedClass.STRING)

                if "queues" in vnode:
                    queues = vnode["queues"].split(",")
                    if queues:
                        NODE['keywords']['queues'] = TypedList([TypedClass.auto(q) for q in queues])
                else: # All queues to the node
                    NODE['keywords']['queues'] = TypedList([TypedClass.auto(q) for q in self._queues[:] ])  

                default_node_info[ NODE['name'] ] = NODE

        except Exception as ex:
            _LOGGER.error("Error processing file %s: %s" % (self._nodes_info_file , str(ex)) )

        clients_by_server = {}
        for server_node in server_nodes_info:
            clients_by_server[ server_node ] = self._get_Clients_by_Server(server_node)  # Obtain ID, Name, Status, NodeClass and if the Client is running some job 
            # Obtain Resources and Queues
            for client_id in clients_by_server[ server_node ]:
                info_client = clients_by_server[ server_node ][ client_id ]
                if (info_client['state'] in [NodeInfo.IDLE, NodeInfo.USED]): # Client is ON
                    # Obtain Client node address for checking used resources
                    info_client ['resources'] = self._get_Client_resources (server_node, client_id)

                    if info_client['name'] in default_node_info: # Valid node for CLUES and IM
                        nodeinfolist[ info_client['name'] ] = self._get_NodeInfo(info_client, default_node_info[ info_client['name'] ])
                    else:
                        _LOGGER.warning("Nomad Client with name '%s' founded using Nomad Server API but not exists this node in the configuration file %s" % (info_client['name'] , self._nodes_info_file) )
        
        # Add nodes from nomad_info file to the list
        for namenode, node_info in default_node_info.items():
            if namenode not in nodeinfolist: 
                nodeinfolist[ namenode ] = NodeInfo(namenode, node_info['cpus'], node_info['cpus'], node_info['memory'], node_info['memory'], node_info['keywords'])
                nodeinfolist[ namenode ].state = node_info['state']
                
        # Print all nodes in log with keywords
        for key, value in nodeinfolist.items():
            string = "%s + keywords={ " % (str(value) ) 
            for key2 in value.keywords:
                string += key2 + ":" + str(value.keywords[key2]) +","
            string = string[:-1]  + "}"
            _LOGGER.debug( string )
        ##_LOGGER.info("***** END - get_nodeinfolist ***** ")
        return nodeinfolist

    def _get_Jobs_by_Server(self, server_node):
        jobs = {}
        url = server_node + self._api_version + self._api_url_get_jobs
        response = self._create_request('GET', url) 
        if (response['status_code'] == 200):
            for job in response['json']:
                jobs[ job['ID'] ]={}
                jobs[ job['ID'] ]['status'] = job['Status']
                jobs[ job['ID'] ]['status_description'] = job['StatusDescription']
                jobs[ job['ID'] ]['job_id'] = job['ID']
                jobs[ job['ID'] ]['name'] = job['Name'] 
                jobs[ job['ID'] ]['TaskGroups'] = {}
                for taskgroup_id, tasks_info in job['JobSummary']['Summary'].items():
                    jobs[ job['ID'] ]['TaskGroups'][taskgroup_id] = {}
                    jobs[ job['ID'] ]['TaskGroups'][taskgroup_id]['name'] = job['ID'] + '-' + taskgroup_id
                    jobs[ job['ID'] ]['TaskGroups'][taskgroup_id]['cpu'] = 0.0
                    jobs[ job['ID'] ]['TaskGroups'][taskgroup_id]['memory'] = 0.0
                    jobs[ job['ID'] ]['TaskGroups'][taskgroup_id]['queue'] = 'no_queue' #self._queues[0]
                    # Check state
                    jobs[ job['ID'] ]['TaskGroups'][taskgroup_id]['state'] = Request.UNKNOWN
                    if (tasks_info['Queued'] > 0 or tasks_info['Starting'] > 0) and jobs[ job['ID'] ]['status'] != "dead":
                        jobs[ job['ID'] ]['TaskGroups'][taskgroup_id]['state'] = Request.PENDING
                    else: 
                        jobs[ job['ID'] ]['TaskGroups'][taskgroup_id]['state'] = Request.SERVED
        else:
            _LOGGER.error("Error getting jobs from Server node with URL = %s: %s: %s" % (server_node, response['status_code'], response['text']))
        return jobs

    def _get_TaskGroup_resources (self, jobs, server_node):
        for job_id in jobs: 
            url = server_node + self._api_version + self._api_url_get_jobs_info.replace('$JOB_ID$',job_id)
            response = self._create_request('GET', url) 
            if response['status_code'] == 200:
                for task_group in response['json']['TaskGroups']:
                    taskgroup_id = task_group['Name']
                    if taskgroup_id in jobs[job_id]['TaskGroups']: 
                        # Obtain Queue of the taskgroup
                        warning_constraint = True
                        if type(task_group['Constraints']) is list:
                            for constraint in task_group['Constraints']:
                                if constraint['LTarget'] == self._queue_constraint_target and constraint['RTarget'] in self._queues:
                                    jobs[job_id]['TaskGroups'][taskgroup_id]['queue'] = constraint['RTarget']
                                    warning_constraint = False
                        
                        if warning_constraint: 
                            jobs[job_id]['TaskGroups'][taskgroup_id]['queue'] = 'no_queue'
                            _LOGGER.warning("No '%s' contraint for taskgroup '%s' of the job '%s' or it isn't a valid queue from Server node with URL=%s. This job will not be added to the CLUES list" % (self._queue_constraint_target, taskgroup_id, job_id, server_node))

                        # Obtain Resources of the taskgroup
                        jobs[job_id]['TaskGroups'][taskgroup_id]['cpu'] = 0.0
                        jobs[job_id]['TaskGroups'][taskgroup_id]['memory'] = 0.0
                        if len(task_group['Tasks']) > 1:
                            _LOGGER.warning( "Taskgroup '%s' of job '%s' has got multiple tasks and this plugin doesn't support this. " % (taskgroup_id, job_id) )
                        for task in task_group['Tasks']:
                            jobs[job_id]['TaskGroups'][taskgroup_id]['cpu'] += float(task['Resources']['CPU']) / self._cpu_mhz_per_core
                            jobs[job_id]['TaskGroups'][taskgroup_id]['memory'] += float(task['Resources']['MemoryMB'] * 1024 * 1024 )

                    
            else:
                _LOGGER.error("Error getting job information with job_id = %s from Server node with URL = %s: %s: %s" % (job_id, server_node, response['status_code'], response['text']))  
                # Default values
                for taskgroup_id in jobs[job_id]['TaskGroups'].keys():
                    jobs[job_id]['TaskGroups'][taskgroup_id]['cpu'] = 0.0
                    jobs[job_id]['TaskGroups'][taskgroup_id]['memory'] = 0.0
                    jobs[job_id]['TaskGroups'][taskgroup_id]['queue'] = 'no_queue'

        return jobs      

    def _get_JobInfo(self, info):
        queue = '"' + info['queue'] + '" in queues'
        taskcount = 1 

        resources = ResourcesNeeded(info['cpu'], info['memory'], [queue], taskcount)
        
        job_info = JobInfo(resources, info['name'], 1)
        
        # Set state
        job_info.set_state(info['state'])
        
        return job_info

    def _get_jobinfolist(self, server_nodes_info):
        taskinfolist = []
        jobs_by_server = {}     
        
        # Obtain jobs id
        for server_node in server_nodes_info:
            # Obtain job_id, job_name, taskgroup name and taskgroup state
            jobs_by_server[ server_node ] = self._get_Jobs_by_Server(server_node)

            # Obtain task resources for each taskgroup 
            jobs_by_server[server_node] = self._get_TaskGroup_resources(jobs_by_server[server_node], server_node)

            for job_id in jobs_by_server[server_node]:
                for taskgroup_id in jobs_by_server[server_node][job_id]['TaskGroups']:
                    added = 'NOT'
                    if 'no_queue' !=  jobs_by_server[server_node][job_id]['TaskGroups'][taskgroup_id]['queue']:
                        added = ''
                        taskinfolist.append( self._get_JobInfo( jobs_by_server[server_node][job_id]['TaskGroups'][taskgroup_id] ) )
                    _LOGGER.debug(" *JOB %s ADDED* - task_name = %s, cpu = %.2f, memory = %.2f, queue = %s and state = %d " % ( added, jobs_by_server[server_node][job_id]['TaskGroups'][taskgroup_id]['name'], jobs_by_server[server_node][job_id]['TaskGroups'][taskgroup_id]['cpu'], jobs_by_server[server_node][job_id]['TaskGroups'][taskgroup_id]['memory'], jobs_by_server[server_node][job_id]['TaskGroups'][taskgroup_id]['queue'], jobs_by_server[server_node][job_id]['TaskGroups'][taskgroup_id]['state'] ) )

        return taskinfolist, jobs_by_server

if __name__ == '__main__':
    pass

