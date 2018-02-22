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

def get_memory_in_bytes(str_memory):
    if (str_memory.strip()[-2:] in ['Mi', 'Gi', 'Ki']):
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

    def __init__(self, NOMAD_SERVER=None, NOMAD_HEADERS=None, NOMAD_API_VERSION=None, NOMAD_API_URL_GET_ALLOCATIONS=None, NOMAD_API_URL_GET_SERVERS=None, NOMAD_API_URL_GET_CLIENTS=None, NOMAD_API_URL_GET_CLIENT_INFO=None, MAX_RETRIES=None, NOMAD_ACL_TOKEN=None, NOMAD_AUTH_DATA=None, NOMAD_API_URL_GET_CLIENT_STATUS=None, NOMAD_STATE_OFF=None, NOMAD_STATE_ON=None, NOMAD_PRIVATE_HTTP_PORT=None, NOMAD_API_URL_GET_JOBS=None, NOMAD_API_URL_GET_JOBS_INFO=None, NOMAD_API_URL_GET_ALLOCATION_INFO=None, NOMAD_NODES_LIST_CLUES=None, NOMAD_QUEUES=None, NOMAD_QUEUES_OJPN=None, NOMAD_API_URL_GET_CLIENT_ALLOCATIONS=None, NOMAD_DEFAULT_CPUS_PER_NODE=None, NOMAD_DEFAULT_MEMORY_PER_NODE=None):

        config_nomad = cpyutils.config.Configuration(
            "NOMAD",
            {
                "NOMAD_SERVER": "http://localhost:4646",
                "NOMAD_HEADERS": "{}",
                "NOMAD_API_VERSION": "/v1",
                "NOMAD_API_URL_GET_SERVERS": "/agent/members", # master server
                "NOMAD_API_URL_GET_CLIENTS": "/nodes", # master server
                "NOMAD_API_URL_GET_CLIENT_INFO": "/node/$CLIENT_ID$", # master server
                "NOMAD_API_URL_GET_CLIENT_STATUS": "/client/stats", # client node
                "NOMAD_API_URL_GET_CLIENT_ALLOCATIONS": "/node/$CLIENT_ID$/allocations", # master server
                "NOMAD_API_URL_GET_ALLOCATIONS": "/allocations", # master node
                "NOMAD_API_URL_GET_JOBS": "/jobs", # master node
                "NOMAD_API_URL_GET_JOBS_INFO": "/job/$JOB_ID$", # master node
                "NOMAD_API_URL_GET_ALLOCATION_INFO": "/allocation", # master node
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
                "NOMAD_DEFAULT_MEMORY_PER_NODE": "8Gi"
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

    def _get_NodeInfo (self, info_node):
        name = info_node['name']

        # Illustrative values for Clues, since the node is not running, we cannot know the real values
        slots_count = float(self._default_cpu_node)
        slots_free = slots_count
        memory_total = get_memory_in_bytes(self._default_memory_node)
        memory_free = memory_total
        
        # Check state
        state = NodeInfo.OFF
        if (info_node['status'] == self._state_on):
            state = NodeInfo.IDLE

        # Keywords
        keywords = {}
        keywords['hostname'] = TypedClass.auto(name)       
        
        # Check queues 
        queues = self._queues[:]                       
        q = info_node['node_class']
        if not (q in self._queues or q == '') :
            _LOGGER.error(" '%s' (node_class of Nomad Client) is not a valid queue, queue is set to all queues." % (q))
        if q in self._queues:
            queues = [ q ]  
        keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])

        # Check node state if has some OJPN queue   
        if ( set( queues ).intersection(self._queues_ojpn) and info_node['any_job_is_running'] and state == NodeInfo.IDLE): # Some queue is a OJPN queue and job is running and the node is ON
            #_LOGGER.info(" ****** Check queues is true  ****** for node %s: any_job_is_running = %s" % (name, str(info_node['any_job_is_running'])))
            state = NodeInfo.USED
    
        
        # Information of query
        if ('client_status' in info_node): 
            slots_count = len( info_node['client_status']['CPU'] )
            memory_total = info_node['client_status']['Memory']['Total']
            memory_free = info_node['client_status']['Memory']['Available']
            slots_free = 0
            for cpu in info_node['client_status']['CPU']:
                slots_free += float(cpu['Idle']) / ( float(slots_count) * 100.0)
            if (memory_free <= 0 or slots_free <= 0):
                state = NodeInfo.USED                   
       

        node = NodeInfo(name, slots_count, slots_free, memory_total, memory_free, keywords)
        node.state = state

        return node

    def _get_Master_nodes(self):
        master_nodes = []
        url = self._server_url + self._api_version + self._api_url_get_servers
        response = self._create_request('GET', url, auth_data=self._auth_data)
        if (response[ 'status_code' ] == 200):
            for node in response['json']['Members']:
                master_nodes.append('http://'+node['Addr']+':'+self._http_port)
        else:
            _LOGGER.error("Error getting Nomad Master nodes addresses: %s: %s" % (response['status_code'], response['text']))
        return master_nodes

    def _is_Client_runningAJob (self, server_node, client_id):
        url = server_node + self._api_version + self._api_url_get_clients_allocations.replace('$CLIENT_ID$', client_id )
        response = self._create_request('GET', url) 
        if (response['status_code'] == 200):
            for alloc in response['json']:
                if alloc['ClientStatus'] in ['pending', 'running']:
                    #_LOGGER.debug("_is_Client_runningAJob is TRUE")
                    return True
        else:
            _LOGGER.error("Error getting information about allocations of client with ID=%s from Master node with URL=%s: %s: %s" % (client_id, server_node, response['status_code'], response['text']))
        return False

    def _get_Clients_by_Master(self, server_node):
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
            _LOGGER.error("Error getting information about the Clients of the Master node with URL=%s: %s: %s" % (server_node, response['status_code'], response['text']))
        return clients

    def _get_Client_address(self, server_node, client_id):
        addr = None
        url = server_node + self._api_version + self._api_url_get_clients_info.replace('$CLIENT_ID$', client_id )
        response = self._create_request('GET', url)
        if (response['status_code'] == 200):
            addr = response['json']['HTTPAddr'] 
        else:
            _LOGGER.error("Error getting client_addr from Master_url=%s and Client_ID=%s: %s: %s" % (server_node, client_id, response['status_code'], response['text']))
        return addr

    def get_nodeinfolist(self):
        ##_LOGGER.info("***** START - get_nodeinfolist ***** ")
        nodeinfolist = collections.OrderedDict()
        clients_by_server = {}

        # Default values
        infile = open_file(self._nodes_info_file )
        if infile:
            for line in infile:
                info_node = {}
                info_node['name'] = line.rstrip('\n')
                info_node['status'] = self._state_off
                info_node['node_class'] = ''
                info_node['status_description'] = 'Node is OFF'
                info_node['any_job_is_running'] = False
                nodeinfolist[ info_node['name'] ] = self._get_NodeInfo(info_node)
            infile.close()

        # Obtain server nodes
        master_nodes = self._get_Master_nodes()       
        
        # Obtain ID, Name, Status and if the Client is running some job 
        for server_node in master_nodes:
            clients_by_server[ server_node ] = self._get_Clients_by_Master(server_node)
            # Obtain Resources and Queues
            for client_id in clients_by_server[ server_node ]:
                info_client = clients_by_server[ server_node ][ client_id ]
                # Client is ON
                if (info_client['state'] in [NodeInfo.IDLE, NodeInfo.USED]):
                    # Obtain Client node address 
                    client_addr = self._get_Client_address(server_node, client_id)
                    if client_addr:
                        # Querying Client node for getting the status
                        url = 'http://' + client_addr + self._api_version + self._api_url_get_clients_status
                        response = self._create_request('GET', url) 
                        if (response['status_code'] == 200):
                            info_client['client_status'] = response['json']
                        else:
                            _LOGGER.error("Error getting client_status from Client_url=%s: %s: %s" % (client_addr, response['status_code'], response['text']))

                if not (nodeinfolist[ info_client['name'] ].state in [NodeInfo.IDLE, NodeInfo.USED]):
                    nodeinfolist[ info_client['name'] ] = self._get_NodeInfo(info_client)
        
        for key, value in nodeinfolist.items():
            string = "%s + keywords={ " % (str(value) ) 
            for key2 in value.keywords:
                string += key2 + ":" + str(value.keywords[key2]) +","
            string = string[:-1]  + "}"
            _LOGGER.debug( string )
        ##_LOGGER.info("***** END - get_nodeinfolist ***** ")
        return nodeinfolist

    def _get_Jobs_by_Master(self, server_node):
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
                    jobs[ job['ID'] ]['TaskGroups'][taskgroup_id]['queue'] = self._queues[0]
                    # Check state
                    jobs[ job['ID'] ]['TaskGroups'][taskgroup_id]['state'] = Request.UNKNOWN
                    if tasks_info['Queued'] > 0 or tasks_info['Starting'] > 0:
                        jobs[ job['ID'] ]['TaskGroups'][taskgroup_id]['state'] = Request.PENDING
                    else: 
                        jobs[ job['ID'] ]['TaskGroups'][taskgroup_id]['state'] = Request.SERVED
        else:
            _LOGGER.error("Error getting jobs from Master node with URL = %s: %s: %s" % (server_node, response['status_code'], response['text']))
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
                            jobs[job_id]['TaskGroups'][taskgroup_id]['queue'] = self._queues[0]
                            _LOGGER.warning("No '%s' contraint for taskgroup '%s' of the job '%s' or it isn't a valid queue from Master node with URL=%s. The queue of this job will be '%s'" % (self._queue_constraint_target, taskgroup_id, job_id, server_node, self._queues[0]))

                        # Obtain Resources of the taskgroup
                        jobs[job_id]['TaskGroups'][taskgroup_id]['cpu'] = 0.0
                        jobs[job_id]['TaskGroups'][taskgroup_id]['memory'] = 0.0
                        if len(task_group['Tasks']) > 1:
                            _LOGGER.warning( "Taskgroup '%s' of job '%s' has got multiple tasks and this plugin doesn't support this. " % (taskgroup_id, job_id) )
                        for task in task_group['Tasks']:
                            jobs[job_id]['TaskGroups'][taskgroup_id]['cpu'] += float(task['Resources']['CPU']) / 1000.0
                            jobs[job_id]['TaskGroups'][taskgroup_id]['memory'] += float(task['Resources']['MemoryMB'] * 1024 * 1024 )

                    
            else:
                _LOGGER.error("Error getting job information with job_id = %s from Master node with URL = %s: %s: %s" % (job_id, server_node, response['status_code'], response['text']))  
                # Default values
                for taskgroup_id in jobs[job_id]['TaskGroups'].keys():
                    jobs[job_id]['TaskGroups'][taskgroup_id]['cpu'] = 0.0
                    jobs[job_id]['TaskGroups'][taskgroup_id]['memory'] = 0.0
                    jobs[job_id]['TaskGroups'][taskgroup_id]['queue'] = self._queues[0]

        return jobs      

    def _get_JobInfo(self, info):
        
        queue = '"' + info['queue'] + '" in queues'
        taskcount = 1 

        resources = ResourcesNeeded(info['cpu'], info['memory'], [queue], taskcount)
        
        job_info = JobInfo(resources, info['name'], 1)
        
        # Set state
        job_info.set_state(info['state'])
        
        return job_info

    def get_jobinfolist(self):
        taskinfolist = []
        jobs_by_server = {}

        # Obtain server nodes
        master_nodes = self._get_Master_nodes()       
        
        # Obtain jobs id
        for server_node in master_nodes:
            # Obtain job_id, job_name, taskgroup name and taskgroup state
            jobs_by_server[ server_node ] = self._get_Jobs_by_Master(server_node)

            # Obtain task resources for each taskgroup 
            jobs_by_server[server_node] = self._get_TaskGroup_resources(jobs_by_server[server_node], server_node)

            for job_id in jobs_by_server[server_node]:
                for taskgroup_id in jobs_by_server[server_node][job_id]['TaskGroups']:
                    taskinfolist.append( self._get_JobInfo( jobs_by_server[server_node][job_id]['TaskGroups'][taskgroup_id] ) )
                    _LOGGER.debug(" *JOB* - task_name = %s, cpu = %.2f, memory = %.2f, queue = %s and state = %d " % ( jobs_by_server[server_node][job_id]['TaskGroups'][taskgroup_id]['name'], jobs_by_server[server_node][job_id]['TaskGroups'][taskgroup_id]['cpu'], jobs_by_server[server_node][job_id]['TaskGroups'][taskgroup_id]['memory'], jobs_by_server[server_node][job_id]['TaskGroups'][taskgroup_id]['queue'], jobs_by_server[server_node][job_id]['TaskGroups'][taskgroup_id]['state'] ) )

        return taskinfolist
    
if __name__ == '__main__':
    pass
