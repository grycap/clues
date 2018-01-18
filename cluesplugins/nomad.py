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

'''
http + https
'''

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

        response = None
        retries = 0
        ok = False
        while (self._max_retries > retries) and (not ok) :
            retries += 1
            try: 
                response =  requests.request(method, url, verify=False, headers=headers, data=body, auth=auth)
                ok=True
            except requests.exceptions.ConnectionError:
                _LOGGER.error("Cannot connect to %s , waiting 5 seconds..." % (url))
                time.sleep(5)

        if not ok:
            _LOGGER.error("Cannot connect to %s . Retries: %s" % (url, retries ))

        return response

    def __init__(self, NOMAD_NODES_LIST_CLUES=None, NOMAD_SERVER=None, NOMAD_HEADERS=None, NOMAD_API_VERSION=None, NOMAD_API_URL_GET_ALLOCATIONS=None, NOMAD_API_URL_GET_SERVERS=None, NOMAD_API_URL_GET_CLIENTS=None, NOMAD_API_URL_GET_CLIENT_INFO=None, MAX_RETRIES=None, NOMAD_ACL_TOKEN=None, NOMAD_AUTH_DATA=None, NOMAD_API_URL_GET_CLIENT_STATUS=None, NOMAD_STATE_OFF=None, NOMAD_STATE_ON=None, NOMAD_PRIVATE_HTTP_PORT=None, NOMAD_API_URL_GET_JOBS=None, NOMAD_API_URL_GET_JOBS_INFO=None, NOMAD_API_URL_GET_ALLOCATION_INFO=None):

        config_nomad = cpyutils.config.Configuration(
            "NOMAD",
            {
                "NOMAD_SERVER": "http://localhost:10000",
                "NOMAD_HEADERS": {},
                "NOMAD_API_VERSION": "/v1",
                "NOMAD_API_URL_GET_SERVERS": "/agent/members", # master server
                "NOMAD_API_URL_GET_CLIENTS": "/nodes", # master server
                "NOMAD_API_URL_GET_CLIENT_INFO": "/node", # master server
                "NOMAD_API_URL_GET_CLIENT_STATUS": "/client/stats", # client node
                "NOMAD_API_URL_GET_ALLOCATIONS": "/allocations", # master node
                "NOMAD_API_URL_GET_JOBS": "/jobs", # master node
                "NOMAD_API_URL_GET_JOBS_INFO": "/job", # master node
                "NOMAD_API_URL_GET_ALLOCATION_INFO": "/allocation", # master node
                "NOMAD_ACL_TOKEN": None,
                "MAX_RETRIES": 100,
                "NOMAD_AUTH_DATA": None,
                "NOMAD_STATE_OFF": "down",
                "NOMAD_STATE_ON": "ready",
                "NOMAD_PRIVATE_HTTP_PORT": "4646",
                "NOMAD_NODES_LIST_CLUES": "/etc/clues2/nomad_vnodes.info"
            }
        )

        self._server_url = Helpers.val_default(NOMAD_SERVER, config_nomad.NOMAD_SERVER)
        self._headers = Helpers.val_default(NOMAD_HEADERS, config_nomad.NOMAD_HEADERS)
        self._api_version = Helpers.val_default(NOMAD_API_VERSION, config_nomad.NOMAD_API_VERSION)
        self._api_url_get_allocations = Helpers.val_default(NOMAD_API_URL_GET_ALLOCATIONS, config_nomad.NOMAD_API_URL_GET_ALLOCATIONS)
        self._api_url_get_allocation_info = Helpers.val_default(NOMAD_API_URL_GET_ALLOCATION_INFO, config_nomad.NOMAD_API_URL_GET_ALLOCATION_INFO)
        self._api_url_get_jobs = Helpers.val_default(NOMAD_API_URL_GET_JOBS, config_nomad.NOMAD_API_URL_GET_JOBS)
        self._api_url_get_jobs_info = Helpers.val_default(NOMAD_API_URL_GET_JOBS_INFO, config_nomad.NOMAD_API_URL_GET_JOBS_INFO)
        self._api_url_get_servers = Helpers.val_default(NOMAD_API_URL_GET_SERVERS, config_nomad.NOMAD_API_URL_GET_SERVERS)
        self._api_url_get_clients = Helpers.val_default(NOMAD_API_URL_GET_CLIENTS, config_nomad.NOMAD_API_URL_GET_CLIENTS)
        self._api_url_get_clients_info = Helpers.val_default(NOMAD_API_URL_GET_CLIENT_INFO, config_nomad.NOMAD_API_URL_GET_CLIENT_INFO)
        self._api_url_get_clients_status = Helpers.val_default(NOMAD_API_URL_GET_CLIENT_STATUS, config_nomad.NOMAD_API_URL_GET_CLIENT_STATUS)
        self._max_retries = Helpers.val_default(MAX_RETRIES, config_nomad.MAX_RETRIES)
        self._acl_token = Helpers.val_default(NOMAD_ACL_TOKEN, config_nomad.NOMAD_ACL_TOKEN)
        self._auth_data = Helpers.val_default(NOMAD_AUTH_DATA, config_nomad.NOMAD_AUTH_DATA)
        self._state_off = Helpers.val_default(NOMAD_STATE_OFF, config_nomad.NOMAD_STATE_OFF)
        self._state_on = Helpers.val_default(NOMAD_STATE_ON, config_nomad.NOMAD_STATE_ON)
        self._http_port = Helpers.val_default(NOMAD_PRIVATE_HTTP_PORT, config_nomad.NOMAD_PRIVATE_HTTP_PORT)
        self._nodes_info_file = Helpers.val_default(NOMAD_NODES_LIST_CLUES, config_nomad.NOMAD_NODES_LIST_CLUES)

        LRMS.__init__(self, "TOKEN_%s" % self._server_url)

    def _get_NodeInfo (self, info_node):
        name = info_node['name']

        # Illustrative values for Clues, since the node is not running, we cannot know the real values
        slots_count = 1.0
        slots_free = 1.0
        memory_total = get_memory_in_bytes('1024Mi')
        memory_free = memory_total
        
        # Check state
        state = NodeInfo.OFF
        if (info_node['status'] == self._state_on):
            state = NodeInfo.IDLE

        # Keywords
        keywords = {}
        keywords['hostname'] = TypedClass.auto(name)
        queues = ["default"]
        if (queues):
            keywords['queues'] = TypedList([TypedClass.auto(q) for q in queues])      

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
        if (response.status_code == 200):
            members = response.json()['Members']
            for node in members:
                master_nodes.append('http://'+node['Addr']+':'+self._http_port)
        else:
            _LOGGER.error("Error getting Nomad Master nodes addresses: %s: %s" % (response.status_code, response.text))
        return master_nodes

    def _get_Clients_by_Master(self, server_node):
        clients = {}
        url = server_node + self._api_version + self._api_url_get_clients
        response = self._create_request('GET', url) 
        if (response.status_code == 200):
            for client in response.json():
                clients[ client['ID'] ] = {}
                clients[ client['ID'] ]['name'] = client['Name'] 
                clients[ client['ID'] ]['status'] = client['Status'] 
                clients[ client['ID'] ]['status_description'] = client['StatusDescription'] 
                clients[ client['ID'] ]['state'] = NodeInfo.OFF
                if (client['Status'] == self._state_on):
                    clients[ client['ID'] ]['state'] = NodeInfo.IDLE
        else:
            _LOGGER.error("Error getting client_id from Master node with URL=%s: %s: %s" % (server_node, response.status_code, response.text))
        
        return clients

    def _get_Client_address(self, server_node, client_id):
        addr = ''
        url = server_node + self._api_version + self._api_url_get_clients_info + '/' + client_id
        response = self._create_request('GET', url) 
        if (response.status_code == 200):
            addr = response.json()['HTTPAddr'] 
        else:
            _LOGGER.error("Error getting client_addr from Master_url=%s and Client_ID=%s: %s: %s" % (server_node, client_id, response.status_code, response.text))
        return addr

    def _get_Client_status( self, client_addr):
        client_status = {}
        url = 'http://' + client_addr + self._api_version + self._api_url_get_clients_status
        response = self._create_request('GET', url) 
        if (response.status_code == 200):
            client_status = response.json()
        else:
            _LOGGER.error("Error getting client_status from Client_url=%s: %s: %s" % (client_addr, response.status_code, response.text))
        return client_status

    def get_nodeinfolist(self):
        nodeinfolist = collections.OrderedDict()
        clients_by_server = {}

        infile = open_file(self._nodes_info_file )
        if infile:
            for line in infile:
                info_node = {}
                info_node['name'] = line.rstrip('\n')
                info_node['status'] = self._state_off
                info_node['status_description'] = 'Node is OFF'
                nodeinfolist[ info_node['name'] ] = self._get_NodeInfo(info_node)
            infile.close()

        # Obtain server nodes
        master_nodes = self._get_Master_nodes()       
        
        # Obtain ID, Name and Status 
        for server_node in master_nodes:
            clients_by_server[ server_node ] = self._get_Clients_by_Master(server_node)

        for server_node in clients_by_server:
            for clientID in clients_by_server[ server_node ]:
                info_client = clients_by_server[ server_node ][ clientID ]
                if (info_client['state'] == NodeInfo.IDLE) or (info_client['state'] == NodeInfo.USED ): # Client is ON
                    # Obtain Client node address 
                    client_addr = self._get_Client_address(server_node, clientID)
                    if (client_addr != ''):
                        info_client['address'] = client_addr
                        # Querying Client node for getting the status
                        client_status = self._get_Client_status( client_addr)
                        if (client_status != {}):
                            info_client['client_status'] = client_status

                nodeinfolist[ info_client['name'] ] = self._get_NodeInfo(info_client)

        return nodeinfolist

    def _get_JobInfo(self, info):
        # Default?
        queue = '"default" in queues'
        taskcount = 1

        resources = ResourcesNeeded(info['cpu'], info['memory'], [queue], taskcount)
        job_info = JobInfo(resources, info[ 'alloc_id' ], 1)
        
        # Set state
        job_state = Request.UNKNOWN
        if info['status'] == "pending":
            job_state = Request.PENDING
        elif info['status'] in ["running", "complete", "failed"]:
            job_state = Request.SERVED
        elif info['status'] == "lost":
            job_state = Request.DISSAPEARED
        job_info.set_state(job_state)
        
        return job_info

    def _get_Allocations_by_Master(self, server_node):
        allocations = {}
        url = server_node + self._api_version + self._api_url_get_allocations
        response = self._create_request('GET', url) 
        if (response.status_code == 200):
            for alloc in response.json():
                allocations[ alloc['ID'] ]={}
                allocations[ alloc['ID'] ]['status'] = alloc['ClientStatus']
                allocations[ alloc['ID'] ]['status_description'] = alloc['ClientDescription']
                allocations[ alloc['ID'] ]['job_id'] = alloc['JobID']
                allocations[ alloc['ID'] ]['node_id'] = alloc['NodeID'] 
                allocations[ alloc['ID'] ]['alloc_id'] = alloc['ID']
        else:
            _LOGGER.error("Error getting alloc_id from Master node with URL=%s: %s: %s" % (server_node, response.status_code, response.text))
        return allocations
    
    def _get_Allocation_status( self, server_node, alloc_id):
        info = {}
        url = server_node + self._api_version + self._api_url_get_allocation_info + '/' + alloc_id
        response = self._create_request('GET', url) 
        if (response.status_code == 200):
            info = response.json()
        else:
            _LOGGER.error("Error getting information about allocation=%s from Master node with URL=%s: %s: %s" % (alloc_id, server_node, response.status_code, response.text))
        return info

    def get_jobinfolist(self):
        jobinfolist = []
        allocations_by_server = {}
        # Obtain server nodes
        master_nodes = self._get_Master_nodes()       
        
        # Obtain allocations id
        for server_node in master_nodes:
            allocations_by_server[ server_node ] = self._get_Allocations_by_Master(server_node)
            if (allocations_by_server[ server_node ] != {}): # Clients of the server have got allocations 
                for alloc_id, info_alloc in allocations_by_server[ server_node].items():
                    info = self._get_Allocation_status(server_node, alloc_id)
                    info_alloc['cpu'] = 0.0
                    info_alloc['memory'] = 0.0
                    if (info != {}):
                        if (info['Resources']['CPU'] != 0):
                            info_alloc['cpu'] = float(info['Resources']['CPU']) / 1000.0
                        if (info['Resources']['MemoryMB'] != 0):
                            info_alloc['memory'] = float(info['Resources']['MemoryMB'] * 1024 * 1024 )
                    
                    jobinfolist.append( self._get_JobInfo(info_alloc) )

        return jobinfolist

if __name__ == '__main__':
    pass
