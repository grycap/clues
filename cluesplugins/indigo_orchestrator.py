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
'''
Created on 26/1/2015

@author: micafer
'''

import yaml
import time
import json
import logging
import httplib
import base64
import string
from urlparse import urlparse

import cpyutils.db
import cpyutils.config
import cpyutils.eventloop
from clueslib.node import Node
from clueslib.platform import PowerManager

_LOGGER = logging.getLogger("[PLUGIN-INDIGO-ORCHESTRATOR]")

class powermanager(PowerManager):

	class VM_Node:	
		def __init__(self, vm_id):
			self.vm_id = vm_id
			self.timestamp_recovered = 0
			self.timestamp_created = self.timestamp_seen = cpyutils.eventloop.now()

		def seen(self):
			self.timestamp_seen = cpyutils.eventloop.now()
			# _LOGGER.debug("seen %s" % self.vm_id)

		def recovered(self):
			self.timestamp_recovered = cpyutils.eventloop.now()

	def __init__(self):
		#
		# NOTE: This fragment provides the support for global config files. It is a bit awful.
		#	   I do not like it because it is like having global vars. But it is managed in
		#	   this way for the sake of using configuration files
		#
		config_indigo = cpyutils.config.Configuration(
			"INDIGO ORCHESTRATOR",
			{
				"INDIGO_ORCHESTRATOR_URL": "http://172.30.15.43:8080",
				"INDIGO_ORCHESTRATOR_DEPLOY_ID": None,
				"INDIGO_ORCHESTRATOR_MAX_INSTANCES": 0,
				"INDIGO_ORCHESTRATOR_FORGET_MISSING_VMS": 30,
				"INDIGO_ORCHESTRATOR_DROP_FAILING_VMS": 30,
				"INDIGO_ORCHESTRATOR_WN_NAME": "vnode-#N#",
				"INDIGO_ORCHESTRATOR_DB_CONNECTION_STRING": "sqlite:///var/lib/clues2/clues.db",
				"INDIGO_ORCHESTRATOR_PAGE_SIZE": 20
			}
		)

		self._INDIGO_ORCHESTRATOR_URL = config_indigo.INDIGO_ORCHESTRATOR_URL
		self._INDIGO_ORCHESTRATOR_DEPLOY_ID = config_indigo.INDIGO_ORCHESTRATOR_DEPLOY_ID
		self._INDIGO_ORCHESTRATOR_MAX_INSTANCES = config_indigo.INDIGO_ORCHESTRATOR_MAX_INSTANCES	
		self._INDIGO_ORCHESTRATOR_FORGET_MISSING_VMS = config_indigo.INDIGO_ORCHESTRATOR_FORGET_MISSING_VMS
		self._INDIGO_ORCHESTRATOR_DROP_FAILING_VMS = config_indigo.INDIGO_ORCHESTRATOR_DROP_FAILING_VMS
		self._INDIGO_ORCHESTRATOR_WN_NAME = config_indigo.INDIGO_ORCHESTRATOR_WN_NAME
		self._INDIGO_ORCHESTRATOR_PAGE_SIZE = config_indigo.INDIGO_ORCHESTRATOR_PAGE_SIZE

		# TODO: to specify the auth data to access the orchestrator
		self._auth_data = None

		self._inf_id = None
		self._master_node_id = None
		# Structure for the recovery of nodes
		self._db = cpyutils.db.DB.create_from_string(config_indigo.INDIGO_ORCHESTRATOR_DB_CONNECTION_STRING)
		self._create_db()
		self._mvs_seen = self._load_mvs_seen()

	def _get_auth_header(self):
		auth_header = None
		# This is an example of the header to add
		# other typical option "X-Auth-Token"
		if self._auth_data and 'username' in self._auth_data and 'password' in self._auth_data:
			passwd = self._auth_data['password']
			user = self._auth_data['username'] 
			auth_header = { 'Authorization' : 'Basic ' + string.strip(base64.encodestring(user + ':' + passwd))}

		return auth_header

	def _get_http_connection(self):
		"""
		Get the HTTPConnection object to contact the orchestrator API

		Returns(HTTPConnection or HTTPSConnection): HTTPConnection connection object
		"""

		url = urlparse(self._INDIGO_ORCHESTRATOR_URL)
		
		if url[0] == 'https':
			conn = httplib.HTTPSConnection(url[1])
		elif url[0] == 'http':
			conn = httplib.HTTPConnection(url[1])

		return conn
	
	def _get_inf_id(self):
		return self._INDIGO_ORCHESTRATOR_DEPLOY_ID
	
	def _get_nodename_from_uuid(self, uuid):
		for node_name, vm in self._mvs_seen.items():
			if vm.vm_id== uuid:
				return node_name
		return None
	
	def _get_uuid_from_nodename(self, nodename):
		for node_name, vm in self._mvs_seen.items():
			if node_name == nodename:
				return vm.vm_id
		return None
	
	def _get_master_node_id(self, resources):
		if not self._master_node_id:
			older_resource = None
			# if this plugin is used after year 5000 please change this
			last_time = time.strptime("5000-12-01T00:00", "%Y-%m-%dT%H:%M") 
			for resource in resources:
				# date format: 2016-02-04T10:43+0000
				creation_time = time.strptime(resource['creationTime'][:-5], "%Y-%m-%dT%H:%M")
				if creation_time < last_time:
					last_time = creation_time
					older_resource = resource
			
			self._master_node_id = older_resource['uuid']

		return self._master_node_id
	
	def _get_resources_page(self, page=0):
		inf_id = self._get_inf_id()
		headers = {'Accept': 'application/json', 'Content-Type' : 'application/json', 'Connection':'close'}
		auth = self._get_auth_header()
		if auth:
			headers.update(auth)
		conn = self._get_http_connection()
		conn.request('GET', "/orchestrator/deployments/%s/resources?size=%d&page=%d" % (inf_id, self._INDIGO_ORCHESTRATOR_PAGE_SIZE, page), headers = headers)
		resp = conn.getresponse()
		output = resp.read()
		conn.close()
		return resp.status, output

	def _get_resources(self):
		try:
			status, output = self._get_resources_page()
			
			resources = []
			if status != 200:
				_LOGGER.error("ERROR getting deployment info: %s" % str(output))
			else:
				res = json.loads(output)
				if 'content' in res:
					resources.extend(res['content'])

				if 'page' in res and res['page']['totalPages'] > 1:
					for page in range(1,res['page']['totalPages']):
						status, output = self._get_resources_page(page)

						if status != 200:
							_LOGGER.error("ERROR getting deployment info: %s, page %d" % (str(output), page))
						else:
							res = json.loads(output)
							if 'content' in res:
								resources.extend(res['content'])

				return [resource for resource in resources if resource['toscaNodeType'] == "tosca.nodes.indigo.Compute"]

			return resources
		except:
			_LOGGER.exception("ERROR getting deployment info.")
			return []
	
	def _get_vms(self):
		now = cpyutils.eventloop.now()
		resources = self._get_resources()
		
		if not resources:
			_LOGGER.warn("No resources obtained from orchestrator.")
		else:
			for resource in resources:
				if resource['uuid'] != self._get_master_node_id(resources):
					vm = self.VM_Node(resource['uuid'])
					status  = resource['status']
					# Possible status
					# CREATE_IN_PROGRESS, CREATE_COMPLETE, CREATE_FAILED, UPDATE_IN_PROGRESS, UPDATE_COMPLETE
					# UPDATE_FAILED, DELETE_IN_PROGRESS, DELETE_COMPLETE, DELETE_FAILED, UNKNOWN

					# The name of the associated node has been stored in VM launch 
					node_name = self._get_nodename_from_uuid(vm.vm_id)
					
					if not node_name:
						_LOGGER.error("No node name obtained for VM ID: %s" % vm.vm_id)
						self._power_off("noname", vm.vm_id)
						break
					elif node_name not in self._mvs_seen:
						# This must never happen but ...
						_LOGGER.warn("Node name %s not in the list of seen VMs." % node_name)
						self._mvs_seen[node_name] = vm
					else:
						self._mvs_seen[node_name].seen()
					
					if status in ["CREATE_FAILED", "UPDATE_FAILED", "DELETE_FAILED"]:
						# This VM is in a "terminal" state remove it from the infrastructure 
						_LOGGER.error("Node %s in VM with id %s is in state: %s, msg: %s." % (node_name, vm.vm_id, status, resource['statusReason']))
						self.recover(node_name)
					elif status in ["UNKNOWN"]:
						# Do not terminate this VM, let's wait to lifecycle to check if it must be terminated 
						_LOGGER.warn("Node %s in VM with id %s is in state: %s" % (node_name, vm.vm_id, status))

		# from the nodes that we have powered on, check which of them are still running
		for nname, node in self._mvs_seen.items():
			if (now - node.timestamp_seen) > self._INDIGO_ORCHESTRATOR_FORGET_MISSING_VMS:
				_LOGGER.debug("vm %s is not seen for a while... let's forget it" % nname)
				self._delete_mvs_seen(nname)

		return self._mvs_seen
	
	def _recover_ids(self, nodenames):
		for nodename in nodenames:
			self.power_off(nodename)

	def recover(self, nname):
		success, nname = self.power_off(nname)
		if success:
			return Node.OFF
		
		return False

	def lifecycle(self):
		try:
			monitoring_info = self._clues_daemon.get_monitoring_info()
			now = cpyutils.eventloop.now()
	
			vms = self._get_vms()
				
			recover = []
			# To store the name of the nodes to use it in the third case
			node_names = []
	
			# Two cases: (1) a VM that is on in the monitoring info, but it is not seen in IM; and (2) a VM that is off in the monitoring info, but it is seen in IM
			for node in monitoring_info.nodelist:
				node_names.append(node.name)
				if node.enabled:
					if node.state in [Node.OFF, Node.OFF_ERR, Node.UNKNOWN]:
						if self._INDIGO_ORCHESTRATOR_DROP_FAILING_VMS > 0:
							if node.name in vms:
								vm = vms[node.name]
								time_off = now - node.timestamp_state
								time_recovered = now - vm.timestamp_recovered
								_LOGGER.warning("node %s has a VM running but it is OFF or UNKNOWN in the monitoring system since %d seconds" % (node.name, time_off))
								if time_off > self._INDIGO_ORCHESTRATOR_DROP_FAILING_VMS:
									if time_recovered > self._INDIGO_ORCHESTRATOR_DROP_FAILING_VMS:
										_LOGGER.warning("Trying to recover it (state: %s)" % node.state)
										vm.recovered()
										recover.append(node.name)
									else:
										_LOGGER.debug("node %s has been recently recovered %d seconds ago. Do not recover it yet." % (node.name, time_recovered))
					else:
						if node.name not in vms:
							# This may happen because it is launched by hand using other credentials than those for the user used for IM (and he cannot manage the VMS)
							_LOGGER.warning("node %s is detected by the monitoring system, but there is not any VM associated to it (there are any problem connecting with the Orchestrator?)" % node.name)
	
			# A third case: a VM that it is seen in Orchestrator but does not correspond to any node in the monitoring info
			# This is a strange case but we assure not to have uncontrolled VMs
			for name in vms:
				vm = vms[name]
				if name not in node_names:
					_LOGGER.warning("VM with name %s is detected by the Orchestrator but it does not exist in the monitoring system... recovering it.)" % name)
					vm.recovered()
					recover.append(node.name)
	
			self._recover_ids(recover)
		except:
			_LOGGER.exception("Error executing lifecycle of INDIGO Orchestrator PowerManager.")

		return PowerManager.lifecycle(self)

	def _create_db(self):
		try:
			result, _, _ = self._db.sql_query("CREATE TABLE IF NOT EXISTS orchestrator_vms(node_name varchar(128) PRIMARY KEY, uuid varchar(128))", True)
		except:
			_LOGGER.exception("Error creating INDIGO orchestrator plugin DB. The data persistence will not work!")
			result = False			
		return result

	def _delete_mvs_seen(self, nname):
		del self._mvs_seen[nname]
		try:
			self._db.sql_query("DELETE FROM orchestrator_vms WHERE node_name = %s" % nname,True)
		except:
			_LOGGER.exception("Error trying to save INDIGO orchestrator plugin data.")
		
	def _add_mvs_seen(self, nname, vm):
		self._mvs_seen[nname] = vm
		try:
			self._db.sql_query("INSERT INTO orchestrator_vms VALUES ('%s', '%s')" % (nname, vm.vm_id),True)
		except:
			_LOGGER.exception("Error trying to save INDIGO orchestrator plugin data.")

	def _load_mvs_seen(self):
		res = {}
		try:
			result, _, rows = self._db.sql_query("select * from orchestrator_vms")
			if result:
				for (node_name, uuid) in rows:
					self._mvs_seen[node_name] = self.VM_Node(uuid)
		except:
			_LOGGER.exception("Error trying to load INDIGO orchestrator plugin data.")

		return res

	def _modify_deployment(self, vms, remove_node = None):
		inf_id = self._get_inf_id()

		conn = self._get_http_connection()
		conn.putrequest('PUT', "/orchestrator/deployments/%s" % inf_id)
		auth_header = self._get_auth_header()
		if auth_header:
			conn.putheader(auth_header.keys()[0], auth_header.values()[0])
		conn.putheader('Accept', 'application/json')
		conn.putheader('Content-Type', 'application/json')
		conn.putheader('Connection', 'close')
		
		template = self._get_template(len(vms), remove_node)
		body = '{ "template": "%s" }' % template.replace('"','\"').replace('\n','\\n')

		conn.putheader('Content-Length', len(body))
		conn.endheaders(body)

		resp = conn.getresponse()
		output = str(resp.read())
		conn.close()

		return resp.status,  output

	def power_on(self, nname):
		try:
			vms = self._mvs_seen
			
			if nname in vms:
				_LOGGER.warn("Trying to launch an existing node %s. Ignoring it." % nname)
				return True, nname
			
			if len(vms) >= self._INDIGO_ORCHESTRATOR_MAX_INSTANCES:
				_LOGGER.debug("There are %d VMs running, we are at the maximum number. Do not power on." % len(vms))
				return False, nname

			resp_status, output = self._modify_deployment(vms)
			
			if resp_status not in [200, 201, 202, 204]:
				_LOGGER.error("Error launching node %s: %s" % (nname, output))
				return False, nname
			else:
				nname = self._INDIGO_ORCHESTRATOR_WN_NAME.replace("#N#", str(len(vms)+1))
				_LOGGER.debug("Node %s successfully created" % nname)
				#res = json.loads(output)
				
				# wait to assure the orchestrator process the operation
				delay = 2
				wait = 0
				timeout = 30
				new_uuids = []
				while not new_uuids and wait < timeout:
					# Get the list of resources now to get the new vm added
					resources = self._get_resources()
					current_uuids = [vm.vm_id for vm in vms]
					for resource in resources:
						if resource['uuid'] != self._get_master_node_id(resources) and resource['uuid'] not in current_uuids:
							new_uuids.append(resource['uuid'])
					if len(new_uuids) < 1:
						time.sleep(delay)
						wait += delay
				
				if len(new_uuids) != 1:
					_LOGGER.error("Trying to get the uuid of the new node and get %d uuids!!" % len(new_uuids))
					return False, nname
				elif len(new_uuids) > 0: 
					self._add_mvs_seen(nname, self.VM_Node(new_uuids[0]))
				
				return True, nname
		except:
			_LOGGER.exception("Error launching node %s " % nname)
			return False, nname

	def _power_off(self, nname, vmid):
		try:
			success = False
	
			resp_status, output = self._modify_deployment(self._mvs_seen, str(vmid))

			if resp_status not in [200, 201, 202, 204]:
				_LOGGER.error("ERROR deleting node: %s: %s" % (nname,output))
			else:
				_LOGGER.debug("Node %s successfully deleted." % nname)
				success = True
		except:
			_LOGGER.exception("Error powering off node %s " % nname)

		return success, nname

	def power_off(self, nname):
		_LOGGER.debug("Powering off %s" % nname)
		vmid = self._get_uuid_from_nodename(nname)
		if not vmid:
			_LOGGER.error("There is not any VM associated to node %s." % nname)
			return False, nname
		else:
			return self._power_off(nname, vmid)

	def _get_template(self, count, remove_node = None):		
		inf_id = self._get_inf_id()
		headers = {'Accept': 'text/plain', 'Connection':'close'}
		auth = self._get_auth_header()
		if auth:
			headers.update(auth)
		conn = self._get_http_connection()
		conn.request('GET', "/orchestrator/deployments/%s/template" % inf_id, headers = headers)
		resp = conn.getresponse()
		output = resp.read()
		conn.close()

		if resp.status != 200:
			_LOGGER.error("ERROR getting deployment template: %s" % str(output))
			return None
		else:
			templateo = yaml.load(output)
			if remove_node:
				if count < 1:
					count = 1
				templateo['topology_template']['node_templates']['torque_wn']['capabilities']['scalable']['properties']['count'] = count - 1
				templateo['topology_template']['node_templates']['torque_wn']['capabilities']['scalable']['properties']['removal_list'] = [remove_node]
			else:
				templateo['topology_template']['node_templates']['torque_wn']['capabilities']['scalable']['properties']['count'] = count + 1

		return yaml.dump(templateo)
