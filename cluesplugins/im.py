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

import xmlrpclib
from uuid import uuid1

from radl import radl_parse
from IM.VirtualMachine import VirtualMachine

import cpyutils.db
import cpyutils.config
import cpyutils.eventloop
from clueslib.node import Node
from clueslib.platform import PowerManager

import cpyutils.log
_LOGGER = cpyutils.log.Log("PLUGIN-IM")

class powermanager(PowerManager):

	class VM_Node:	
		def __init__(self, vm_id, radl):
			self.vm_id = vm_id
			self.radl = radl
			self.timestamp_recovered = 0
			self.timestamp_created = self.timestamp_seen = cpyutils.eventloop.now()
			self.last_state = None

		def seen(self):
			self.timestamp_seen = cpyutils.eventloop.now()
			# _LOGGER.debug("seen %s" % self.vm_id)

		def recovered(self):
			self.timestamp_recovered = cpyutils.eventloop.now()
		
		def update(self, vm_id, radl):	
			self.vm_id = vm_id
			self.radl = radl

	def __init__(self):
		#
		# NOTE: This fragment provides the support for global config files. It is a bit awful.
		#	   I do not like it because it is like having global vars. But it is managed in
		#	   this way for the sake of using configuration files
		#
		config_im = cpyutils.config.Configuration(
			"IM VIRTUAL CLUSTER",
			{
				"IM_VIRTUAL_CLUSTER_XMLRPC": "http://localhost:8899",
				"IM_VIRTUAL_CLUSTER_XMLRCP_SSL_CA_CERTS": "",
				"IM_VIRTUAL_CLUSTER_XMLRCP_SSL": False,
				"IM_VIRTUAL_CLUSTER_AUTH_DATA_FILE": "/usr/local/ec3/auth.dat",
				"IM_VIRTUAL_CLUSTER_DROP_FAILING_VMS": 30,
				"IM_VIRTUAL_CLUSTER_FORGET_MISSING_VMS": 30,
				"IM_VIRTUAL_CLUSTER_DB_CONNECTION_STRING": "sqlite:///var/lib/clues2/clues.db"
			}
		)

		self._IM_VIRTUAL_CLUSTER_AUTH_DATA_FILE = config_im.IM_VIRTUAL_CLUSTER_AUTH_DATA_FILE		
		self._IM_VIRTUAL_CLUSTER_DROP_FAILING_VMS = config_im.IM_VIRTUAL_CLUSTER_DROP_FAILING_VMS
		self._IM_VIRTUAL_CLUSTER_FORGET_MISSING_VMS = config_im.IM_VIRTUAL_CLUSTER_FORGET_MISSING_VMS
		self._IM_VIRTUAL_CLUSTER_XMLRCP_SSL = config_im.IM_VIRTUAL_CLUSTER_XMLRCP_SSL
		self._IM_VIRTUAL_CLUSTER_XMLRPC = config_im.IM_VIRTUAL_CLUSTER_XMLRPC
		self._IM_VIRTUAL_CLUSTER_XMLRCP_SSL_CA_CERTS = config_im.IM_VIRTUAL_CLUSTER_XMLRCP_SSL_CA_CERTS

		self._db = cpyutils.db.DB.create_from_string(config_im.IM_VIRTUAL_CLUSTER_DB_CONNECTION_STRING)
		self._create_db()

		# Structure for the recovery of nodes
		self._mvs_seen = {}
		self._golden_images = self._load_golden_images()
		# TODO: save this var into DB to be persistent
		self._stopped_vms = {}
		self._inf_id = None

	def _create_db(self):
		try:
			result, _, _ = self._db.sql_query("CREATE TABLE IF NOT EXISTS im_golden_images(ec3_class varchar(255) "
											  "PRIMARY KEY, image varchar(255), password varchar(255))", True)
		except:
			_LOGGER.exception(
				"Error creating IM plugin DB. The data persistence will not work!")
			result = False
		return result

	def _store_golden_image(self, ec3_class, image, password):
		try:
			self._db.sql_query("INSERT into im_golden_images values ('%s','%s','%s')" % (ec3_class,
                                                                                         image,
                                                                                         password), True)
		except:
			_LOGGER.exception("Error trying to save IM golden image data.")

	def _load_golden_images(self):
		res = {}
		try:
			result, _, rows = self._db.sql_query("select * from im_golden_images")
			if result:
				for (ec3_class, image, password) in rows:
					res[ec3_class] = image, password
			else:
				_LOGGER.error("Error trying to load IM golden images data.")
		except:
			_LOGGER.exception("Error trying to load IM golden images data.")

		return res

	def _get_inf_id(self):
		if self._inf_id is not None:
			return self._inf_id
		else:
			server = self._get_server()
			auth_data = self._read_auth_data(self._IM_VIRTUAL_CLUSTER_AUTH_DATA_FILE)
			(success, inf_list) = server.GetInfrastructureList(auth_data)
			if success:
				if len(inf_list) > 0:
					_LOGGER.debug("The IM Inf ID is %s" % inf_list[0])
					self._inf_id = inf_list[0]
					return inf_list[0]
				else:
					_LOGGER.error("Error getting infrastructure list: No infrastructure!.")
			else:
				_LOGGER.error("Error getting infrastructure list: %s" % inf_list)
				return None

	def _get_server(self):
		if self._IM_VIRTUAL_CLUSTER_XMLRCP_SSL:
			from springpython.remoting.xmlrpc import SSLClient
			return SSLClient(self._IM_VIRTUAL_CLUSTER_XMLRPC, self._IM_VIRTUAL_CLUSTER_XMLRCP_SSL_CA_CERTS)
		else:
			return xmlrpclib.ServerProxy(self._IM_VIRTUAL_CLUSTER_XMLRPC,allow_none=True)

	# From IM.auth
	@staticmethod
	def _read_auth_data(filename):
		if isinstance(filename, list):
			lines = filename
		else:
			auth_file = open(filename, 'r')
			lines = auth_file.readlines()
			auth_file.close()
	
		res = []
	
		for line in lines:
			line = line.strip()
			if len(line) > 0 and not line.startswith("#"):
				auth = {}
				tokens = line.split(";")
				for token in tokens:
					key_value = token.split(" = ")
					if len(key_value) != 2:
						break;
					else:
						value = key_value[1].strip().replace("\\n","\n")
						# Enable to specify a filename and set the contents of it
						if value.startswith("file(") and value.endswith(")"):
							filename = value[5:len(value)-1]
							try:
								value_file = open(filename, 'r')
								value = value_file.read()
								value_file.close()
							except:
								pass
						auth[key_value[0].strip()] = value
				res.append(auth)
		
		return res
	
	def _get_radl(self, nname):
		server = self._get_server()
		auth_data = self._read_auth_data(self._IM_VIRTUAL_CLUSTER_AUTH_DATA_FILE)
		(success, vm_ids) = server.GetInfrastructureInfo(self._get_inf_id(), auth_data)

		# Get all the info from RADL
		# Especial features in system:
		#- 'ec3_max_instances': maximum number of nodes with this system configuration; a negative value is like no constrain; default value is -1.
		#- 'ec3_destroy_interval': some cloud providers pay a certain amount of time in advance, like AWS EC2. The node will be destroyed only when it is idle at the end of the interval expressed by this option in seconds. The default value is 0.
		#- 'ec3_destroy_safe': seconds before the deadline set by \'ec3_destroy_interval\' that the node can be destroyed; the default value is 0.
		#- 'ec3_if_fail': name of the next system configuration to try after this fails a launch or the number of instances saturates; the default value is ''.

		vm_info = {}
		if success:
			# The first one is always the front-end node
			for vm_id in vm_ids[1:]:
				(success, radl_data)  = server.GetVMInfo(self._get_inf_id(), vm_id, auth_data)
				if success:
					radl = radl_parse.parse_radl(radl_data)
					ec3_class = radl.systems[0].getValue("ec3_class")
					if ec3_class not in vm_info:
						vm_info[ec3_class] = {}
						vm_info[ec3_class]['count'] = 0
					vm_info[ec3_class]['count'] += 1
				else:
					_LOGGER.error("Error getting VM info: %s" % radl_data)
		else:
			_LOGGER.error("Error getting infrastructure info: %s" % vm_ids)

		(success, radl_data) = server.GetInfrastructureRADL(self._get_inf_id(), auth_data)
		if success:
			radl_all = radl_parse.parse_radl(radl_data)
		else:
			_LOGGER.error("Error getting infrastructure RADL: %s" % radl_data)
			return None

		# Get info from the original RADL
		for system in radl_all.systems:
			if system.name not in vm_info:
				vm_info[system.name] = {}
				vm_info[system.name]['count'] = 0
			vm_info[system.name]['radl'] = system

		# Start with the system named "wn"
		current_system = "wn"
		while current_system:
			system_orig = vm_info[current_system]["radl"]
			ec3_max_instances = system_orig.getValue("ec3_max_instances", -1)
			if ec3_max_instances < 0:
				ec3_max_instances = 99999999
			if vm_info[current_system]["count"] < ec3_max_instances:
				# launch this system type
				new_radl = ""
				for net in radl_all.networks:
					new_radl += "network " + net.id + "\n"

				system_orig.name = nname
				system_orig.setValue("net_interface.0.dns_name", str(nname))
				system_orig.setValue("ec3_class", current_system)
				if current_system in self._golden_images:
					image, password = self._golden_images[current_system]
					system_orig.setValue("disk.0.image.url", image)
					_LOGGER.debug("A golden image for %s node is stored, using it: %s" % (current_system, image))
					if password:
						system_orig.setValue("disk.0.os.credentials.password", password)
				new_radl += str(system_orig) + "\n"
				
				for configure in radl_all.configures:
					if configure.name == current_system:
						configure.name = nname
						new_radl += str(configure) + "\n"
				
				new_radl += "deploy " + nname + " 1"
				
				return new_radl
			else:
				# we must change the system to the next one
				current_system = system_orig.getValue("ec3_if_fail", '')
				if not current_system:
					_LOGGER.error("Error: we need more instances but ec3_if_fail of system %s is empty" % system_orig.name)
		
		_LOGGER.error("Error generating infrastructure RADL")
		return None
	
	def _get_vms(self, monitoring_info=None):
		now = cpyutils.eventloop.now()
		server = self._get_server()
		auth_data = self._read_auth_data(self._IM_VIRTUAL_CLUSTER_AUTH_DATA_FILE)
		(success, vm_ids) = server.GetInfrastructureInfo(self._get_inf_id(), auth_data)
		if not success:
			_LOGGER.error("ERROR getting infrastructure info: %s" % vm_ids)
		else:
			# The first one is always the front-end node
			for vm_id in vm_ids[1:]:
				clues_node_name = None
				try:
					(success, radl_data)  = server.GetVMInfo(self._get_inf_id(), vm_id, auth_data)
					if success:
						radl = radl_parse.parse_radl(radl_data)
						clues_node_name = radl.systems[0].getValue('net_interface.0.dns_name')
						state = radl.systems[0].getValue('state')
					else:
						_LOGGER.error("ERROR getting VM info: %s" % vm_id)
				except TypeError:
					success = False
					reload(radl_parse)
					_LOGGER.exception("ERROR getting VM info: %s. Trying to reload radl_parse module." % vm_id)
				except:
					success = False
					_LOGGER.exception("ERROR getting VM info: %s" % vm_id)

				if clues_node_name and state not in [VirtualMachine.STOPPED]:
					# Create or update VM info
					if clues_node_name not in self._mvs_seen:
						self._mvs_seen[clues_node_name] = self.VM_Node(vm_id, radl)
					else:
						if self._mvs_seen[clues_node_name].vm_id != vm_id:
							# this must not happen ...
							_LOGGER.warning("Node %s in VM with id %s now have a new ID: %s" % (clues_node_name, self._mvs_seen[clues_node_name].vm_id, vm_id))
							self.power_off(clues_node_name)
						self._mvs_seen[clues_node_name].update(vm_id, radl)
					
					self._mvs_seen[clues_node_name].seen()
					last_state = self._mvs_seen[clues_node_name].last_state
					self._mvs_seen[clues_node_name].last_state = state

					if state in [VirtualMachine.FAILED, VirtualMachine.UNCONFIGURED]:
						# This VM is in "terminal" state remove it from the infrastructure 
						_LOGGER.error("Node %s in VM with id %s is in state: %s" % (clues_node_name, vm_id, state))

						if state == VirtualMachine.UNCONFIGURED:
							# in case of unconfigured show the log to make easier debug
							# but only the first time
							if last_state != VirtualMachine.UNCONFIGURED:
								(success, contmsg)  = server.GetVMContMsg(self._get_inf_id(), vm_id, auth_data)
								_LOGGER.debug("Contextualization msg: %s" % contmsg)
							# check if node is disabled and do not recover it
							enabled = True
							if monitoring_info:
								for node in monitoring_info.nodelist:
									if node.name == clues_node_name:
										enabled = node.enabled
								if enabled:
									self.recover(clues_node_name)
								else:
									_LOGGER.debug("Node %s is disabled not recovering it." % clues_node_name)
							else:
								_LOGGER.debug("No monitoring info not recovering it.")
						else:
							self.recover(clues_node_name)
					elif state in [VirtualMachine.OFF, VirtualMachine.UNKNOWN]:
						# Do not terminate this VM, let's wait to lifecycle to check if it must be terminated 
						_LOGGER.warning("Node %s in VM with id %s is in state: %s" % (clues_node_name, vm_id, state))
				else:
					if state not in [VirtualMachine.STOPPED]:
						_LOGGER.warning("VM with id %s does not have dns_name specified." % vm_id)
					else:
						continue
						#_LOGGER.debug("Node %s with VM with id %s is stopped." % (clues_node_name, vm_id))

		# from the nodes that we have powered on, check which of them are still running
		for nname, node in self._mvs_seen.items():
			if (now - node.timestamp_seen) > self._IM_VIRTUAL_CLUSTER_FORGET_MISSING_VMS:
				_LOGGER.debug("vm %s is not seen for a while... let's forget it" % nname)
				del self._mvs_seen[nname]

		return self._mvs_seen
	
	def _recover_ids(self, vms):
		for vm in vms:
			self.power_off(vm)
		
	def power_on(self, nname):
		try:
			vms = self._get_vms()
			
			if nname in vms:
				_LOGGER.warning("Trying to launch an existing node %s. Ignoring it." % nname)
				return True, nname
			
			ec3_reuse_nodes = False
			if len(self._stopped_vms) > 0:
				if self._stopped_vms.get(nname):
					ec3_reuse_nodes = True
					vm = self._stopped_vms.get(nname)
					#ec3_reuse_nodes = vm.radl.systems[0].getValue('ec3_reuse_nodes', 0)
			
			server = self._get_server()
			auth_data = self._read_auth_data(self._IM_VIRTUAL_CLUSTER_AUTH_DATA_FILE)
			if ec3_reuse_nodes:
				(success, vms_id) = server.StartVM(self._get_inf_id(), vm.vm_id, auth_data)
			else:
				radl_data = self._get_radl(nname)
				if radl_data:
					_LOGGER.debug("RADL to launch/restart node %s: %s" % (nname, radl_data))
					(success, vms_id) = server.AddResource(self._get_inf_id(), radl_data, auth_data)
				else:
					_LOGGER.error("RADL to launch node %s is empty!!" % nname)
		except:
			_LOGGER.exception("Error launching/restarting node %s " % nname)
			return False, nname

		if success:
			_LOGGER.debug("Node %s successfully created/restarted" % nname)
		else:
			_LOGGER.error("ERROR launching node %s: %s" % (nname, vms_id))
		
		return success, nname

	def power_off(self, nname):
		_LOGGER.debug("Powering off/stopping %s" % nname)
		try:
			server = self._get_server()
			success = False
	
			if nname in self._mvs_seen:
				vm = self._mvs_seen[nname]
				ec3_destroy_interval = vm.radl.systems[0].getValue('ec3_destroy_interval', 0)
				ec3_destroy_safe = vm.radl.systems[0].getValue('ec3_destroy_safe', 0)
				ec3_reuse_nodes = vm.radl.systems[0].getValue('ec3_reuse_nodes', 0)
				
				poweroff = True
				if ec3_destroy_interval > 0:
					poweroff = False
					live_time = cpyutils.eventloop.now() - vm.timestamp_created
					remaining_paid_time = ec3_destroy_interval - live_time % ec3_destroy_interval
					_LOGGER.debug("Remaining_paid_time = %d for node %s" % (int(remaining_paid_time), nname))
					if remaining_paid_time < ec3_destroy_safe:
						poweroff = True
				
				if poweroff:
					auth_data = self._read_auth_data(self._IM_VIRTUAL_CLUSTER_AUTH_DATA_FILE)
					if ec3_reuse_nodes:
						(success, vm_ids) = server.StopVM(self._get_inf_id(), vm.vm_id, auth_data)
						self._stopped_vms[nname] = vm
						if not success: 
							_LOGGER.error("ERROR stopping node: %s: %s" % (nname,vm_ids))
						elif vm_ids == 0:
							_LOGGER.error("ERROR stopping node: %s. No VM has been stopped." % nname)
					else:
						(success, vm_ids) = server.RemoveResource(self._get_inf_id(), vm.vm_id, auth_data)
						if not success: 
							_LOGGER.error("ERROR deleting node: %s: %s" % (nname,vm_ids))
						elif vm_ids == 0:
							_LOGGER.error("ERROR deleting node: %s. No VM has been deleted." % nname)
				else:
					_LOGGER.debug("Not powering off/stopping node %s" % nname)
					success = False
			else:
				_LOGGER.warning("There is not any VM associated to node %s (are IM credentials compatible to the VM?)" % nname)
		except:
			_LOGGER.exception("Error powering off/stopping node %s " % nname)
			success = False
			
		return success, nname

	def lifecycle(self):
		try:
			monitoring_info = self._clues_daemon.get_monitoring_info()
			now = cpyutils.eventloop.now()
	
			vms = self._get_vms(monitoring_info)
				
			recover = []
			# To store the name of the nodes to use it in the third case
			node_names = []
	
			# Two cases: (1) a VM that is on in the monitoring info, but it is not seen in IM; and (2) a VM that is off in the monitoring info, but it is seen in IM
			for node in monitoring_info.nodelist:
				node_names.append(node.name)
				if node.enabled:
					if node.state in [Node.OFF, Node.OFF_ERR, Node.UNKNOWN]:
						if self._IM_VIRTUAL_CLUSTER_DROP_FAILING_VMS > 0:
							if node.name in vms:
								vm = vms[node.name]
								time_off = now - node.timestamp_state
								time_recovered = now - vm.timestamp_recovered
								_LOGGER.warning("node %s has a VM running but it is OFF or UNKNOWN in the monitoring system since %d seconds" % (node.name, time_off))
								if time_off > self._IM_VIRTUAL_CLUSTER_DROP_FAILING_VMS:
									if time_recovered > self._IM_VIRTUAL_CLUSTER_DROP_FAILING_VMS:
										_LOGGER.warning("Trying to recover it (state: %s)" % node.state)
										vm.recovered()
										recover.append(node.name)
									else:
										_LOGGER.debug("node %s has been recently recovered %d seconds ago. Do not recover it yet." % (node.name, time_recovered))
					else:
						if node.state in [Node.IDLE, Node.USED]:
							vm = vms[node.name]
							# user request use of golden images
							if vm.radl.systems[0].getValue("ec3_golden_images"):
								ec3_class = vm.radl.systems[0].getValue("ec3_class")
								# check if the image is in the list of saved images
								if ec3_class not in self._golden_images:
									# if not save it
									self._save_golden_image(vm)
						if node.name not in vms:
							# This may happen because it is launched by hand using other credentials than those for the user used for IM (and he cannot manage the VMS)
							_LOGGER.warning("node %s is detected by the monitoring system, but there is not any VM associated to it (are IM credentials compatible to the VM?)" % node.name)
	
			# A third case: a VM that it is seen in IM but does not correspond to any node in the monitoring info
			# This is a strange case but we assure not to have uncontrolled VMs
			for name in vms:
				vm = vms[name]
				if name not in node_names:
					_LOGGER.warning("VM with name %s is detected by the IM but it does not exist in the monitoring system... (%s) recovering it.)" % (name, node_names))
					vm.recovered()
					recover.append(name)
	
			self._recover_ids(recover)
		except:
			_LOGGER.exception("Error executing lifecycle of IM PowerManager.")

		return PowerManager.lifecycle(self)
	
	def recover(self, nname):
		success, nname = self.power_off(nname)
		if success:
			return Node.OFF
		
		return False

	def _save_golden_image(self, vm):
		success = False
		_LOGGER.debug("Saving golden image for VM id: " + vm.vm_id)
		try:
			server = self._get_server()
			auth_data = self._read_auth_data(self._IM_VIRTUAL_CLUSTER_AUTH_DATA_FILE)
			image_name = "im-%s" % str(uuid1())
			(success, new_image) = server.CreateDiskSnapshot(self._get_inf_id(), vm.vm_id, 0, image_name, True, auth_data)
			if success:
				ec3_class = vm.radl.systems[0].getValue("ec3_class")
				password = vm.radl.systems[0].getValue("disk.0.os.credentials.password")
				self._golden_images[ec3_class] = new_image, password
				# Save it to DB for persistence
				self._store_golden_image(ec3_class, new_image, password)
			else:
				_LOGGER.error("Error saving golden image: %s." % new_image)
		except:
			_LOGGER.exception("Error saving golden image.")
		return success
