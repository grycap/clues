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

import logging
import clueslib.helpers
import cpyutils.runcommand
from cpyutils.evaluate import TypedClass, TypedList
from clueslib.node import NodeInfo
from xml.dom.minidom import parseString
import os
import collections

# TODO: check if information about nodes is properly set (check properties, queues and so on)
import cpyutils.log
_LOGGER = cpyutils.log.Log("PLUGIN-SGE")

class lrms(clueslib.platform.LRMS):
	def __init__(self, SGE_QHOST_COMMAND = None, SGE_QCONF_COMMAND = None, SGE_QSTAT_COMMAND = None, SGE_ROOT = None, SGE_DEFAULT_QUEUE = None):
		#
		# NOTE: This fragment provides the support for global config files. It is a bit awful.
		#	   I do not like it because it is like having global vars. But it is managed in
		#	   this way for the sake of using configuration files
		#
		import cpyutils.config
		config_sge = cpyutils.config.Configuration(
			"SGE",
			{
				"SGE_QHOST_COMMAND": "/usr/bin/qhost",
				"SGE_QCONF_COMMAND": "/usr/bin/qconf",
				"SGE_QSTAT_COMMAND": "/usr/bin/qstat",
				"SGE_ROOT": "/var/lib/gridengine",
				"SGE_DEFAULT_QUEUE": "all.q",
				"SGE_TIMEOUT_COMMANDS": 10
			}
		)
		
		self._qhost = clueslib.helpers.val_default(SGE_QHOST_COMMAND, config_sge.SGE_QHOST_COMMAND)
		self._qconf = clueslib.helpers.val_default(SGE_QCONF_COMMAND, config_sge.SGE_QCONF_COMMAND)
		self._qstat = clueslib.helpers.val_default(SGE_QSTAT_COMMAND, config_sge.SGE_QSTAT_COMMAND)
		self._sge_root = clueslib.helpers.val_default(SGE_ROOT, config_sge.SGE_ROOT)
		self._default_queue = clueslib.helpers.val_default(SGE_DEFAULT_QUEUE, config_sge.SGE_DEFAULT_QUEUE)
		self._timeout_commands = config_sge.SGE_TIMEOUT_COMMANDS
		# Cache to store the allocation rule values for the SGE PEs to reduce the number of commands to run
		self._allocation_rule_cache = {}
		clueslib.platform.LRMS.__init__(self, "SGE")
	
	def _parse_qhost_xml(self):
		# Set the SGE_ROOT environment variable
		sge_root_var = os.getenv("SGE_ROOT")
		if sge_root_var is None:
			os.environ['SGE_ROOT'] = self._sge_root
	
		command = [ self._qhost, '-xml', '-q' ]
		success, out_command = cpyutils.runcommand.runcommand(command, False, timeout = self._timeout_commands)
		if not success:
			_LOGGER.error("could not get information about the hosts: %s" % out_command)
			return None
	
		dom = parseString(out_command)
		return dom
	
	def _parse_qstat_xml(self):
		# Set the SGE_ROOT environment variable
		sge_root_var = os.getenv("SGE_ROOT")
		if sge_root_var is None:
			os.environ['SGE_ROOT'] = self._sge_root
	
		command = [ self._qstat, '-r', '-xml', '-u', '*' ]
		success, out_command = cpyutils.runcommand.runcommand(command, False, timeout = self._timeout_commands)
		if not success:
			_LOGGER.error("could not get information about the jobs: %s" % out_command)
			return None
	
		dom = parseString(out_command)
		return dom
	
	def _get_hostgroups(self):
		# Set the SGE_ROOT environment variable
		sge_root_var = os.getenv("SGE_ROOT")
		if sge_root_var is None:
			os.environ['SGE_ROOT'] = self._sge_root
	
		res = {}
		command = [ self._qconf, '-shgrpl']
		success, out_command = cpyutils.runcommand.runcommand(command, False, timeout = self._timeout_commands)
		if not success:
			_LOGGER.error("could not get information about the hosts: %s" % out_command)
		else:
			hostgroups = out_command.split('\n')
			for hg in hostgroups:
				hg = hg.strip()
				if len(hg) > 0:
					command = [ self._qconf, '-shgrp', hg]
					success, out_command = cpyutils.runcommand.runcommand(command, False, timeout = self._timeout_commands)
					if success:
						for line in out_command.split('\n'):
							if line.startswith("group_name"):
								group_name = line[11:].strip()
							if line.startswith("hostlist"):
								hostlist = line[9:].split(" ")
						res[group_name] = hostlist
					else:
						_LOGGER.error("could not get information about the host group %s: %s" % (hg, out_command))
	
		return res
	
	# Procesa el formato de especificacion de slots de los trabajos tipo array: n[-m[:s]]
	# para estos casos
	# n-m -> m-n numero de slots
	# n-m:s -> (m-n)/s numero de slots
	def _get_range_slots(self, t_slots):
		partes = t_slots.split("-")
		if len(partes) != 2:
			_LOGGER.error("Array Range Job slots " + str(t_slots) + " incorrectly specified. Format: n[-m[:s]]")
			return 1
	
		ini = int(partes[0]);

		if partes[1].find(":") == -1:
			fin = int(partes[1]);
			paso = 1
		else:
			subpartes = partes[1].split(":")
			if len(subpartes) != 2:
				_LOGGER.error("Array Range Job slots " + str(t_slots) + " incorrectly specified. Format: n-m:s")
				return 1
			fin = int(subpartes[0])
			paso = int(subpartes[1])
	
		if fin < ini:
			aux = fin
			fin = ini
			ini = aux
	
		return ((fin - ini) / paso) + 1
	
	# Procesa el formato de especificacion de slots de los trabajos tipo array: n[-m[:s]]
	# siendo n el minimo de slots requeridos y m el maximo.
	# n -> numero fijo
	# n-m -> m-n numero de slots
	# n-m:s -> (m-n)/s numero de slots
	# 1,2,3,4... -> Lista de ids de los array jobs
	# 1,3,6,10-12,24,33... -> Lista de ids de los array jobs, en los que pueden aparecer rangos
	def _get_t_slots(self, t_slots):
		if t_slots.find("-") == -1:
			if t_slots.find(",") == -1:
				# Caso numero fijo
				return 1
			else:
				# caso lista de ids (sin ningun rango)
				partes = t_slots.split(",")
				return len(partes)
		else:
			# caso lista de ids (con algun rango)
			partes = t_slots.split(",")
			slots = 0
			for token in partes:
				if token.find("-") == -1:
					slots += 1
				else:
					slots += self._get_range_slots(token)
			return slots
	
	def _get_pe_allocation_rule(self, pe_name):
			# Set the SGE_ROOT environment variable
			sge_root_var = os.getenv("SGE_ROOT")
			if sge_root_var is None:
				os.environ['SGE_ROOT'] = self._sge_root

			allocation_rule = None
			command = [ self._qconf, '-sp', pe_name]
			success, out_command = cpyutils.runcommand.runcommand(command, False, timeout = self._timeout_commands)
			if not success:
				_LOGGER.error("could not get information about the parallel environments: %s" % out_command)
			else:
				pe_info = out_command.split('\n')
				for pe_info_line in pe_info:
					if pe_info_line.startswith('allocation_rule'):
						allocation_rule = pe_info_line[15:].strip()
	
			return allocation_rule
	
	def _get_slots_pn_in_pe(self, params):
		pe_name = None
		pe_max = None
		for param in params:
			if param[0] == "pe_max":
				pe_max = int(param[1])
			if param[0] == "pe_name":
				pe_name = param[1]
	
		res = None
		if pe_name:
			if pe_name in self._allocation_rule_cache:
				allocation_rule = self._allocation_rule_cache[pe_name]
			else:
				allocation_rule = self._get_pe_allocation_rule(pe_name)
				self._allocation_rule_cache[pe_name] = allocation_rule
			if not allocation_rule:
				res = None
			elif allocation_rule == "$fill_up":
				res = None
			elif allocation_rule == "$round_robin":
				res = None
			elif allocation_rule == "$pe_slots":
				res = pe_max
			else:
				res = int(allocation_rule)
		return res
	
	def _get_wc_queue_list(self, wc_queue_list):
		res = []
		for queue_data in wc_queue_list.split(','):
			queue = None
			host = None
			hostgroup = None
			if queue_data.find('@@') != -1:
				parts = queue_data.split('@@')
				queue = parts[0]
				hostgroup = parts[1]
			elif queue_data.find('@') != -1:
				parts = queue_data.split('@')
				queue = parts[0]
				host = parts[1]
			else:
				queue = queue_data
			res.append((queue, host, hostgroup))
	
		return res
	
	@staticmethod
	def _format_request_multi_parameter(name, value_list):
		req_str = ""
		for value in value_list:
			if req_str:
				req_str += " || "
			else:
				req_str += "("
			req_str += "'" + value + "' in " + name
		req_str += ") "
		
		return req_str
	
	@staticmethod
	def _translate_mem_value(memval):
		memval = memval.rstrip(".").strip()
		
		multiplier = 1
		if len(memval) > 0:
			qualifier = memval[-1:]
			if qualifier == 'k':
				multiplier = 1000
			elif qualifier == 'm':
				multiplier = 1000*1000
			elif qualifier == 'g':
				multiplier = 1000*1000*1000
			elif qualifier == 'K':
				multiplier = 1024
			elif qualifier == 'M':
				multiplier = 1024*1024
			elif qualifier == 'G':
				multiplier = 1024*1024*1024
			
		if multiplier > 1:
			value_str = memval[:-1]
		else:
			value_str = memval
		
		try:
			value = int(value_str)
		except:
			try:
				value = float(value_str)
			except:
				value = -1
				
		return value * multiplier
	
	def get_nodeinfolist(self):
		hostgroups = self._get_hostgroups()
		
		dom = self._parse_qhost_xml()
		if dom is None:
			return None

		hosts = dom.getElementsByTagName("host")
	
		nodeinfolist = collections.OrderedDict()
		for h in hosts:
			hostname = h.getAttribute("name");
			keywords = {}
			node_queues = []
			# ignore the generic host "global"
			if hostname != "global":
				memory_total = 0
				memory_used = 0
				# get the host values to get the information
				powered_on = False
				hostvalues = h.getElementsByTagName("hostvalue")
				for hv in hostvalues:
					valuename = hv.getAttribute("name");
					if valuename == "load_avg":
						# If the load_avg is defined, the node is considered to be on
						# TODO: Try to improve this
						if hv.firstChild.nodeValue != "-":
							powered_on = True
					elif valuename == "mem_total":
						if hv.firstChild.nodeValue != "-":
							memory_total = self._translate_mem_value(hv.firstChild.nodeValue)
					elif valuename == "mem_used":
						if hv.firstChild.nodeValue != "-":
							memory_used = self._translate_mem_value(hv.firstChild.nodeValue)
	
				used_slots = 0
				total_slots = 0
				# Get the info about the queues
				queues = h.getElementsByTagName("queue")
				for q in queues:
					queue_name = q.getAttribute("name");
					node_queues.append(TypedClass.auto(str(queue_name)))
					# Get the queue values
					queuevalues = q.getElementsByTagName("queuevalue")
					queue_used_slots = 0
					queue_total_slots = 0
					state = None
					for qv in queuevalues:
						queuevaluename = qv.getAttribute("name");
						if queuevaluename == "slots_used":
							queue_used_slots = int(qv.firstChild.nodeValue)
						if queuevaluename == "slots":
							queue_total_slots = int(qv.firstChild.nodeValue)
						if queuevaluename == "state_string":
							if qv.firstChild != None:
								state = qv.firstChild.nodeValue
	
					# if some of the queues are in "Alarm Unknown" state the node is down
					if state != None and (state.lower().find('au') != -1):
						powered_on = False

					# This slots are disabled/suspended
					if state != None and (state.lower().find('d') != -1 or state.lower().find('s') != -1):
						_LOGGER.debug(queue_name + "@" + hostname + " is in " + state + " state. Ignoring this slots")
					else:
						used_slots += queue_used_slots
						total_slots += queue_total_slots
	
				keywords['hostname'] = TypedClass.auto(hostname)
				if len(node_queues) > 0:
					keywords['queues'] = TypedList(node_queues)
	
				node_hgs = []
				for hg, nodelist in hostgroups.iteritems():
					if hostname in nodelist:
						node_hgs.append(TypedClass.auto(str(hg)))
	
				keywords['hostgroups'] = TypedList(node_hgs)
	
				free_slots = total_slots - used_slots
	
				if powered_on:
					if free_slots > 0:
						state = NodeInfo.IDLE
					else:
						state = NodeInfo.USED
				else:
					state = NodeInfo.OFF
	
				memory_free = -1
				if memory_total != -1:
					memory_free = memory_total - memory_used
				nodeinfolist[hostname] = NodeInfo(hostname, total_slots, free_slots, memory_total, memory_free, keywords)
				nodeinfolist[hostname].state = state
	
		return nodeinfolist
	
	def get_jobinfolist(self):
		dom = self._parse_qstat_xml()
		if dom is None:
			return None
		jobinfolist = []
		jobs = dom.getElementsByTagName("job_list")
		# Empty the allocation_rule_cache in every job info call
		# TODO: Use time stamps to manage the validity of this cache
		self._allocation_rule_cache = {}
		for j in jobs:
			state = j.getAttribute("state")
			if state == 'running':
				state = clueslib.request.Request.ATTENDED
				continue
			elif state == 'pending':
				state = clueslib.request.Request.PENDING
	
			job_id = j.getElementsByTagName("JB_job_number")[0].firstChild.nodeValue
			
			slots = int(j.getElementsByTagName("slots")[0].firstChild.nodeValue)
			if len(j.getElementsByTagName("tasks")) > 0:
				tasks = j.getElementsByTagName("tasks")[0].firstChild.nodeValue
				slots = self._get_t_slots(tasks)
	
			slots_pn = None
			if j.getElementsByTagName("requested_pe"):
				pe_name = j.getElementsByTagName("requested_pe")[0].getAttribute("name")
				pe_max = j.getElementsByTagName("requested_pe")[0].firstChild.nodeValue
				slots_pn = self._get_slots_pn_in_pe([['pe_name', pe_name], ['pe_max', pe_max]])

			memory = 0
			if j.getElementsByTagName("hard_request"):
				hard_request_name = j.getElementsByTagName("hard_request")[0].getAttribute("name")
				if hard_request_name in ["h_vmem", "s_vmem"]:
					memory = self._translate_mem_value(j.getElementsByTagName("hard_request")[0].firstChild.nodeValue)
	
			hostnames = []
			hostgroups = []
			queues = []
			if len(j.getElementsByTagName("hard_req_queue")) > 0:
				reqs_str = ""
				for i, req in enumerate(j.getElementsByTagName("hard_req_queue")):
					if i > 0:
						reqs_str += ","
					reqs_str += req.firstChild.nodeValue
				queue_params = self._get_wc_queue_list(reqs_str)
				
				for (queue, host, hostgroup) in queue_params:
					if queue is not None and queue not in queues:
						queues.append(queue)
					if host is not None and host not in hostnames:
						hostnames.append(host)
					if hostgroup is not None and hostgroup not in hostgroups:
						hostgroups.append(hostgroup)
			else:
				queues = [self._default_queue]
	
			req_str = self._format_request_multi_parameter('queues', queues)
		
			if len(hostnames) > 0:
				req_str += ' && hostname in %s ' % hostnames
			if len(hostgroups) > 0:
				req_str += ' && ' + self._format_request_multi_parameter('hostgroups', hostgroups)
			if slots_pn:
				slots = int(slots / slots_pn)
			else:
				slots_pn = 1

			nodes = []
	
			resources = clueslib.request.ResourcesNeeded(slots_pn, memory, [req_str], slots)
			job = clueslib.request.JobInfo(resources, job_id, nodes)
			job.set_state(state)
			jobinfolist.append(job)
	
		return jobinfolist
	
if __name__ == '__main__':
	pass
