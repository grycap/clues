#!/usr/bin/env python
#
# CLUES - Cluster Energy Saving System
# Copyright (C) 2018 - GRyCAP - Universitat Politecnica de Valencia
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
import cpyutils.db
class Stats(object):
	def __init__(self, slots, slots_free, memory, memory_free, state, timestamp):
		self._slots = float(slots)
		self._slots_free = 0
		self._slots_used = 0
		self.slots_free = float(slots_free)
		self._memory = float(memory)
		self._memory_free = 0
		self._memory_used = 0
		self.memory_free = float(memory_free)
		self.state = state
		self.t = timestamp
	@property
	def slots_free(self):
		return self._slots_free
	@slots_free.setter
	def slots_free(self, x):
		self._slots_free = x
		self._slots_used = self._slots - self._slots_free
		return self._slots_free
	@property
	def slots_used(self):
		return self._slots_used
	@property
	def slots(self):
		return self._slots
	@property
	def memory_free(self):
		return self._memory_free
	@memory_free.setter
	def memory_free(self, x):
		self._memory_free = x
		self._memory_used = self._memory - self._memory_free
		return self._memory_free
	@property
	def memory_used(self):
		return self._memory_used
	@property
	def memory(self):
		return self._memory
	def toJSONobj(self):
		return {
				"slots": self._slots,
				"slots_used": self._slots_used,
				"memory": self._memory,
				"memory_used": self._memory_used,
				"state": self.state,
				"t": self.t
			}
	def clone(self, t = None):
		if t is None:
			return Stats(self._slots, self._slots_free, self._memory, self._memory_free, self.state, self.t)
		else:
			return Stats(self._slots, self._slots_free, self._memory, self._memory_free, self.state, t)

def get_requests_data(connection_string, FROM, TO):
	db = cpyutils.db.DB.create_from_string(connection_string)

	# We'll get the max and min timestamp, and so check whether we have access to the db or not
	max_timestamp = 0
	min_timestamp = 0
	result, row_count, rows = db.sql_query("select max(timestamp_created),min(timestamp_created) from requests")

	if result:
		max_timestamp = rows[0][0]
		min_timestamp = rows[0][1]
		if max_timestamp is None: max_timestamp = 0
		if min_timestamp is None: min_timestamp = 0
	else:
		raise Exception("failed to read from the database")

	# Now correct the values of TO and FROM
	if TO == 0:
		TO = max_timestamp
	elif TO < 0:
		TO = max_timestamp + TO

	if FROM < 0:
		FROM = TO + FROM
	if FROM < 0:
		FROM = 0
	if FROM < min_timestamp:
		FROM = min_timestamp

	# Just in case that there are no requests
	if FROM is None:
		FROM = 0
	if TO is None:
		TO = 0

	# Finally get the data from the database
	result, row_count, rows = db.sql_query("select * from requests where timestamp_created >= %d and timestamp_created <= %d order by timestamp_created" % (FROM, TO) )

	requests = []
	if result:
		# Read the data from the database and create the data structure (Stats)
		for (reqid, timestamp_created, timestamp_state, state, slots, memory, expressions, taskcount, maxtaskspernode, jobid, nodes, x) in rows:
			requests.append(\
			{
				"id": reqid,\
				"t_created": timestamp_created,\
				"state": state,\
				"t_state": timestamp_state,\
				"slots": slots,\
				"memory": memory,\
				"requirements": expressions,\
				"taskcount": taskcount,\
				"maxtaskspernode": maxtaskspernode,\
				"jobid": jobid,\
				"nodes": nodes\
			}\
			)
		return requests, min_timestamp, max_timestamp
	else:
		return None, None, None

def get_reports_data(connection_string, FROM, TO):
	db = cpyutils.db.DB.create_from_string(connection_string)

	# We'll get the max and min timestamp, and so check whether we have access to the db or not
	max_timestamp = 0
	result, row_count, rows = db.sql_query("select max(timestamp),min(timestamp) from host_monitoring")

	if result:
		max_timestamp = rows[0][0]
		min_timestamp = rows[0][1]
	else:
		raise Exception("failed to read from the database")

	# Now correct the values of TO and FROM
	if TO == 0:
		TO = max_timestamp
	elif TO < 0:
		TO = max_timestamp + TO

	if FROM < 0:
		FROM = TO + FROM
	if FROM < 0:
		FROM = 0

	# Just in case that there is no monitoring information
	if FROM is None:
		FROM = 0
	if TO is None:
		TO = 0

	# Finally get the data from the database
	result, row_count, rows = db.sql_query("select * from host_monitoring where timestamp_state >= %d and timestamp_state <= %d order by timestamp_state" % (FROM, TO) )
	timeline={}
	hostnames = []

	if result:
		# Read the data from the database and create the data structure (Stats)
		for (name, timestamp_state, slots_count, slots_free, memory_total, memory_free, state, timestamp, x) in rows:
			timestamp_state = int(timestamp)
			s = Stats(slots_count, slots_free, memory_total, memory_free, state, timestamp)
			if timestamp_state not in list(timeline.keys()):
				timeline[timestamp_state] = {}
			if name not in list(timeline[timestamp_state].keys()):
				timeline[timestamp_state][name] = s
			if name not in hostnames:
				hostnames.append(name)

		# Get the timestamp sorted
		timesteps = list(timeline.keys())
		timesteps.sort()

		# Now we are filling the data blanks for each host (to get data for every host at each timestamp)
		fill_the_blanks = True
		if fill_the_blanks:
			current_values = {}
			for nname in hostnames:
				current_values[nname] = Stats(0, 0, 0, 0, 2, 0)

			for t in timesteps:
				for nname in hostnames:
					if nname not in list(timeline[t].keys()):
						timeline[t][nname] = current_values[nname].clone(t)
					current_values[nname] = timeline[t][nname]

		# Now we are re-organizing the data, indexing by host
		hostdata = {}
		for hostname in hostnames:
			hostdata[hostname] = []

		for t in timesteps:
			for hostname in hostnames:
				if hostname in timeline[t]:
					t_s = "%d" % t
					hostdata[hostname].append(timeline[t][hostname].toJSONobj())

		return hostdata, min_timestamp, max_timestamp
	else:
		return None, None, None