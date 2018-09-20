import sys
import cpyutils.db
from pprint import pprint

class Stats(object):
	def __init__(self, slots, slots_free, memory, memory_free, state):
		self._slots = float(slots)
		self._slots_free = 0
		self._slots_used = 0
		self.slots_free = float(slots_free)
		self._memory = float(memory)
		self._memory_free = 0
		self._memory_used = 0
		self.memory_free = float(memory_free)
		self.state = state
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

	def clone(self):
		return Stats(self._slots, self._slots_free, self._memory, self._memory_free, self.state)
	def __str__(self):
		return "%.2f/%.2f, %.2f/%.2f, %d" % (self._slots_used, self._slots, self._memory_used, self._memory, self.state)
	def __repr__(self):
		return "%.2f/%.2f, %.2f/%.2f, %d" % (self._slots_used, self._slots, self._memory_used, self._memory, self.state)

connection_string = "sqlite:///home/calfonso/Programacion/git/clues/var/lib/clues2/clues.db"
db = cpyutils.db.DB.create_from_string(connection_string)

result, row_count, rows = db.sql_query("select * from host_monitoring")
timeline={}
nodenames = []

if result:
	pprint(rows)
	for (name, timestamp_state, slots_count, slots_free, memory_total, memory_free, state, timestamp, x) in rows:
		timestamp_state = int(timestamp_state)
		s = Stats(slots_count, slots_free, memory_total, memory_free, state)
		if timestamp_state not in timeline.keys():
			timeline[timestamp_state] = {}
		if name not in timeline[timestamp_state].keys():
			timeline[timestamp_state][name] = s
		if name not in nodenames:
			nodenames.append(name)

	t = timeline.keys()
	t.sort()
	fill_the_blanks = True
	if fill_the_blanks:
		current_values = {}
		for nname in nodenames:
			current_values[nname] = Stats(0, 0, 0, 0, 2)

		for k in t:
			for nname in nodenames:
				if nname not in timeline[k].keys():
					timeline[k][nname] = current_values[nname].clone()
				current_values[nname] = timeline[k][nname]

	first = True
	output = ""
	for k in t:
		for nname in nodenames:
			if nname in timeline[k]:
				nodestats = timeline[k][nname]
				if first:
					first = False
				else:
					output = output + ", "
				output = output + "{ \"timestamp\": %ld, \"nodename\": \"%s\", \"slots\": %.2f, \"slots_used\": %.2f, \"memory\": %.2f, \"memory_used\": %.2f, \"state\" :%d}\n" % (k, nname, nodestats.slots, nodestats.slots_used, nodestats.memory, nodestats.memory_used, nodestats.state)
	print "[ %s ]" % output
