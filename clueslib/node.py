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
from configlib import _CONFIGURATION_MONITORING
import logging
import time
import threading
from request import Resources
from cpyutils.xmlobject import XMLObject
import cpyutils.evaluate
import sys
import helpers
import cpyutils.eventloop
import collections
import hooks

import cpyutils.log
_LOGGER = cpyutils.log.Log("NODE")


try:
    _annie
except:
    # We use a unique analyzer because it is a costly object (constructing a lexer and a yacc is very expensive in CPU time),
    # but we are able to include new variables easily, to evaluate the expressions
    _annie = cpyutils.evaluate.Analyzer(autodefinevars=False)

class NodeInfo():
    ERROR=-2
    UNKNOWN=-1
    IDLE=0
    USED=1
    OFF=2
    
    state2str = { UNKNOWN:'unk', IDLE:'idle', USED:'used', OFF:'off' }

    def copy(self):
        # Creates a copy of this object
        new = NodeInfo(self.name, self.slots_count, self.slots_free, self.memory_total, self.memory_free, self.keywords)
        new.state = self.state
        return new

    def get_nodeinfo(self):
        return self.copy()
    
    def __init__(self, name, slots_count, slots_free, memory_total, memory_free, keywords = {}):
        self.name = name
        self.slots_count = slots_count
        self.slots_free = slots_free
        self.slots_free_original = slots_free
        self.memory_total = memory_total
        self.memory_free = memory_free
        self.memory_free_original = memory_free
        self.state = NodeInfo.IDLE
        self.keywords = keywords.copy()

    def __str__(self):
        retval = "[NODE \"%s\"] state: %s, %d/%d (free slots), %d/%d (mem)" % (self.name, self.state2str[self.state], self.slots_free, self.slots_count, self.memory_free, self.memory_total)
        return retval

class Node(NodeInfo, helpers.SerializableXML):
    _ID = 0
    
    POW_ON=3
    POW_OFF=4
    ON_ERR=5        # The node was tried to power off, but it is still reported to be powered on after a period of time
    OFF_ERR=6       # The node was tried to power on, but it is still reported to be powered off after a period of time

    state2str = { NodeInfo.UNKNOWN:'unk', NodeInfo.IDLE:'idle', NodeInfo.USED:'used', NodeInfo.OFF:'off', POW_ON:'powon', POW_OFF:'powoff', ON_ERR:'on (err)', OFF_ERR:'off (err)' }

    @staticmethod
    def create_from_nodeinfo(ni):
        # Enables somehow upgrade the NodeInfo to a Node object
        new = Node(ni.name, ni.slots_count, ni.slots_free, ni.memory_total, ni.memory_free, ni.keywords)
        new.state = ni.state
        return new
    
    @staticmethod
    def _get_id():
        Node._ID = Node._ID + 1
        return Node._ID
    
    def __str__(self):
        retval = "[NODE \"%s\"] state: %s (since %.0f), %d/%d (slots) - ID: %.3d @%.0f" % (self.name, self.state2str[self.state], self.timestamp_state, self.slots_free, self.slots_count, self.id, self.timestamp_info)
        return retval

    def __init__(self, name, slots_count, slots_free, memory_total, memory_free, keywords = {}):
        NodeInfo.__init__(self, name, slots_count, slots_free, memory_total, memory_free, keywords)
        self.id = Node._get_id()
        self.timestamp_created = cpyutils.eventloop.now()
        self.timestamp_state = self.timestamp_created
        self.timestamp_info = self.timestamp_created
        self.timestamp_poweredon = self.timestamp_created
        self.timestamp_poweredoff = self.timestamp_created 
        self.power_on_operation_failed = 0                     
        self.power_off_operation_failed = 0                     
        self.enabled = True

        # NEW: try to implement a mechanism to avoid glitches in ON->OFF or OFF->ON
        self._store_prev()

    def _store_prev(self):
        # PS Issue
        self.prev_state = self.state
        self.prev_timestamp_state = self.timestamp_state
        self.prev_timestamp_poweredon = self.timestamp_poweredon
        self.prev_timestamp_poweredoff = self.timestamp_poweredoff
        self.prev_power_on_operation_failed = self.power_on_operation_failed
        self.prev_power_off_operation_failed = self.power_off_operation_failed

    def _restore_prev_if_needed(self, state, now):
        # PS Issue:
        # Tries to address the following scenario: the monitoring system is restarted and the nodes change to "off" or to "not monitorized" (usually equivalent to off),
        #   and then the system monitorizes the nodes again and the nodes are in the same state (in fact, they never got off) so the values are restored.
        #   It is implemented in a general way, but it is actually limited to the case of the glitch to "OFF".
        if (_CONFIGURATION_MONITORING.TIME_OFF_GLITCH_DETECTION > 0) and (self.state == Node.OFF) and (state == self.prev_state) and (now - self.prev_timestamp_state <= _CONFIGURATION_MONITORING.TIME_OFF_GLITCH_DETECTION):
            _LOGGER.info("restoring node %s to the previous state" % (self.name))
            self.state = self.prev_state
            self.timestamp_state = self.prev_timestamp_state
            self.timestamp_poweredon = self.prev_timestamp_poweredon
            self.timestamp_poweredoff = self.prev_timestamp_poweredoff
            self.power_on_operation_failed = self.prev_power_on_operation_failed
            self.power_off_operation_failed = self.prev_power_off_operation_failed
            return True
        return False

    def mark_poweredon(self):
        _LOGGER.debug("node %s has been powered on" % self.name)
        self.power_on_operation_failed = 0
        self.timestamp_poweredon = cpyutils.eventloop.now()

    def mark_poweredoff(self):
        _LOGGER.debug("node %s has been powered off" % self.name)
        self.power_off_operation_failed = 0
        self.timestamp_poweredoff = cpyutils.eventloop.now()

    def meets_resources(self, resources):
        if (self.slots_count < 0) and (_CONFIGURATION_MONITORING.NEGATIVE_RESOURCES_MEANS_INFINITE):
            pass
        else:
            if (resources.slots is not None) and (resources.slots > self.slots_free): return False
        if (self.memory_total < 0) and (_CONFIGURATION_MONITORING.NEGATIVE_RESOURCES_MEANS_INFINITE):
            pass
        else:
            if (resources.memory is not None) and (resources.memory > self.memory_free): return False

        if len(resources.requests) > 0:
            _annie.add_vars(self.keywords, True)

            # _LOGGER.debug("%s" % self.keywords)
            # _LOGGER.debug("evaluating resource: %s" % resources.requests)
            for expr in resources.requests:
                result = False
                try:
                    expr = expr.strip()
                    if expr == "":
                        result = True
                    else:
                        res_check = _annie.check(expr)
                        if res_check.type == cpyutils.evaluate.TypedClass.BOOLEAN:
                            result = res_check.get()
                except:
                    result = False
                    
                if not result:
                    return False
        return True

    def get_nodeinfo(self):
        # Gets the nodeinfo structure that will represent this object (it is more simple)
        new = NodeInfo(self.name, self.slots_count, self.slots_free, self.memory_total, self.memory_free, self.keywords)
        new.state = self.state
        return new

    def copy(self):
        # Creates a copy of this object
        new = Node(self.name, self.slots_count, self.slots_free, self.memory_total, self.memory_free, self.keywords)
        new.id = self.id
        new.state = self.state
        new.timestamp_info = self.timestamp_info
        new.timestamp_state = self.timestamp_state
        new.timestamp_created = self.timestamp_created
        new.timestamp_poweredoff = self.timestamp_poweredoff
        new.timestamp_poweredon = self.timestamp_poweredon
        new.power_on_operation_failed = self.power_on_operation_failed
        new.power_off_operation_failed = self.power_off_operation_failed
        new.enabled = self.enabled
        
        # PS Issue
        new.prev_state = self.prev_state
        new.prev_timestamp_state = self.prev_timestamp_state
        new.prev_timestamp_poweredon = self.prev_timestamp_poweredon
        new.prev_timestamp_poweredoff = self.prev_timestamp_poweredoff
        new.prev_power_on_operation_failed = self.prev_power_on_operation_failed
        new.prev_power_off_operation_failed = self.prev_power_off_operation_failed
        return new

    def update_info(self, other):
        # Updates the information of the node according to a new observation. It updates the timestamps in case that the information varies
        info_updated = False
        state_changed = False
        if self.slots_count != other.slots_count:
            info_updated = True
        if self.slots_free != other.slots_free:
            info_updated = True
        if self.slots_free_original != other.slots_free_original:
            info_updated = True
        if self.memory_free != other.memory_free:
            info_updated = True
        if self.memory_free_original != other.memory_free_original:
            info_updated = True
        if self.memory_total != other.memory_total:
            info_updated = True

        kw_updated = False
        for kw in self.keywords:
            if kw not in other.keywords or self.keywords[kw] != other.keywords[kw]:
                kw_updated = True
        for kw in other.keywords:
            if kw not in self.keywords:
                kw_updated = True

        current_time = cpyutils.eventloop.now()
        if self.set_state(other.state):
            state_changed = True
            info_updated = True
            current_time = self.timestamp_state

        self.slots_count = other.slots_count
        self.slots_free = other.slots_free
        self.slots_free_original = other.slots_free_original
        self.memory_total = other.memory_total
        self.memory_free = other.memory_free
        self.memory_free_original = other.memory_free_original

        if kw_updated:
            info_updated = True
            self.keywords = other.keywords.copy()
        
        if info_updated:
            self.timestamp_info = current_time
            
        return info_updated, state_changed
    
    def set_state(self, state, force = False):
        # Updates the state of the node and updates the timestamp, in case that the state varies
        changed = False

        if state != self.state:
            # Let's check the possible states
            now = cpyutils.eventloop.now()
            if self._restore_prev_if_needed(state, now):
                return True
            
            unexpected = False
            accept_change = True
            if self.state == Node.IDLE:
                if state in [ Node.POW_ON, Node.OFF_ERR, Node.OFF, Node.ON_ERR ]:
                    unexpected = True
                if state in [ Node.USED, Node.POW_OFF ]:
                    pass
            if self.state == Node.USED:
                if state in [ Node.POW_ON, Node.OFF, Node.OFF_ERR, Node.ON_ERR ]:
                    unexpected = True
                if state in [ Node.IDLE, Node.POW_OFF ]:
                    pass
            if self.state == Node.OFF:
                if state in [ Node.IDLE, Node.USED, Node.POW_OFF, Node.OFF_ERR, Node.ON_ERR ]:
                    unexpected = True
                if state in [ Node.POW_ON ]:
                    pass
            if self.state == Node.ON_ERR:
                if state in [ Node.POW_ON, Node.OFF, Node.OFF_ERR, Node.ON_ERR ]:
                    unexpected = True
                if state in [ Node.IDLE, Node.USED ]:
                    accept_change = False
                if state in [ Node.POW_OFF ]:
                    pass
            if self.state == Node.OFF_ERR:
                if state in [ Node.IDLE, Node.USED, Node.POW_OFF, Node.ON_ERR ]:
                    unexpected = True
                if state in [ Node.OFF ]:
                    accept_change = False
                if state in [ Node.POW_ON ]:
                    pass
            if self.state == Node.POW_ON:
                # TODO: ver si hacemos que el tiempo de espera a que se encienda sea adaptativo
                if state in [ Node.POW_OFF, Node.OFF, Node.ON_ERR ]:
                    unexpected = True
                if state in [ Node.OFF ]:
                    # We cannot accept the state immediately
                    #now = cpyutils.eventloop.now()
                    if (now - self.timestamp_state) < _CONFIGURATION_MONITORING.MAX_WAIT_POWERON:
                        accept_change = False
                    else:
                        _LOGGER.warning("node was tried to be powered on, but it is still off")
                        state = Node.OFF_ERR
                if state in [ Node.IDLE, Node.USED ]:
                    #now = cpyutils.eventloop.now()
                    if (now - self.timestamp_state) < _CONFIGURATION_MONITORING.DELAY_POWON:
                        accept_change = False
                    else:
                        _LOGGER.debug("node is detected to be ON but we are waiting to confirm the state")
                if state in [ Node.OFF_ERR ]:
                    pass
            if self.state == Node.POW_OFF:
                # TODO: ver si hacemos que el tiempo de espera a que se encienda sea adaptativo
                if state in [ Node.POW_ON, Node.OFF_ERR ]:
                    unexpected = True
                if state in [ Node.IDLE, Node.USED ]:
                    # We cannot accept the state immediately
                    #now = cpyutils.eventloop.now()
                    if (now - self.timestamp_state) < _CONFIGURATION_MONITORING.MAX_WAIT_POWEROFF:
                        accept_change = False
                    else:
                        _LOGGER.warning("node was tried to be powered off, but it is still on")
                        state = Node.ON_ERR
                if state in [ Node.OFF ]:
                    #now = cpyutils.eventloop.now()
                    if (now - self.timestamp_state) < _CONFIGURATION_MONITORING.DELAY_POWOFF:
                        accept_change = False
                    else:
                        _LOGGER.debug("node is detected to be OFF but we are waiting to confirm the state")
                if state in [ Node.ON_ERR ]:
                    pass

            if accept_change or force:
                if unexpected:
                    _LOGGER.warning("node %s was unexpectedly changed from state %s to state %s" % (self.name, self.state2str[self.state], self.state2str[state]))
                
                changed = True
                
                if (state in [ Node.IDLE, Node.USED ]):
                    if (self.state not in [ Node.IDLE, Node.USED ]):
                        self.mark_poweredon()
                        if unexpected:
                            hooks.HOOKS.unexpected_poweron(self.name)
                        hooks.HOOKS.poweredon(self.name)
                    else:
                        if state == Node.IDLE:
                            hooks.HOOKS.idle(self.name)
                        else:
                            hooks.HOOKS.used(self.name)

                if (state in [ Node.OFF ]) and (self.state not in [ Node.OFF ]):
                    self.mark_poweredoff()
                    if unexpected:
                        hooks.HOOKS.unexpected_poweroff(self.name)
                    hooks.HOOKS.poweredoff(self.name)

                if state == Node.OFF_ERR:
                    self.power_on_operation_failed += 1
                    hooks.HOOKS.offerr(self.name, self.power_on_operation_failed)
                    _LOGGER.debug("failed to power on node %s (%d fails)" % (self.name, self.power_on_operation_failed))
                if state == Node.ON_ERR:
                    self.power_off_operation_failed += 1
                    hooks.HOOKS.onerr(self.name, self.power_off_operation_failed)
                    _LOGGER.debug("failed to power off node %s (%d fails)" % (self.name, self.power_off_operation_failed))

                if state == Node.UNKNOWN:
                    hooks.HOOKS.unknown(self.name)
                    _LOGGER.debug("state of node %s changed to unknown" % (self.name))

                self._store_prev()                    
                self.state = state
                self.timestamp_state = cpyutils.eventloop.now()
                #if self.state == Node.IDLE:
                #    _LOGGER.debug("node %s is set to idle @%.0f" % (self.name, self.timestamp_state))
            else:
                pass
        return changed

    def enable(self):
        # Sets the node to enabled
        self.enabled = True
        
    def disable(self):
        # Sets the node to disabled
        self.enabled = False
    
    def allocate(self, resources):
        # Allocates the resources of the node for one job
        self.slots_free -= resources.slots
        self.memory_free -= resources.memory
        return True
    
    def deallocate(self, resources):
        # Deallocates the resources of the nodes used by one job
        self.slots_free += resources.slots
        self.memory_free += resources.memory
        return True
    
class NodeList():
    # This class will allow to filter node lists
    def __init__(self, nodelist):
        self._nodeinfo = collections.OrderedDict()
        self._filtered_nodeinfo = None
        self._current_node = -1
        self._nodenames = []

        if nodelist is not None:
            for n_id, node in nodelist.items():
                self._nodeinfo[n_id] = node.copy()

    # These next functions are used to create an iterator to be used in a "for" construction, for example
    def begin_iterating(self):
        self._current_node = -1
        self._nodenames = self._nodeinfo.keys()

    def next(self):
        if self._current_node >= len(self._nodenames)-1: raise StopIteration
        self._current_node += 1
        node = self._nodeinfo[self._nodenames[self._current_node]]
        return node

    def __iter__(self):
        self.begin_iterating()
        return self
    
    def __next__(self):
        return self.next()

    # Duplicates the list
    def duplicate(self):
        return NodeList(self._nodeinfo)

    # Gets the node object from its name
    def get_node(self, n_id):
        if n_id not in self._nodeinfo:
            return None
        return self._nodeinfo[n_id]

    # Gets the inner dictionary contruction
    def get_list(self):
        return self._nodeinfo
    
    # Returns the number of nodes
    def count(self):
        return len(self._nodeinfo)
    
    # Returns the string representation of the objects contained in lines
    def __str__(self):
        retval = ""
        for n_id, node in self._nodeinfo.items():
            retval = "%s%s\n" % (retval, str(node))
        return retval
    
    # The next functions are provided to make it easier to filter the list of nodes
    def FILTER_count(self):
        if self._filtered_nodeinfo is None:
            self.FILTER_reset()
            
        return len(self._filtered_nodeinfo)

    def FILTER_reset(self):
        self._filtered_nodeinfo = collections.OrderedDict()
        for n_id, node in self._nodeinfo.items():
            self._filtered_nodeinfo[n_id] = node
        
    def FILTER_basic(self, resources = None, enabled = None, states = None):
        filtered_nodeinfo = self._filtered_nodeinfo
        if filtered_nodeinfo is None:
            filtered_nodeinfo = self._nodeinfo
            
        self._filtered_nodeinfo = collections.OrderedDict()
        for n_id, node in filtered_nodeinfo.items():
            # Let's perform the filtering using first the criteria that cost less
            if (enabled is not None) and (node.enabled != enabled): continue
            if (states is not None) and (node.state not in states): continue
            if (resources is not None) and (not node.meets_resources(resources)): continue
            self._filtered_nodeinfo[n_id] = node
            
        return self._filtered_nodeinfo
            
    def get_nodelist_filtered(self):
        if self._filtered_nodeinfo is None:
            self.FILTER_reset()
        return self._filtered_nodeinfo
