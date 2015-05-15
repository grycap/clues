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
import cpyutils.config
import cpyutils.eventloop
import logging
from node import Node
import request
import schedulers
from schedulers import CLUES_Scheduler

_LOGGER = logging.getLogger("[SCHED]")

class CLUES_Scheduler_Recover_OFF_ERR(CLUES_Scheduler):
    def schedule(self, requests_queue, monitoring_info, candidates_on, candidates_off):
        now = cpyutils.eventloop.now()
        if now - self._timestamp_run < self.EXTRA_NODES_PERIOD:
            # We are not running now
            return True

        return True
    def __init__(self):
        #
        # Falta incorporar el metodo RECOVER para la plataforma
        #
        CLUES_Scheduler.__init__(self, "Tries to recover those nodes that are in _ERR status by powering them on or off")
        cpyutils.config.read_config("scheduler_extra_resources",
            {
                "RECOVER_NODES_PERIOD": 300
            },
            self)
        self._timestamp_run = cpyutils.eventloop.now()
    
class CLUES_Scheduler_PowOn_Free(CLUES_Scheduler):
    def __init__(self):
        CLUES_Scheduler.__init__(self, "Power On extra nodes to maintain a set of slots or nodes free")
        cpyutils.config.read_config("scheduler_extra_resources",
            {
                "EXTRA_SLOTS_FREE": 0,
                "EXTRA_NODES_FREE": 0,
                "EXTRA_NODES_PERIOD": 30
            },
            self)
        self._timestamp_run = cpyutils.eventloop.now()

    def schedule(self, requests_queue, monitoring_info, candidates_on, candidates_off):

        now = cpyutils.eventloop.now()
        if now - self._timestamp_run < self.EXTRA_NODES_PERIOD:
            # We are not running now
            return True

        # _LOGGER.debug("running the scheduler %s" % self.name)
        self._timestamp_run = now
        nodelist = monitoring_info.nodelist
        nodecount_poweron = len(candidates_on)
        nodecount_poweroff = len(candidates_off)

        # WARNING: this algorithm may be improved for better performance, but in this way it is easier to understand

        slots_free = 0
        nodes_free = 0
        slots_powon = 0
        nodes_powon = 0
        slots_powoff = 0
        nodes_powoff= 0 
        # Count the total free slots of the platform
        
        nodes_that_can_be_poweron_on = []
        nodes_that_can_be_poweron_off = []
        
        for node in nodelist:
            if node.state in [ Node.IDLE, Node.USED, Node.ON_ERR ]:
                # In these states, the free slots are usable
                node_slots_free = max(0, node.slots_free_original)                  # When the resources are negative they are commited to be understood as unknown
                    
                if node.name in candidates_off:
                    nodes_that_can_be_poweron_on.append((node_slots_free, node.name))
                else:
                    slots_free += node_slots_free
                    if node.state == Node.IDLE:
                        nodes_free += 1
                    
            elif node.state in [ Node.POW_ON ]:
                # In this state, the node will be usable
                node_slots_free = max(0, node.slots_count)                      # When the resources are negative they are commited to be understood as unknown
                slots_free += node_slots_free
                nodes_free += 1
            
            elif node.state in [ Node.OFF ]:
                node_slots_free = max(0, node.slots_count)                      # When the resources are negative they are commited to be understood as unknown
                nodes_that_can_be_poweron_off.append((node_slots_free, node.name))
            elif node.state in [ Node.OFF_ERR ]:

                # TODO: check if the cooldown of nodes is fulfilled
                if node.power_on_operation_failed < schedulers.config_scheduling.RETRIES_POWER_ON:
                    node_slots_free = max(0, node.slots_count)                      # When the resources are negative they are commited to be understood as unknown
                    nodes_that_can_be_poweron_off.append((node_slots_free, node.name))

        slots_to_power_on = 0
        nodes_to_power_on = 0
        if slots_free < self.EXTRA_SLOTS_FREE:
            slots_to_power_on = self.EXTRA_SLOTS_FREE - slots_free

        if nodes_free < self.EXTRA_NODES_FREE:
            nodes_to_power_on = self.EXTRA_NODES_FREE - nodes_free

        nodes_that_can_be_poweron_off.sort(key=lambda tup:tup[1])
        nodes_that_can_be_poweron_on.sort(key=lambda tup:tup[1])
        nodes_that_can_be_poweron = nodes_that_can_be_poweron_on + nodes_that_can_be_poweron_off
        
        local_poweron = []
        while ((len(nodes_that_can_be_poweron) > 0) and ((slots_to_power_on > 0) or (nodes_to_power_on >0))):
            (slots_count, nname) = nodes_that_can_be_poweron.pop(0)
            node = nodelist.get_node(nname)
            
            slots_to_power_on -= slots_count
            if node.state in [ Node.IDLE, Node.POW_ON, Node.OFF, Node.OFF_ERR ]:
                nodes_to_power_on -= 1
                
            local_poweron.append(nname)

        # _LOGGER.debug("would poweron %s; poweroff: %s" % (str(local_poweron), str(candidates_off)))

        if len(local_poweron) > 0:
            log = False
            for node in local_poweron:
                if node in candidates_off:
                    candidates_off.remove(node)
                else:
                    if node not in candidates_on:
                        candidates_on[node] = []
            if log:
                _LOGGER.debug("will power on %s; still need %d slots and %d nodes" % (str(local_poweron), extra_slotcount, extra_nodecount))
        else:
            if (slots_to_power_on > 0) or (nodes_to_power_on > 0):
                _LOGGER.debug("cannot power on any node but still need %d slots and %d nodes" % (slots_to_power_on, nodes_to_power_on))
            
        # _LOGGER.debug("will power on %s" % local_poweron)
        return True