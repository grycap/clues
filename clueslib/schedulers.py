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
from request import Request
import math
import collections

try:
    config_scheduling
except:
    config_scheduling = cpyutils.config.Configuration("scheduling",
        {
            "PERIOD_SCHEDULE": 5,
            "MAX_BOOTING_NODES": 0,
            "SCHEDULER_CLASSES": "",
            "RETRIES_POWER_ON": 3,
            "RETRIES_POWER_OFF": 3,
            "PERIOD_RECOVERY_NODES": 30
        })

import cpyutils.log
_LOGGER = cpyutils.log.Log("SCHED")
_LOGGER.setup_log(cpyutils.log.logging.DEBUG)

def _allocate_nodes(resources, nodelist, node_ids, count):
    n_nodes = len(node_ids)
    n_allocations = min(n_nodes, count)
    allocated_nodes = []
    for i in range(0, n_allocations):
        nodelist.get_node(node_ids[i]).allocate(resources)
        allocated_nodes.append(node_ids[i])
        count -= 1
    return count, allocated_nodes


def _nodes_meet_resources(resources, _nodelist, node_ids):
    
    # First we duplicate the list to check whether we can meet the resources or not
    nodelist = _nodelist.duplicate()
    
    taskcount = resources.taskcount
    nodes_allocated = []
    
    for n_id in node_ids:
        node = nodelist.get_node(n_id)
        taskspernode = resources.maxtaskspernode
        while node.meets_resources(resources.resources) and taskcount > 0 and taskspernode > 0:
            node.allocate(resources.resources)
            nodes_allocated.append(n_id)
            taskspernode -= 1
            taskcount -= 1
    
    if taskcount == 0:
        return True, nodes_allocated
    else:
        return False, []

def _allocate_nodes0(resources, nodelist, node_ids):
    taskcount = resources.taskcount
    for n_id in node_ids:
        node = nodelist.get_node(n_id)
        node.allocate(resources.resources)
        taskcount -= 1
    return taskcount

class CLUES_Scheduler():
    def __init__(self, name = ""):
        self.name = name
        _LOGGER.info("Activated the scheduler with name: '%s'" % name)
        
    def __str__(self):
        return "CLUES Scheduler \"%s\"" % self.name

    def schedule(self, requests_queue, monitoring_info, candidates_on, candidates_off):
        return True

class CLUES_Scheduler_PowOff_IDLE(CLUES_Scheduler):
    def __init__(self):
        CLUES_Scheduler.__init__(self, "Power Off Nodes that are idle")
        cpyutils.config.read_config("scheduling",
            {
                "COOLDOWN_NODES": 0,
                "IDLE_TIME": 1800
            },
            self)

    def schedule(self, requests_queue, monitoring_info, candidates_on, candidates_off):
        now = cpyutils.eventloop.now()
        eps_time_idle = now - self.IDLE_TIME
        eps_time_cooldown = now - self.COOLDOWN_NODES

        # TODO: es posible incluir un sistema de recuperacion de los ON_ERR, para reintentarlo hasta 3 veces (por ejemplo)
        
        nodes = monitoring_info.nodelist.get_list()
        for n_id, node in nodes.items():
            if (node.enabled) and (node.timestamp_poweredon < eps_time_cooldown) and (node.state in [ Node.IDLE ]) and (node.timestamp_state < eps_time_idle) :
                # _LOGGER.debug("considering %s to power off @%.0f (powered on@%.0f, and state@%.0f)" % (n_id, now, node.timestamp_poweredon, node.timestamp_state))
                candidates_off.append(n_id)
        
        return True

class BookInfo:
    def __init__(self, r_id, node_ids):
        self.r_id = r_id
        self.node_ids = node_ids[:]
        self.timestamp = cpyutils.eventloop.now()
        
    def __str__(self):
        now = cpyutils.eventloop.now()
        return "request %s, pending from %s for %s seconds" % (self.r_id, self.node_ids, now - self.timestamp)

class BookingSystem:
    def __init__(self, COOLDOWN_SERVED_REQUESTS):
        self._bookings = {}
        # The time that a request will be booking its resources once it is served
        self._COOLDOWN_SERVED_REQUESTS = COOLDOWN_SERVED_REQUESTS
        
    def __str__(self):
        retval = ""
        for r_id, book_info in self._bookings.items():
            retval="%s%s\n" % (retval, str(book_info))
        return retval
        
    def book(self, request, nodes):
        r_id = request.id
        self._bookings[r_id] = BookInfo(r_id, nodes)

    def _purgue_ids(self, r_ids_to_cleanup):
        for r_id in r_ids_to_cleanup:
            del self._bookings[r_id]

    def _purgue_missing_requests(self, requests):
        r_ids_to_cleanup = []
        for r_id in self._bookings:
            current_request = requests.get_by_id(r_id)
            
            if current_request is None:
                # The request has been removed from the list, so we are not booking the resources anymore
                r_ids_to_cleanup.append(r_id)

        self._purgue_ids(r_ids_to_cleanup)

    def _purgue_requests_with_served_jobs(self, requests, monitoring_info):

        if monitoring_info.joblist is None:
            # Nothing to do
            return
        
        # The jobs that have been attended
        if monitoring_info.joblist is None:
            # Nothing to do
            return
        j_ids = monitoring_info.joblist.get_ids(Request.ATTENDED)
        # TODO: revisar cuando se enciende un nodo, para ver si tras entrar, aparece el j_id (ha habido un momento en que me ha parecido que no ha funcionado bien y que ha entrado una por COOLDOWN de served)

        r_ids_to_cleanup = []
        for r_id in self._bookings:
            current_request = requests.get_by_id(r_id)

            # Avoiding errors
            if current_request is None:
                continue

            if current_request.job_id is not None:
                # TODO: should check if it is running?
                if (current_request.job_id in j_ids) and (current_request.state in [Request.ATTENDED, Request.SERVED]) and (current_request.timestamp_state < monitoring_info.timestamp_joblist): # (len(current_request.job_nodes_ids) > 0):
                    _LOGGER.debug("request %s has already been attended by the LRMS (%s)" % (r_id, current_request.job_id))
                    
                    r_ids_to_cleanup.append(r_id)
                    current_request.set_state(Request.SERVED)

        self._purgue_ids(r_ids_to_cleanup)

    def make_books(self, monitoring_info, requests):
        self._purgue_missing_requests(requests)
        self._purgue_requests_with_served_jobs(requests, monitoring_info)
        now = cpyutils.eventloop.now()
        r_ids_to_cleanup = []
        
        # Now we have only the books that we consider that are "alive"
        for r_id, book_info in self._bookings.items():
            current_request = requests.get_by_id(r_id)

            # This should not happen as the missing requests should have been already removed
            if current_request is None:
                continue
        
            if current_request.state == Request.ATTENDED:
                '''
                # A request that is attended is a request that is waiting for the resources to be available (i.e. powered on)
                # This is not needed, because if a node fails, the next block will set the the request to pending again.
                if (now - book_info.timestamp) > self._TIME_REQUEST_WAIT_RESOURCES:
                    _LOGGER.debug("request %s is not waiting anymore for resources %s" % (r_id, book_info.node_ids))
                    r_ids_to_cleanup.append(r_id)
                    current_request.set_state(Request.PENDING)
                    continue
                '''

                still_waiting = False
                # _LOGGER.debug("checking if booked resources for %s are ready" % r_id)
                for n_id in book_info.node_ids:
                    node = monitoring_info.nodelist.get_node(n_id)
                    
                    # We will wait for nodes if there are some of them that are being powered on; otherwise
                    # it has no sense to wait (if they are off, they won't be usable)
                    if node.state in [ Node.POW_OFF, Node.POW_ON, Node.ON_ERR ]:
                        still_waiting = True
                        break
                    
                if still_waiting or (current_request.timestamp_state > monitoring_info.timestamp_nodelist):
                    # _LOGGER.debug("request %s will be pending of its resources" % r_id)
                    for n_id in book_info.node_ids:
                        node = monitoring_info.nodelist.get_node(n_id)
                        node.allocate(current_request.resources.resources)
                else:
                    _LOGGER.debug("request %s now has all the nodes that it was waiting for in a state in which the request should be reconsidered" % r_id)
                    r_ids_to_cleanup.append(r_id)
                    current_request.set_state(Request.PENDING)

            elif (current_request.state == Request.SERVED):
                # Once the request is served, the LRMS monitoring system will not take into account immdiately, so we'll allocate the resources for a while to take them into account
                if (monitoring_info.timestamp_nodelist - current_request.timestamp_state < self._COOLDOWN_SERVED_REQUESTS):
                    for n_id in book_info.node_ids:
                        node = monitoring_info.nodelist.get_node(n_id)
                        node.allocate(current_request.resources.resources)
                else:
                    _LOGGER.debug("deallocating resources for request %s, as it was served some time ago" % r_id)
                    r_ids_to_cleanup.append(r_id)

        self._purgue_ids(r_ids_to_cleanup)

class CLUES_Scheduler_Reconsider_Jobs(CLUES_Scheduler):
    def __init__(self):
        CLUES_Scheduler.__init__(self, "Simple power On Nodes for jobs that have been waiting for too long")
        cpyutils.config.read_config("scheduling",
            {
                "RECONSIDER_JOB_TIME": 60,                 # Tiempo tras el que un trabajo que ha estado pending debe ser reconsiderado (creada una nueva request)
            },
            self)

    def schedule(self, requests_queue, monitoring_info, candidates_on, candidates_off):
        joblist = monitoring_info.joblist
        if joblist is None:
                _LOGGER.debug("could not get information about the jobs")
                return True

        #timestamp_joblist = monitoring_info.timestamp_joblist
        now = cpyutils.eventloop.now()
        
        for job_request in joblist:
            if job_request.state in [ Request.PENDING ]:
                inactivity_timestamp = max(job_request.timestamp_state, job_request.timestamp_attended)
                if (now - inactivity_timestamp) > self.RECONSIDER_JOB_TIME:
                    should_reconsider = True
                    for request in requests_queue:
                        if request.job_id == job_request.job_id:
                            if request.state == Request.SERVED:
                                # We should check if it has been recently served
                                if (now - request.timestamp_state) < self.RECONSIDER_JOB_TIME:
                                    _LOGGER.debug("job %s has been recently served, do no reconsider it" % job_request)
                                    should_reconsider = False
                            elif request.state not in [ Request.DISCARDED, Request.NOT_SERVED ]:
                                should_reconsider = False
                            
                    if should_reconsider:
                        _LOGGER.debug("job %s has been pending for too long... reconsidering it" % job_request)
                        job_request.set_state(Request.ATTENDED)
                        requests_queue.append(Request(job_request.resources, job_request.job_id, job_request.job_nodes_ids))
                    
                    # TODO: falta comprobar si hay alguna request "viva" con el job request id

        return True

# TODO: Eliminar; this is for debugging purposes only
def get_booking_system():
    global BOOKING_SYSTEM
    try:
        BOOKING_SYSTEM
    except:
        import configlib
        BOOKING_SYSTEM = BookingSystem(configlib._CONFIGURATION_MONITORING.COOLDOWN_SERVED_REQUESTS)
    return BOOKING_SYSTEM

class CLUES_Scheduler_PowOn_Requests(CLUES_Scheduler):
    def __init__(self):
        CLUES_Scheduler.__init__(self, "Simple power On Nodes for requests")
        cpyutils.config.read_config("monitoring",
            {
                "COOLDOWN_SERVED_REQUESTS": 1,                 # Tiempo que una request mantiene la reserva de recursos, una vez ya ha sido servida (tiene nodos validos)
            },
            self)
        
        self._sched_time = 0
        self._request_2_state = {}
        self._booking_system = get_booking_system() #BookingSystem(self.COOLDOWN_SERVED_REQUESTS)

    def __str__(self):
        return str(self._booking_system)

    def debug(self, req, str_debug):
        if req.id in self._request_2_state:
            if req.state != self._request_2_state[req.id]:
                self._request_2_state[req.id] = req.state
                _LOGGER.debug(str_debug)
        else:
            self._request_2_state[req.id] = req.state
            _LOGGER.debug(str_debug)

    def schedule(self, requests_queue, monitoring_info, candidates_on, candidates_off):
        local_candidates_on = collections.OrderedDict()
        requests_alive = 0
        
        self._booking_system.make_books(monitoring_info, requests_queue)
        # self._preprocess_requests_and_allocations(monitoring_info, requests_queue)
        nodelist = monitoring_info.nodelist
        nodes_powering_on = [ x.name for x in nodelist if x.state in [Node.POW_ON] ]
        # _LOGGER.debug("nodes powering on: %s" % nodes_powering_on)
        # _LOGGER.debug("scheduling @%d" % self._sched_time)
        # _LOGGER.debug("%s" % nodelist)
        self._sched_time += 1
        
        for current_req in requests_queue:
            # TODO: podriamos incluir aqui las "SERVED" que aun no han pasado el tiempo de COOLDOWN_SERVED_REQUESTS
            
            # ************ HAY QUE HACER LA OPCION DE "RESERVAR RECURSOS" Y ASI DEJAR LA PARTE COMUN AL FINAL Y NO OBLIGAR A QUE SE FILTRE DOS VECES
            should_allocate_resources = False
            node_pool = []
            nodes_on = []
            nodes_off = []
            nodes_powoff = []

            '''            
            if current_req.state == Request.ATTENDED:
                still_attended = True

                if requests_alive == 0:
                    # Let's see if there are enough resources to 
                    nodelist.FILTER_reset()
                    nodelist.FILTER_basic(current_req.resources.resources, states = [Node.IDLE, Node.USED, Node.ON_ERR])
                    node_count_on = nodelist.FILTER_count()
                    nodes_on = nodelist.get_nodelist_filtered().keys()
                    
                    if node_count_on >= current_req.resources.nodecount:
                        # We should return the allocated resources and treat it as pending (it will allocate the resources later)
                        self.debug(current_req, "#1'. we have %d nodes to serve request %s" % (node_count_on, current_req.id))
                        # _LOGGER.debug("#1'. we have %d nodes to serve request %s" % (node_count_on, current_req.id))
                        current_req.set_state(request.Request.SERVED)

                        node_pool = nodes_on
                        should_allocate_resources = True
                        still_attended = False
                
                if still_attended:
                    # There is a live request, it is waiting for resources, but latter requests should wait for this one
                    # This avoids that requests that cannot be satisfied will be freed and steal the resources
                    requests_alive += 1
                    continue
            '''
            
            if (current_req.state == Request.PENDING) or (current_req.state == Request.BLOCKED):
                node_pool = []
                nodes_on = []
                nodes_off = []
                nodes_powoff = []

                # 0. let's check if we can do anything with this request
                # _LOGGER.debug("checking request %s" % current_req.id)

                request_held = True

                nodelist.FILTER_reset()
                nodelist.FILTER_basic(current_req.resources.resources, states = [Node.IDLE, Node.USED, Node.ON_ERR])
                node_count_on = nodelist.FILTER_count()
                nodes_on = nodelist.get_nodelist_filtered().keys()
                
                # Case 1: are there enough resources with those that are ON?
                resources_met, nodes_meeting_resources = _nodes_meet_resources(current_req.resources, nodelist, nodes_on)
                if resources_met:
                # if node_count_on >= current_req.resources.nodecount:
                    
                    if requests_alive > 0:
                        # The request is blocked by the scheduler, because even it has resources to be served we want that the jobs get the queue in the same order
                        # that they have been submitted. Nevertheless we are allocating the resources, to continue scheduling powering on nodes (e.g. check whether other
                        # requests in the queue need more resources to be powered on)
                        if current_req.state == Request.PENDING:
                            self.debug(current_req, "#1. we have %d nodes to serve request %s, but there are other alive requests (%d)" % (node_count_on, current_req.id, requests_alive))
                            # _LOGGER.debug("#1. we have %d nodes to serve request %s, but there are other alive requests (%d)" % (node_count_on, current_req.id, requests_alive))
                            
                        current_req.set_state(request.Request.BLOCKED)
                    else:
                        # self.debug(current_req, "#1. we have %d nodes to serve request %s" % (node_count_on, current_req.id))
                        _LOGGER.debug("#1. we have %d nodes to serve request %s" % (node_count_on, current_req.id))
                        current_req.set_state(request.Request.SERVED)

                    node_pool = nodes_on
                    request_held = False

                # Case 2: (if not served) are there enough resources with those that are being powered on?
                if request_held:
                    nodelist.FILTER_reset()
                    nodelist.FILTER_basic(current_req.resources.resources, states = [Node.POW_ON])
                    node_count_powon = nodelist.FILTER_count()
                    nodes_powon = nodelist.get_nodelist_filtered().keys()
    
                    node_pool = node_pool + nodes_powon # The nodes that are powering on are likely to be used to serve the request
                    resources_met, nodes_meeting_resources = _nodes_meet_resources(current_req.resources, nodelist, node_pool)
                    if resources_met:
                    # if node_count_on + node_count_powon >= current_req.resources.nodecount:
                        # _LOGGER.debug("#2. there are nodes that are powering on that will serve request %s" % (current_req.id))
                        self.debug(current_req, "#2. there are nodes that are powering on that will serve request %s" % (current_req.id))
                        current_req.set_state(request.Request.ATTENDED)

                        request_held = False

                # If we have reached the limit of nodes powering on, we will not power on more nodes
                if request_held:
                    if (config_scheduling.MAX_BOOTING_NODES > 0) and ((len(nodes_powering_on) + len(local_candidates_on.keys())) >= config_scheduling.MAX_BOOTING_NODES):
                        _LOGGER.debug("Maximum number of booting nodes achieved (%s)... will wait so see what happens" % config_scheduling.MAX_BOOTING_NODES)
                        break

                # Case 3: (if not served) are there enough resources with those that we already want to power on? (remember that we are allocating the resources for the requests that forced to power on the resources)
                #      *** remember that we serve multiple requests in one pass, so there are nodes that we will call for power on when we finish scheduling, but they are not being powered on for the monitor, yet.
                # TODO: it is possible to include a system to recover OFF_ERR nodes and re-try powering on nodes
                if request_held:
                    nodelist.FILTER_reset()
                    nodelist.FILTER_basic(current_req.resources.resources, enabled = True, states = [Node.OFF])
                    node_count_off = nodelist.FILTER_count()
                    nodes_off = nodelist.get_nodelist_filtered().keys()
    
                    nodes_off_but_powon = [ n_id for n_id in nodes_off if n_id in local_candidates_on ]
                    node_count_off_but_powon = len(nodes_off_but_powon)
    
                    node_pool = node_pool + nodes_off_but_powon # The nodes that we already want to power on are likely to be used to serve the request
                    resources_met, nodes_meeting_resources = _nodes_meet_resources(current_req.resources, nodelist, node_pool)
                    if resources_met:
                    # if node_count_on + node_count_powon + node_count_off_but_powon >= current_req.resources.nodecount:
                        self.debug(current_req, "#3. we have requested to power on nodes that will serve %s (%s)" % (current_req.id, str(nodes_off_but_powon)))
                        current_req.set_state(request.Request.ATTENDED)

                        request_held = False

                # Case 4: (if not served) are there enough resources with those that are off?
                if request_held:
                    nodes_off_off = [ n_id for n_id in nodes_off if n_id not in local_candidates_on ]
                    node_count_off_off = len(nodes_off_off)
    
                    node_pool = node_pool + nodes_off_off   # The nodes off are likely to be used to serve the request
                    resources_met, nodes_meeting_resources = _nodes_meet_resources(current_req.resources, nodelist, node_pool)
                    if resources_met:
                    # if node_count_on + node_count_powon + node_count_off_but_powon + node_count_off_off >= current_req.resources.nodecount:
                        self.debug(current_req, "#4. we need to power on some nodes that are off to serve %s (%s)" % (current_req.id, str(nodes_off_off)))
                        current_req.set_state(request.Request.ATTENDED)

                        request_held = False

                # Case 5: (if not served) will be enough resources with those that are being powered off?
                if request_held:
                    nodelist.FILTER_reset()
                    nodelist.FILTER_basic(current_req.resources.resources, enabled = True, states = [Node.POW_OFF])
                    node_count_powoff = nodelist.FILTER_count()
                    nodes_powoff = nodelist.get_nodelist_filtered().keys()
    
                    
                    node_pool = node_pool + nodes_powoff    # The nodes that are being powered off are likely to be used to serve the request
                    resources_met, nodes_meeting_resources = _nodes_meet_resources(current_req.resources, nodelist, node_pool)
                    if resources_met:
                    # if node_count_on + node_count_powon + node_count_off_but_powon + node_count_off_off + node_count_powoff >= current_req.resources.nodecount:
                        self.debug(current_req, "#5. we need to wait for some nodes that are being powered off to serve %s (%s)" % (current_req.id, str(nodes_powoff)))
                        current_req.set_state(request.Request.ATTENDED)

                        request_held = False

                # Case 6: (if not served) we cannot do anything with the request... it may be because there are not enough resources or because there are not enough *free* resources; anyway we should power on as much resources as we can
                # TODO: should this be configurable? Later in job reevaluation it may be corrected
                # TODO: should wait for the nodes that are being powered on BEFORE setting it as "not served"? (maybe setting it to BLOCKED status)
                if request_held:
                    # We cannot do anything with this, so we'll free it if there are no pending requests in the queue
                    if requests_alive == 0:
                        if len(node_pool) == 0:
                            _LOGGER.debug("#6. request %s cannot be satisfied, but there are no requests pending from powering on nodes, so let's free this request" % current_req.id)
                            current_req.set_state(request.Request.NOT_SERVED)
                        else:
                            # If there are nodes to serve at least partially the request, we are marking it as attended, to wait for the nodes to be powered on
                            self.debug(current_req, "#6. request %s cannot be satisfied, but some nodes can be powered on" % current_req.id)
                            current_req.set_state(request.Request.BLOCKED)
                        
                        # node_pool = []
                        request_held = False
                        
                        # WARNING TODO: Importante... comprobar que esto es correcto y esta funcionando correctamente!!! Seguro que se deben reservar recursos en este caso?

                if request_held:
                    # If we cannot do anything with this request, we'll wait to the next schedule
                    break   
                else:
                    should_allocate_resources = True

            if should_allocate_resources:
                # -----------------------------------
                # Important:
                # TODO: CHECK if, in case that the current request is served the number of alive requests should be decreased in one (otherwise) the requests are served sequentially (i.e. only one on each schedule step)
                # -----------------------------------

                # We have to allocate the resources
                # still_needed, allocated_nodes = _allocate_nodes(current_req.resources.resources, nodelist, node_pool, current_req.resources.nodecount)
                still_needed = _allocate_nodes0(current_req.resources, nodelist, nodes_meeting_resources)
                self.debug(current_req, "nodes allocated for %s: %s" % (current_req.id, str(nodes_meeting_resources)))
                if still_needed > 0:
                    self.debug(current_req, "%d allocated nodes, but still need %d more nodes to serve the request" % (len(nodes_meeting_resources), still_needed))

                nodes_off = [ x for x in nodes_meeting_resources if (x in nodes_off) or (x in nodes_powoff) ]
                for n_id in nodes_off:
                    if n_id not in candidates_on:
                        local_candidates_on[n_id] = []
                    local_candidates_on[n_id].append(current_req)

                self._booking_system.book(current_req, nodes_meeting_resources)

            if current_req.state in [ Request.PENDING, Request.ATTENDED ]:
                requests_alive += 1
                # _LOGGER.debug("request %s still alive (%d)" % (current_req.id, requests_alive))

        candidates_on.update(local_candidates_on)
        return True

class CLUES_Scheduler_PowOn_Free(CLUES_Scheduler):
    def __init__(self):
        CLUES_Scheduler.__init__(self, "Power On extra nodes to maintain a set of slots or nodes free")
        cpyutils.config.read_config("scheduling",
            {
                "EXTRA_SLOTS_FREE": 0,
                "EXTRA_NODES_FREE": 0,
                "EXTRA_NODES_PERIOD": 30
            },
            self)
        self._timestamp_run = cpyutils.eventloop.now()

    def schedule(self, requests_queue, monitoring_info, candidates_on, candidates_off):

        now = cpyutils.eventloop.now()
        if (now - self._timestamp_run < self.EXTRA_NODES_PERIOD) and (len(candidates_off) == 0):
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
                if node.power_on_operation_failed < config_scheduling.RETRIES_POWER_ON:
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