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
import cpyutils.eventloop
import logging
import time

import cpyutils.log
_LOGGER = cpyutils.log.Log("REQ")

class Resources:
    def __init__(self, slots, memory, requests_expressions = []):
        self.slots = slots
        self.memory = memory
        self.requests = requests_expressions[:]
    def __le__(self, other):
        if self.slots > other.slots: return False
        if self.memory > other.memory: return False
        return True
    def __ge__(self, other):
        if self.slots < other.slots: return False
        if self.memory < other.memory: return False
        return True
    def __str__(self):
        retval = "%.1f slots, %.0f memory, (%s) " % (self.slots, self.memory, str(self.requests))
        return retval

class ResourcesNeeded:
    def __init__(self, slots, memory, requests_expressions = [], nodecount = 1):
        self.resources = Resources(slots, memory, requests_expressions)
        self.nodecount = nodecount

    def __str__(self):
        retval = "%d nodes: %s" % (self.nodecount, str(self.resources))
        return retval

class _Request():
    PENDING = 0
    ATTENDED = 1        # It is being attended (its resources are being activated)
    SERVED = 2          # The request has been fully attended: its resources have been powered on and the request has been freed
    BLOCKED = 3         # The request could be served, but the scheduler has blocked it (maybe because there are other requests pending from powering on resources)
    DISSAPEARED = -1    # The request has dissapeared: it was observed (i.e. a job) and now it cannot be observed, but it is not kown if it has been served
    NOT_SERVED = -2
    DISCARDED = -3       # The request has been discarded by the server, probably because it has been attended too many times without success
    UNKNOWN = -4

    STATE2STR = { PENDING: 'pending', ATTENDED: 'attended', SERVED: 'served', BLOCKED: 'blocked', DISSAPEARED: 'dissapeared', NOT_SERVED: 'not-served', UNKNOWN: 'unknown' }
    
    def __init__(self, resources, job_id = None, job_nodes = [], req_id = None):
        self.id = req_id
        self.resources = resources
        self.timestamp_created = cpyutils.eventloop.now()
        self.timestamp_state = self.timestamp_created
        self.timestamp_attended = 0
        self.attended_retries = 0
        self.state = Request.PENDING
        self.job_id = job_id
        self.job_nodes_ids = job_nodes

    def set_state(self, state):
        if self.state != state:
            self.state = state
            self.timestamp_state = cpyutils.eventloop.now()
            if state == Request.ATTENDED:
                self.timestamp_attended = self.timestamp_state
                self.attended_retries += 1
            return True
        return False

    def update(self, req):
        self.resources = req.resources
        self.set_state(req.state)
        self.job_id = req.job_id
        self.job_nodes_ids = req.job_nodes_ids
        
    def update_info(self, job_info):
        # The code is exactly the same than the one before, but it is important to distinguishe that each of them refer to different information, just in case of future improvements
        self.resources = job_info.resources
        self.set_state(job_info.state)
        self.job_id = job_info.job_id
        self.job_nodes_ids = job_info.job_nodes_ids
        
    def _copy(self, new):
        new.resources = self.resources
        new.timestamp_state = self.timestamp_state
        new.timestamp_created = self.timestamp_created
        new.state = self.state
        new.id = self.id
        new.job_id = self.job_id
        new.job_nodes_ids = self.job_nodes_ids
        return new

    def copy(self):
        new = Request(self.resources)
        return self._copy(new)

    def __str__(self):
        retval = "[REQUEST %s] state: %s@%.2f; resources: %s; attended @%.2f; job id: %s; " % (self.id, self.STATE2STR[self.state], self.timestamp_state, self.resources, self.timestamp_attended, self.job_id)
        return retval

    @staticmethod
    def create_from_jobinfo(job_info):
        res = _Request(job_info.resources, job_info.job_id, job_info.job_nodes_ids)
        res.set_state(job_info.state)
        return res

class Request(_Request):
    _ID = 0
    @staticmethod
    def _get_id():
        Request._ID = Request._ID + 1
        return Request._ID

    def __init__(self, resources, job_id = None, job_nodes = []):
        _Request.__init__(self, resources, job_id, job_nodes, Request._get_id())

class JobInfo():
    def __init__(self, resources, job_id, nodes_ids):
        self.resources = resources
        self.job_id = job_id
        self.job_nodes_ids = nodes_ids
        self.state = Request.UNKNOWN

    def set_state(self, state):
        if self.state != state:
            self.state = state

    '''
    def to_request(self):
        req = Request(self.resources, self.job_id, self.node_ids)
        req.set_state(self.state)
        return req
    '''

class RequestList():
    class ExceptionAlreadyExists(Exception): pass
    
    def __init__(self, requests = []):
        self._current_request = -1
        self._requests = {}
        self._queue = []
        for req in requests:
            self.append(req)

    def begin_iterating(self):
        self._current_request = -1

    def next(self):
        if self._current_request >= len(self._queue)-1: raise StopIteration
        self._current_request += 1
        request = self._requests[self._queue[self._current_request]]
        return request

    def __iter__(self):
        self.begin_iterating()
        return self
    
    def __next__(self):
        return self.next()

    def get_list(self):
        return self._requests

    def append(self, req):
        if req.id in self._requests: raise RequestList.ExceptionAlreadyExists()
        
        self._requests[req.id] = req
        self._queue.append(req.id)
    
    def get_by_id(self, r_id):
        if r_id not in self._requests: return None
        return self._requests[r_id]
    
    def del_by_id(self, r_id):
        if r_id not in self._requests: return False

        i_to_delete = self._queue.index(r_id)

        del self._queue[i_to_delete]
        del self._requests[r_id]

        if self._current_request >= i_to_delete:
            self._current_request -= 1

        return True

    def __str__(self):
        retval = ""
        for r_id in self._queue:
            retval = "%s%s\n" % (retval, str(self._requests[r_id]))
        return retval

class JobList(RequestList):
    class ExceptionNotJob(Exception): pass
    
    @staticmethod
    def create_from_jobinfolist(jobinfolist = []):
        jl = JobList([Request.create_from_jobinfo(j) for j in jobinfolist])
        return jl
    
    def __init__(self, requests = []):
        self._job_id_to_req_id = {}
        RequestList.__init__(self, requests)

    def get_ids(self, state = None):
        if state is None:
            # return self._job_id_to_req_id.keys()
            return self._queue[:]
        else:
            job_ids = []
            for j_id in self._queue:
            # for j_id, r_id in self._job_id_to_req_id.items():
                if self._requests[j_id].state == state:
                    job_ids.append(j_id)
            return job_ids

    def append(self, req):
        # A JobList only contains Jobs, so job_id CANNOT be None
        if req.job_id is None: raise JobList.ExceptionNotJob()

        if req.job_id in self._requests: raise RequestList.ExceptionAlreadyExists()
        
        self._requests[req.job_id] = req
        self._queue.append(req.job_id)
        self._job_id_to_req_id[req.job_id] = req.id

    '''   
    def get_by_job_id(self, j_id):
        if j_id not in self._job_id_to_req_id: return None
        return self._requests[self._job_id_to_req_id[j_id]]

    def del_by_id(self, r_id):
        if r_id not in self._requests: return False
        
        job_id = self._requests[r_id].job_id
        
        if RequestList.del_by_id(self, r_id):
            del self._job_id_to_req_id[job_id]
            return True
        
        return False
    '''