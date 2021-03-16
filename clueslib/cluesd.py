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
import cpyutils.db
import cpyutils.eventloop
import logging
import time
from . import schedulers
from . import helpers
import collections
from . import hooks
from clues.configserver import _CONFIGURATION_MONITORING, _CONFIGURATION_CLUES
from .node import Node, NodeList, NodeInfo
from .request import JobList, RequestList, Request

import cpyutils.log
_LOGGER = cpyutils.log.Log("CLUES")

class DBSystem_dummy(object):
    def __init__(self):
        self._hosts = collections.OrderedDict()

    def enable_host(self, host, enable = True):
        self._hosts[host]=enable
        
    def store_node_info(self, host_data):
        if host_data.name not in self._hosts:
            self.enable_host(host_data.name, True)
    
    def retrieve_latest_monitoring_data(self):
        return None
    
    def get_hosts(self):
        return self._hosts

class DBSystem(DBSystem_dummy):
    @staticmethod
    def create_from_connection_string(connection_string = None):
        if connection_string is None:
            connection_string = _CONFIGURATION_CLUES.DB_CONNECTION_STRING
        if connection_string.strip() == "":
            return DBSystem_dummy()
        else:
            return DBSystem(connection_string)
    
    def __init__(self, connection_string):
        self._connection_string = connection_string
        self._db = cpyutils.db.DB.create_from_string(connection_string)
        self._create_db()
        self._hosts = collections.OrderedDict()
        self._get_hosts()

    def _get_hosts(self):
        result, row_count, rows = self._db.sql_query("SELECT name, enabled from hostdata")
        _hosts = collections.OrderedDict()
        if result:
            for (name, enabled) in rows:
                _hosts[ name ] = helpers.str_to_bool(enabled)
        else:
            _LOGGER.error("could not obtain hostdata from database")
        self._hosts = _hosts
        return True

    def _create_db(self):
        result1, _, _ = self._db.sql_query("CREATE TABLE IF NOT EXISTS hostdata(name varchar(128) PRIMARY KEY, enabled bool)", True)
        result2, _, _ = self._db.sql_query("CREATE TABLE IF NOT EXISTS host_monitoring(name varchar(128), timestamp_state INTEGER, slots_count INTEGER, slots_free INTEGER, memory_total INTEGER, memory_free INTEGER, state INTEGER, timestamp INTEGER, x INTEGER PRIMARY KEY)", True)
        result3, _, _ = self._db.sql_query("CREATE TABLE IF NOT EXISTS requests(reqid varchar, timestamp_created INTEGER, timestamp_state INTEGER, state INTEGER, slots INTEGER, memory INTEGER, expressions varchar, taskcount INTEGER, maxtaskspernode INTEGER, jobid varchar, nodes varchar, x INTEGER PRIMARY KEY)", True)
        return (result1 and result2 and result3)

    def enable_host(self, host, enable = True):
        if not self._get_hosts(): return False
        
        if host in self._hosts:
            result, _, _ = self._db.sql_query("UPDATE hostdata SET enabled = '%s' WHERE name = '%s'" % (helpers.bool_to_str(enable), host), True)
        else:
            result, _, _ = self._db.sql_query("INSERT INTO hostdata VALUES (\"%s\", \"%s\")" % (host, helpers.bool_to_str(enable)), True)

        if result:
            self._hosts[ host ] = enable
        return result

    def store_node_info(self, host_data):
        now = cpyutils.eventloop.now()
        result, _, _ = self._db.sql_query("INSERT INTO host_monitoring(name, timestamp_state, slots_count, slots_free, memory_total, memory_free, state, timestamp) VALUES (\"%s\", %s, %s, %s, %s, %s, %s, %s)" % \
                                          ( host_data.name, \
                                           host_data.timestamp_state, \
                                           host_data.slots_count, \
                                           host_data.slots_free, \
                                           host_data.memory_total, \
                                           host_data.memory_free, \
                                           host_data.state, \
                                           now \
                                           )\
                                          , True)
        
        if host_data.name not in self._hosts:
            self.enable_host(host_data.name, True)

    def store_request_info(self, request):
        import json
        result, _, _ = self._db.sql_query("INSERT INTO requests(reqid, timestamp_created, timestamp_state, state, slots, memory, expressions, taskcount, maxtaskspernode, jobid, nodes) VALUES (\"%s\", %s, %s, %d, %d, %d, '%s', %d, %d, \"%s\", '%s')" % \
        (request.id, \
        request.timestamp_created, \
        request.timestamp_state, \
        request.state, \
        request.resources.resources.slots, \
        request.resources.resources.memory, \
        json.dumps(request.resources.resources.requests), \
        request.resources.taskcount, \
        request.resources.maxtaskspernode, \
        request.job_id if request.job_id is not None else "null", \
        json.dumps(request.job_nodes_ids)), True)
        return result

    def update_request_info(self, request):
        result, _, _ = self._db.sql_query("UPDATE requests SET timestamp_state = %d, state = %d WHERE reqid = \"%s\"" % \
        ( request.timestamp_state, request.state, request.id ), True)
        return result

    def retrieve_latest_monitoring_data(self):
        result, row_count, rows = self._db.sql_query("select max(timestamp), m.*, d.enabled from host_monitoring as m left join hostdata as d on m.name=d.name group by m.name")
        _nodes = collections.OrderedDict()
        if result:
            for (timestamp, name, timestamp_state, slots_count, slots_free, memory_total, memory_free, state, _, _, enabled) in rows:
                n = Node(name, slots_count, slots_free, memory_total, memory_free)
                n.state = state
                # int is suggested by 2to3; will accept
                n.timestamp_state = int(timestamp_state) 
                n.enabled = helpers.str_to_bool(enabled)
                _nodes[name] = n
        else:
            _LOGGER.error("could not obtain hostdata from database")
            return None
        return _nodes

def _update_enable_status_for_nodes(_lrms_nodelist, _hosts_state):
    # We will disable the nodes that are configured to not to be treated by CLUES
    disabled_nodes = _CONFIGURATION_CLUES.DISABLED_HOSTS.split(" ")
    for nname in disabled_nodes:
        if nname in _lrms_nodelist:
            # _LOGGER.warning("disabling node %s due to configuration" % nname)
            _lrms_nodelist[nname].enabled = False
            
    disabled = [ x for x, state in list(_hosts_state.items()) if state == False ]
    enabled = [ x for x, state in list(_hosts_state.items()) if state == True ]
    for nname in disabled:
        if nname in _lrms_nodelist:
            _lrms_nodelist[nname].enabled = False
    for nname in enabled:
        if nname in _lrms_nodelist:
            _lrms_nodelist[nname].enabled = True
            
class MonitoringInfo():
    def __init__(self, nodelist, timestamp_nodelist, joblist, timestamp_joblist):
        self.nodelist = nodelist
        self.timestamp_nodelist = timestamp_nodelist
        self.joblist = joblist
        self.timestamp_joblist = timestamp_joblist
        
    @staticmethod
    def create_from_platform(platform):
        lrms_nodelist = platform.get_nodeinfolist()
        lrms_jobinfolist = platform.get_jobinfolist()
        now = cpyutils.eventloop.now()
        
        if lrms_nodelist is None:
            _LOGGER.warning("could not obtain the node list from the platform in the last %s seconds" % err_time)
            lrms_nodelist = collections.OrderedDict()
        
        # Now we are updating the information about the nodes in the monitor
        _lrms_nodelist = collections.OrderedDict()
        for n_id, node in list(lrms_nodelist.items()):
            _lrms_nodelist[n_id] = Node.create_from_nodeinfo(node.get_nodeinfo())
        
        # This is done here in order to get the list of nodes ready to be used (and avoid calling DB each time the list is retrieved)
        _update_enable_status_for_nodes(_lrms_nodelist, {})
            
        # Now we are updating the items in the lrms_jobinfo
        _lrms_joblist = JobList()
        for job in lrms_jobinfolist:
            _lrms_joblist.append(job)
                
        return MonitoringInfo(_lrms_nodelist, now, _lrms_joblist, now)

class CluesDaemon:
    def __init__(self, platform, _schedulers = []):
        self._platform = platform
        platform.attach_clues_system(self)
        self._lrms_nodelist = None          # Currently it is only a dictionary... should it change to a NodeList? (in coherence with the architecture of the jobs)
        self._lrms_joblist = None           # It is a list of jobs JobList structure that manages the queue and the information
        self._schedulers = _schedulers
        
        # self._lrms_requests = None
        self._requests_queue = RequestList()
        self._timestamp_nodelist = -1
        self._timestamp_joblist = -1
        self._timestamp_mark = cpyutils.eventloop.now()
        
        self._db_system = DBSystem.create_from_connection_string()

    def get_nodelist(self):
        if self._lrms_nodelist is not None:
            return NodeList(self._lrms_nodelist)
        return NodeList(collections.OrderedDict())

    def get_node(self, nname):
        if self._lrms_nodelist is None:
            return None
        if nname in self._lrms_nodelist:
            return self._lrms_nodelist[nname]
        return None

    def get_monitoring_info(self):
        return MonitoringInfo(NodeList(self._lrms_nodelist), self._timestamp_nodelist, self._lrms_joblist, self._timestamp_joblist)

    def _update_disabled_nodes(self):
        _update_enable_status_for_nodes(self._lrms_nodelist, self._db_system.get_hosts())

    def request(self, request):
        hooks.HOOKS.request(request)
        self._db_system.store_request_info(request)
        self._requests_queue.append(request)
        _LOGGER.debug("new request: %s" % request)
        # cpyutils.eventloop.get_eventloop().add_event(schedulers.config_scheduling.PERIOD_SCHEDULE, "CONTROL EVENT - the request will be scheduled", stealth = True)
        return request.id
    
    def get_requests_list(self):
        return self._requests_queue
    
    def get_job_list(self):
        return self._lrms_joblist
    
    def request_in_queue(self, r_id):
        req = self._requests_queue.get_by_id(r_id)
        if req is None:
            return False
        if req.state in [ Request.SERVED, Request.NOT_SERVED, Request.DISSAPEARED ]:
            return False
        return True

    def _monitor_lrms_nodes(self):
        lrms_nodelist = self._platform.get_nodeinfolist()
        now = cpyutils.eventloop.now()
        
        if lrms_nodelist is None:
            err_time = now - self._timestamp_nodelist
            if err_time < _CONFIGURATION_MONITORING.PERIOD_MONITORING_NODES_FAIL_GRACE:
                _LOGGER.debug("could not obtain the node list from the platform in the last %s seconds, but we are in grace time" % err_time)
                return
            
            _LOGGER.warning("could not obtain the node list from the platform in the last %s seconds" % err_time)
            lrms_nodelist = collections.OrderedDict()

        if self._lrms_nodelist is None:
            self._lrms_nodelist = collections.OrderedDict()

            node_str = ""
            for n_id, node in list(lrms_nodelist.items()):
                self._lrms_nodelist[n_id] = Node.create_from_nodeinfo(node.get_nodeinfo())
                node_str = "%s%s\n" % (node_str, str(self._lrms_nodelist[n_id]))
                
            # Now we'll get all the remaining nodes in the BD
            
            existing_hosts = self._db_system.get_hosts()
            latest_monitoring_data = self._db_system.retrieve_latest_monitoring_data()
            for h_id in existing_hosts:
                if h_id not in self._lrms_nodelist:
                    if h_id in latest_monitoring_data:
                        ni = latest_monitoring_data[h_id].get_nodeinfo()
                    else:
                        ni = NodeInfo(h_id, -1, -1, -1, -1)
                    unknown_node = Node.create_from_nodeinfo(ni)
                    unknown_node.set_state(Node.UNKNOWN)
                    self._lrms_nodelist[h_id] = unknown_node

            _LOGGER.debug("\nFirst monitorization of LRMS:\nList of nodes:\n%s" % node_str)

        # Now we are updating OUR information from the information that we got from the nodelist
        nodes_new = []
        nodes_changed = []
        
        for n_id, node in list(lrms_nodelist.items()):
            nodeinfo = node.get_nodeinfo()

            if n_id in self._lrms_nodelist:
                _, state_changed, resources_changed = self._lrms_nodelist[n_id].update_info(nodeinfo)
                if state_changed or resources_changed:
                    nodes_changed.append(n_id)
            else:
                _LOGGER.warning("node %s has just appeared" % n_id)
                self._lrms_nodelist[n_id] = Node.create_from_nodeinfo(nodeinfo)
                nodes_new.append(n_id)
                
        for n_id, node in list(self._lrms_nodelist.items()):
            if (node.state != Node.UNKNOWN) and (n_id not in lrms_nodelist):
                _LOGGER.warning("node %s has dissapeared!" % n_id)
                # TODO: should delete the node?
                node.set_state(Node.UNKNOWN)
                nodes_changed.append(n_id)
                
        self._update_disabled_nodes()
        self._timestamp_nodelist = now

        for n_id, node in list(self._lrms_nodelist.items()):
            if (node.state == Node.POW_ON) and ((now - node.timestamp_state) > _CONFIGURATION_MONITORING.MAX_WAIT_POWERON):
                node.set_state(Node.OFF_ERR)
                nodes_changed.append(n_id)
            if (node.state == Node.POW_OFF) and ((now - node.timestamp_state) > _CONFIGURATION_MONITORING.MAX_WAIT_POWEROFF):
                node.set_state(Node.ON_ERR)
                nodes_changed.append(n_id)
                
        # Now we should update the DB
        for n_id in nodes_changed + nodes_new:
            _LOGGER.debug("node %s changed its state" % n_id)
            self._db_system.store_node_info(self._lrms_nodelist[n_id])
                        
    def _monitor_lrms_jobs(self):
        lrms_jobinfolist = self._platform.get_jobinfolist()
        now = cpyutils.eventloop.now()
        
        if lrms_jobinfolist is None:
            err_time = now - self._timestamp_joblist
            if err_time < _CONFIGURATION_MONITORING.PERIOD_MONITORING_JOBS_FAIL_GRACE:
                _LOGGER.debug("could not obtain the job list from the platform in the last %s seconds, but we are in grace time" % err_time)
                return
            
            _LOGGER.warning("could not obtain the job list from the platform in the last %s seconds" % err_time)
            lrms_jobinfolist = collections.OrderedDict()

        # Now we are updating the items in the lrms_jobinfo
        if self._lrms_joblist is None:
            _LOGGER.debug("first monitorisation of jobs in LRMS\n")
            self._lrms_joblist = JobList()
            for job in lrms_jobinfolist:
                self._lrms_joblist.append(Request.create_from_jobinfo(job))

        stored_j_ids = self._lrms_joblist.get_ids()
        for job_info in lrms_jobinfolist:
            j_id = job_info.job_id
            if j_id in stored_j_ids:
                job = self._lrms_joblist.get_by_id(j_id)
                job.update_info(job_info)
            else:
                _LOGGER.warning("job %s has just appeared" % j_id)
                self._lrms_joblist.append(Request.create_from_jobinfo(job_info))

        # First we are detecting the missing jobs
        job_ids = [ j.job_id for j in lrms_jobinfolist ]
        j_ids_to_delete = []

        for job in self._lrms_joblist:
            # Final states are SERVED, NOT_SERVED y DISSAPEARED, so if it the job is not in the observed list but it was in a temporary state (e.g. ATTENDED), it has dissapeared
            if (job.state not in [ Request.SERVED, Request.NOT_SERVED, Request.DISSAPEARED ]) and (job.job_id not in job_ids):
                job.set_state(Request.DISSAPEARED)

            # These are jobs that are kept because the job list is incremental and it is not substituted
            if (job.state in [ Request.NOT_SERVED, Request.SERVED, Request.DISSAPEARED ]) and ((now - job.timestamp_state) > _CONFIGURATION_MONITORING.COOLDOWN_SERVED_JOBS):
                j_ids_to_delete.append(job.job_id)
                
        for job_id in j_ids_to_delete:
            self._lrms_joblist.del_by_id(job_id)
                
        self._timestamp_joblist = now
        
    def _monitor_lrms_nodes_and_jobs(self):
        # This method is to make a doble monitoring, in case that the frequency is the same (in order to avoid double connections)
        self._monitor_lrms_nodes()
        self._monitor_lrms_jobs()

    def _purge_served_requests(self):
        now = cpyutils.eventloop.now()
        
        r_ids_to_delete = []
        for req in self._requests_queue:
            # These are jobs that are kept because the job list is incremental and it is not substituted
            if (req.state in [ Request.NOT_SERVED, Request.SERVED, Request.DISSAPEARED ]) and ((now - req.timestamp_state) > _CONFIGURATION_MONITORING.COOLDOWN_SERVED_REQUESTS):
                self._db_system.update_request_info(req)
                r_ids_to_delete.append(req.id)
                
        for r_id in r_ids_to_delete:
            self._requests_queue.del_by_id(r_id)

    #def _purge_served_jobs(self):
    #    now = cpyutils.eventloop.now()
    #    served_ids = []
    #    for job in self._lrms_joblist:
    #        if job.state in [ Request.SERVED, Request.NOT_SERVED ]:
    #            served_ids.append(job.id)
    #            
    #    # This is a purging mechanism to delete jobs that are for a stable state and will not
    #    # be needed anymore
    #    eps_served = now - _CONFIGURATION_MONITORING.COOLDOWN_SERVED_JOBS
    #    
    #    for r_id in served_ids:
    #        job = self._lrms_joblist.get_by_id(r_id)
    #        if job.timestamp_state < eps_served:
    #            self._lrms_joblist.del_by_id(r_id)
    #            
    #    # TODO: mirar tambien las requests?
    #
    #def _purge_dissapeared_jobs(self):
    #    now = cpyutils.eventloop.now()
    #    missing_ids = []
    #    for job in self._lrms_joblist:
    #        if job.state == Request.DISSAPEARED:
    #            missing_ids.append(job.id)
    #            
    #    # This is a purging mechanism to delete jobs that are for a stable state and will not
    #    # be needed anymore
    #    eps_dissapeared = now - _CONFIGURATION_MONITORING.COOLDOWN_DISSAPEARED_JOBS
    #    
    #    for r_id in missing_ids:
    #        job = self._lrms_joblist.get_by_id(r_id)
    #        if job.timestamp_state < eps_dissapeared:
    #            self._lrms_joblist.del_by_id(r_id)
    #
    #    # TODO: mirar tambien las requests?
                
    def enable_host(self, n_id, enable = True):
        # TODO: currently we do not allow to enable or disable a host that has not been monitored, but if one node dissapears, it will be enabled (or disabled) forever in the DB
        if n_id in self._lrms_nodelist:
            self._db_system.enable_host(n_id, enable)
            self._update_disabled_nodes()
            return True, ""
        else:
            return False, "Node %s does not exist" % n_id

    def reset_node_state(self, n_id):
        if n_id in self._lrms_nodelist:
            node = self._lrms_nodelist[n_id]

            _LOGGER.debug("Resetting the state of the node %s to %s" % (n_id, node.state2str[node.IDLE]))

            node.set_state(Node.IDLE, True)
            self._db_system.store_node_info(node)
            return True, "Node %s reset to %s" % (n_id, node.state2str[node.state])
        else:
            return False, "Node is not managed by CLUES"

    def power_off(self, n_id, force = False):
        if n_id in self._lrms_nodelist:
            node = self._lrms_nodelist[n_id]
            
            if (not node.enabled) and (not force):
                _LOGGER.warning("node %s cannot be powered off because is disabled" % n_id)
                return False, ""
            
            if node.state in [ Node.OFF, Node.POW_OFF, Node.OFF_ERR ]:
                _LOGGER.warning("the node is already OFF or it is being powered off")
                return True, n_id

            hooks.HOOKS.pre_poweroff(n_id)
            success, nname = self._platform.power_off(n_id)
            if success:
                if nname != n_id:
                    _LOGGER.warning("tried to power off %s but LRMS powered off %s" % (n_id, nname))
                    n_id = nname
                    node = self._lrms_nodelist[n_id]

                node.set_state(Node.POW_OFF)
                self._db_system.store_node_info(node)
                hooks.HOOKS.post_poweroff(n_id, 1, nname)

                return True, n_id
            else:
                _LOGGER.warning("could not power off node %s. It will be considered ON, but with errors" % n_id)
                node.set_state(Node.POW_OFF)
                hooks.HOOKS.post_poweroff(n_id, 0, nname)

                node.set_state(Node.ON_ERR)
                self._db_system.store_node_info(node)
                return False, ""
        else:
            return False, ""

    def power_on(self, n_id, force = False):
        if n_id in self._lrms_nodelist:
            node = self._lrms_nodelist[n_id]

            if (not node.enabled) and (not force):
                _LOGGER.warning("node %s cannot be powered on because is disabled" % n_id)
                return False, ""

            if node.state in [ Node.IDLE, Node.USED, Node.POW_ON, Node.ON_ERR ]:
                _LOGGER.warning("the node is already ON or it is being powered on")
                return True, n_id
            
            hooks.HOOKS.pre_poweron(n_id)
            success, nname = self._platform.power_on(n_id)
            if success:
                if nname != n_id:
                    _LOGGER.warning("tried to power on %s but LRMS powered on %s" % (n_id, nname))
                    n_id = nname
                    node = self._lrms_nodelist[n_id]                
                
                node.set_state(Node.POW_ON)
                self._db_system.store_node_info(node)
                hooks.HOOKS.post_poweron(n_id, 1, nname)
                return True, n_id
            else:
                _LOGGER.warning("could not power on node %s. It will be considered OFF, but with errors" % n_id)
                node.set_state(Node.POW_ON)
                hooks.HOOKS.post_poweron(n_id, 0, nname)

                node.set_state(Node.OFF_ERR)
                self._db_system.store_node_info(node)
                return False, ""
        else:
            return False, ""

    def _schedulers_pipeline(self):
        self._purge_served_requests()
        now = cpyutils.eventloop.now()
        
        if (now - self._timestamp_mark) > _CONFIGURATION_CLUES.LOGGER_MARK:
            self._timestamp_mark = now
            _LOGGER.debug("--------- LOGGING MARK (everything continues) @ %f" % now)

        if (self._lrms_nodelist is None) and (self._lrms_joblist is None):
            # If there is no monitoring information up to now, we will wait till next call
            _LOGGER.debug("there is no monitoring info up by now... skipping scheduling")
            return
            
        nodelist = NodeList(self._lrms_nodelist)

        candidates_on = collections.OrderedDict()
        candidates_off = []
        
        monitoring_info = MonitoringInfo(nodelist, self._timestamp_nodelist, self._lrms_joblist, self._timestamp_joblist)
        for scheduler in self._schedulers:
            existing_requests = [x for x in self._requests_queue.get_list() ]
            if not scheduler.schedule(self._requests_queue, monitoring_info, candidates_on, candidates_off):
                _LOGGER.error("failed to schedule with scheduler %s" % str(scheduler))
            new_requests = [ x for x in self._requests_queue.get_list() if x not in existing_requests ]
            for r in new_requests:
                self._db_system.store_request_info(self._requests_queue.get_by_id(r))

        if len(candidates_off) > 0:
            _LOGGER.info("nodes %s are considered to be powered off" % str(candidates_off))            
            for n_id in candidates_off:
                self.power_off(n_id)                    
        if len(candidates_on) > 0:
            _LOGGER.info("nodes %s are considered to be powered on" % str(list(candidates_on.keys())))
            for n_id in candidates_on:
                self.power_on(n_id)

    def __str__(self):
        retval = ""
        for scheduler in self._schedulers:
            retval = "%s%s\n" % (retval, str(scheduler))
        return retval
        
    def _auto_recover_nodes(self):
        if self._lrms_nodelist is None:
            return
        
        # _LOGGER.debug("autorecovering nodes... (periof of recovery set by PERIOD_RECOVERY_NODES in config file)")
        
        recoverable_nodes = []
        
        for n_id, node in list(self._lrms_nodelist.items()):
            if (node.state in [ Node.OFF_ERR ]) and (node.power_on_operation_failed < schedulers.config_scheduling.RETRIES_POWER_ON):
                node.set_state(Node.OFF, True)
                self._db_system.store_node_info(node)
                recoverable_nodes.append(n_id)

            if (node.state in [ Node.ON_ERR ]) and (node.power_off_operation_failed < schedulers.config_scheduling.RETRIES_POWER_OFF):
                node.set_state(Node.IDLE, True)
                self._db_system.store_node_info(node)
                recoverable_nodes.append(n_id)

        if len(recoverable_nodes) > 0:
            _LOGGER.debug("have tried to recover nodes %s" % recoverable_nodes)

    def loop(self, real_time_mode = True):
        should_monitor_nodes = True
        if _CONFIGURATION_MONITORING.PERIOD_MONITORING_JOBS > 0:
            if _CONFIGURATION_MONITORING.PERIOD_MONITORING_JOBS == _CONFIGURATION_MONITORING.PERIOD_MONITORING_NODES:
                should_monitor_nodes = False
                _LOGGER.debug("monitoring jobs and nodes at the same time")
                cpyutils.eventloop.get_eventloop().add_event(cpyutils.eventloop.Event_Periodical(0, _CONFIGURATION_MONITORING.PERIOD_MONITORING_NODES, description = "monitoring nodes and jobs", callback = self._monitor_lrms_nodes_and_jobs, parameters = [], mute = True))
            else:
                cpyutils.eventloop.get_eventloop().add_event(cpyutils.eventloop.Event_Periodical(0, _CONFIGURATION_MONITORING.PERIOD_MONITORING_JOBS, description = "monitoring jobs", callback = self._monitor_lrms_jobs, parameters = [], mute = True))
        else:
            _LOGGER.info("not monitoring jobs due to configuration (var PERIOD_MONITORING_JOBS)")
            
        if should_monitor_nodes:
            cpyutils.eventloop.get_eventloop().add_event(cpyutils.eventloop.Event_Periodical(0, _CONFIGURATION_MONITORING.PERIOD_MONITORING_NODES, description = "monitoring nodes", callback = self._monitor_lrms_nodes, parameters = [], mute = True))

        cpyutils.eventloop.get_eventloop().add_event(cpyutils.eventloop.Event_Periodical(0, schedulers.config_scheduling.PERIOD_SCHEDULE, description = "scheduling", callback = self._schedulers_pipeline, parameters = [], mute = True))
        cpyutils.eventloop.get_eventloop().add_event(cpyutils.eventloop.Event_Periodical(0, _CONFIGURATION_MONITORING.PERIOD_LIFECYCLE, description = "lifecycle", callback = self._platform.lifecycle, parameters = [], mute = True))
        cpyutils.eventloop.get_eventloop().add_event(cpyutils.eventloop.Event_Periodical(0, schedulers.config_scheduling.PERIOD_RECOVERY_NODES, description = "recovery of nodes", callback = self._auto_recover_nodes, parameters = [], mute = True))

        cpyutils.eventloop.get_eventloop().loop()
