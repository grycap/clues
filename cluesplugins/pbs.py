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
import clueslib.configlib
import logging
import clueslib.node
import clueslib.helpers
import cpyutils.runcommand
from clueslib.platform import PowerManager
from clueslib.node import Node, NodeInfo, NodeList
from clueslib.request import JobInfo, Request
from cpyutils.xmlobject import XMLObject_KW, XMLObject
import cpyutils.evaluate
import re
import collections

try:
    _annie
except:
    _annie = cpyutils.evaluate.Analyzer()

# TODO: check if information about nodes is properly set (check properties, queues and so on)
import cpyutils.log
_LOGGER = cpyutils.log.Log("PLUGIN-PBS")

def _translate_mem_value(memval):
    memval = memval.lower().rstrip(".").strip()
    
    multiplier = 1
    if len(memval) > 0:
        qualifier = memval[-2:]
        if qualifier == 'kb':
            multiplier = 1024
        elif qualifier == 'mb':
            multiplier = 1024*1024
        elif qualifier == 'gb':
            multiplier = 1024*1024*1024
        elif qualifier == 'tb':
            multiplier = 1024*1024*1024*1024
        elif qualifier == 'pb':
            multiplier = 1024*1024*1024*1024*1024
        
    if multiplier > 1:
        value_str = memval[:-2]
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

class _Nodespec:
    # cadena="nodes=server:hippi+10:ppn=1:noserver+3:bigmem:hippi,walltime=10:00:00"
    # Copied from clues-pbs-wrapper
    # TODO: Merge in a unique location
    
    @staticmethod
    def createFromOptions(request_str):
        # resource = resource_expr , resource2 = resource_expr2
        _nodes_requested = []
        
        requests = request_str.split(",")
        
        for resource_request in requests:
            # resource = resource_expr
            resource_request_a = resource_request.split("=", 1)
            
            if len(resource_request_a) == 2:
                resource, resspec_str = resource_request_a
                
                if resource == 'nodes':
                    # nodes = nodeset1 + nodeset2 + ... 
                    nodespec_a = resspec_str.split("+")
                    
                    for nodespec_str in nodespec_a:
                        # nodeset = {<node_count> | <hostname>} [:ppn=<ppn>][:gpus=<gpu>][:<property>[:<property>]...] [+ ...]
                        nodespec_a = nodespec_str.split(":")
                        if len(nodespec_a) > 0:
                            nodestring = nodespec_a[0]
                            nodename = None
                            node_requested = _Nodespec()
                            
                            # {<node_count> | <hostname>}
                            try:
                                nodecount = int(nodestring)
                                node_requested.nodecount = nodecount
                            except:
                                node_requested.nodecount = 1
                                node_requested.hostname = nodestring
                                
                            # [:ppn=<ppn>][:gpus=<gpu>][:<property>[:<property>]...] [+ ...]
                            for prop_str in nodespec_a[1:]:
                                prop_a = prop_str.split("=", 1)
                                if len(prop_a) == 1:
                                    # <property>
                                    node_requested.properties.append(prop_a[0])
                                else:
                                    # [:ppn=<ppn>][:gpus=<gpu>]
                                    prop, val = prop_a
                                    if prop == "ppn":
                                        node_requested.ppn = val
                                    elif prop == "gpus":
                                        node_requested.gpus = val
                                    else:
                                        logging.warning("ignoring requested value for property %s" % prop)
                            
                            _nodes_requested.append(node_requested)
                else:
                    # Other resources are ignored (e.g. walltime)
                    # http://docs.adaptivecomputing.com/torque/4-1-3/Content/topics/2-jobs/requestingRes.htm
                    logging.debug("ignoring resource %s" % resource)
            else:
                logging.debug("ignoring request %s" % resource_request)
        
        return _nodes_requested
    
    def to_request(self):
        kws = ""
        if self.hostname:
            kws = "%shostname==\"%s\"" % (kws, self.hostname)
        if self.queue:
            if kws != "":
                kws = "%s && " % kws
            kws = "%s[\"%s\"] subset queues" % (kws, self.queue)
        if self.properties:
            comma_str = ""
            p_str = ""
            for p in self.properties:
                p_str = "%s%s\"%s\"" % (p_str, comma_str, p)
                comma_str = " , "

            if kws != "":
                kws = "%s && " % kws

            kws = "%s[%s] subset properties" % (kws, p_str)
        if self.ppn:
            # We are not checking whether the format is correct or not (numeric or not); that is a task of PBS
            if kws != "":
                kws = "%s && " % kws
            kws = "%sppn>=%s" % (kws, self.ppn)
        if self.gpus:
            # We are not checking whether the format is correct or not (numeric or not); that is a task of PBS
            if kws != "":
                kws = "%s && " % kws
            kws = "%sgpus>=%s" % (kws, self.gpus)
        return kws
    
    def __init__(self):
        self.ppn = None
        self.gpus = None
        self.properties = []
        self.nodecount = None
        self.hostname = None
        self.queue = None
        
    def __str__(self):
        if self.hostname is not None:
            ret_str = self.hostname
        elif self.nodecount is not None:
            ret_str = str(self.nodecount)
        if self.ppn is not None:
            ret_str = "%s:ppn=%s" % (ret_str, self.ppn)
        if self.gpus is not None:
            ret_str = "%s:gpus=%s" % (ret_str, self.gpus)
        for p in self.properties:
            ret_str = "%s:%s" % (ret_str, p)
        return ret_str

class _Node(XMLObject):
    values = [ 'name', 'state', 'ntype', 'status', 'np', 'properties', 'jobs' ]
    numeric = [ 'np' ]
    noneval = ""
    
    def to_nodeinfo(self):
        # Transformation of the properties string to a keyword dictionary that would be used for further 
        try:
            # TODO: it seems that creating a lexer or a yacc is very slow, so we should get rid of this piece of code. We could simply recognice the pairs key=value
            # _annie.clear_vars()
            # _annie.check("hostname=\"%s\";ppn=%d;%s" % (self.name, self.slots_count, self.keywords))
            # kw = _annie.get_vars()
            kw = cpyutils.evaluate.vars_from_string("hostname=\"%s\";ppn=%d;%s" % (self.name, self.slots_count, self.keywords))
        except:
            _LOGGER.error("an error happened evaluating host information: 'hostname=\"%s\";%s'" % (self.name, self.keywords))
            kw = {}
        
        ni = NodeInfo(self.name, self.slots_count, self.slots_free, self.memory_total, self.memory_free, kw)
        ni.state = self._infer_clues_state()
        return ni

    def _infer_clues_state(self):
        res_state = ""
        states = self.state.split(',')
        
        for state in states:
            state = state.strip()
            
            if state == 'free': res_state = NodeInfo.IDLE
            elif state == 'offline': res_state = NodeInfo.OFF
            elif state == 'down': res_state = NodeInfo.OFF
            elif state == 'job-exclusive' or state == 'busy' or state == 'reserve': res_state = NodeInfo.USED
            else: res_state = NodeInfo.OFF
            
            # Si ya estamos en estado down, no seguimos mirando
            if res_state == NodeInfo.OFF:
                break;
            
        if (res_state == NodeInfo.IDLE) and (self.slots_count > self.slots_free):
            res_state = NodeInfo.USED
        
        return res_state

    def __init__(self, xml_str, queues_properties):
        XMLObject.__init__(self, xml_str)
        
        if self.np == "" or self.np < 0:
            self.np = 1

        self.slots_count = self.np
        self.properties = self.properties.split(',')
        
        self.queues = []
        for prop in self.properties:
            if prop in queues_properties:
                self.queues += queues_properties[prop]
        
        # If there are queues that do not require properties, any node will be valid for the queue
        if "" in queues_properties:
            self.queues += queues_properties[""]
                
        keyw = self.status.split(',') # [ x for x in self.status.split(',') if x.strip() != "" ]
        self.status_keywords = {}
        # self.properties = {}
        self.vars_str = ""
        
        for kw in keyw:
            kv = kw.split('=', 1)
            if len(kv) == 2:
                k, v = kv
                self.status_keywords[k] = v
                if (v!="") and (re.match(r'^(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?(kb|gb|mb|tb|pb|Kb|Gb|Mb|Tb|Pb)?$', v) is None):
                    self.vars_str = "%s%s=\"%s\";" % (self.vars_str, k, v)
                else:
                    self.vars_str = "%s%s=%s;" % (self.vars_str, k, v)

        queues_str = ""
        if len(self.queues) > 0:
            queues_str = "\"%s\"" % "\",\"".join(self.queues)
        self.keywords = "%squeues=[%s];" % (self.vars_str, queues_str)

        # TODO: check this code
        if len(self.properties) > 0:
            self.keywords = "%sproperties=[\"%s\"];" % (self.keywords, "\",\"".join(self.properties))
        else:
            self.keywords = "%sproperties=[];" % self.keywords
    
        n_jobs = 0
        if self.jobs != "":
            jobs = self.jobs.split(',')
            n_jobs = len(jobs)
            
        # TODO: can be removed... just uses part of the original code structure
        self.np = self.np - n_jobs
        self.slots_free = self.np
        
        # Try to infer other values
        self.memory_free = -1
        self.memory_total = -1
        
        if 'totmem' in self.status_keywords:
            self.memory_total = (_translate_mem_value(self.status_keywords['totmem']))
            
        if 'availmem' in self.status_keywords:
            self.memory_free = (_translate_mem_value(self.status_keywords['availmem']))
            
            if self.memory_total == -1:
                self.memory_total = self.memory_free
        else:
            self.memory_free = self.memory_total

    def __str__(self):
        return "<name>%s</name><np>%d</np><ntype>%s</ntype><status>%s</status><properties>%s</properties><keywords>%s</keywords>" % (self.name, self.np, self.ntype, self.status, ",".join(self.properties), self.keywords)

class _PBSNodes(XMLObject):
    tuples_lists = {'Node': _Node}

    def __init__(self, xml_str, queues_properties):
        XMLObject.__init__(self, xml_str, [queues_properties])

class _Job(XMLObject):
    values = [ 'Job_Id', 'job_state', 'nodes', 'queue', 'ctime', 'start_time' ]
    noneval = ""
    
    def __init__(self, xml_str):
        XMLObject.__init__(self, xml_str)
        if not self.nodes:
            self.nodes = "1"
    
    def __str__(self):
        return "%s %s" % (self.Job_Id, self.nodes)
       
    def to_jobinfo(self):

        nodespec = []
        if self.nodes:
            nodespec = _Nodespec.createFromOptions("nodes=" + self.nodes)
        else:
            ns = _Nodespec()
            ns.nodecount=1
            nodespec.queue = self.queue
            nodespec.append(ns)
        
        req_str = []
        for ns in nodespec:
            req_str.append(ns.to_request())
        if not req_str:
            req_str.append("")
        resources = clueslib.request.ResourcesNeeded(ns.nodecount, 0, req_str, taskcount = 1)
        ji = JobInfo(resources, self.Job_Id, [])
        if self.job_state == "Q":
            ji.set_state(Request.PENDING)
        elif self.job_state in ["R","E","C","T"]:
            ji.set_state(Request.SERVED)
        elif self.job_state in ["H", "W"]:
            ji.set_state(Request.BLOCKED)
        else:
            _LOGGER.error("The PBS job state '%s' is unknown" % self.job_state)
            
        return ji

class _Jobs(XMLObject):
    tuples_lists = {'Job': _Job}
        
class lrms(clueslib.platform.LRMS):
    def _get_queues_properties(self):
        # Changed with respect to CLUES 1, because it seems to be less prone to variations
        command = self._qstat + [ '-Qf', "@%s" % self._server_ip ]
        success, out_command = cpyutils.runcommand.runcommand(command, False, timeout = clueslib.configlib._CONFIGURATION_GENERAL.TIMEOUT_COMMANDS)
        if not success:
            _LOGGER.error("could not get information about the queues: %s" % out_command)
            return None
            
        lines = out_command.split('\n')
        
        properties = {}
        queue = None
        properties_count = 0
        for line in lines:
            line = line.strip()
            if line.find('Queue: ') != -1:
                if queue is not None:
                    if properties_count == 0:
                        # The queue did not requested any property, so any node is valid for this queue
                        if "" not in properties:
                            properties[""] = []
                        properties[""].append(queue)
                
                words = line.split(' ', 1)
                queue = words[1].strip()
                properties_count = 0
                
            elif line.find('resources_default.neednodes') != -1:
                words = line.split('=', 1)
                prop = words[1].strip()
                
                if prop not in properties:
                    properties[prop] = []
                    
                if queue is not None:
                    properties[prop].append(queue)
                    
                properties_count+=1

        # When there are no more lines, we must check if the last queue requested no properties
        if queue is not None:
            if properties_count == 0:
                # The queue did not requested any property, so any node is valid for this queue
                if "" not in properties:
                    properties[""] = []
                properties[""].append(queue)
                    
        return properties

    def __init__(self, PBS_SERVER = None, PBS_QSTAT_COMMAND = None, PBS_PBSNODES_COMMAND = None): # PBS_PATH = None):
        #
        # NOTE: This fragment provides the support for global config files. It is a bit awful.
        #       I do not like it because it is like having global vars. But it is managed in
        #       this way for the sake of using configuration files
        #
        import cpyutils.config
        config_pbs = cpyutils.config.Configuration(
            "PBS",
            {
                "PBS_SERVER": "localhost", 
                "PBS_QSTAT_COMMAND": "/usr/bin/qstat",
                "PBS_PBSNODES_COMMAND": "/usr/bin/pbsnodes"
            }
        )
        
        self._server_ip = clueslib.helpers.val_default(PBS_SERVER, config_pbs.PBS_SERVER)
        _qstat_cmd = clueslib.helpers.val_default(PBS_QSTAT_COMMAND, config_pbs.PBS_QSTAT_COMMAND)
        self._qstat = _qstat_cmd.split(" ")
        _pbsnodes_cmd = clueslib.helpers.val_default(PBS_PBSNODES_COMMAND, config_pbs.PBS_PBSNODES_COMMAND)
        self._pbsnodes = _pbsnodes_cmd.split(" ")
        clueslib.platform.LRMS.__init__(self, "PBS_%s" % self._server_ip)
        
    def get_nodeinfolist(self):
        queues_properties = self._get_queues_properties()

        if queues_properties is None:
            _LOGGER.error("could not obtain information about the queues from PBS server %s (check if it is online and the user running CLUES is allowed to poll the queues)" % self._server_ip)
            return None
        
        command = self._pbsnodes + [ '-x', '-s', self._server_ip ]
        success, out_command = cpyutils.runcommand.runcommand(command, False, timeout = clueslib.configlib._CONFIGURATION_GENERAL.TIMEOUT_COMMANDS)

        if not success:
            _LOGGER.error("could not obtain information about PBS server %s (%s)" % (self._server_ip, out_command))
            return None
            
        hosts = _PBSNodes(out_command, queues_properties)
        nodeinfolist = collections.OrderedDict()
        
        if hosts is not None:
            for host in hosts.Node:
                nodeinfolist[host.name] = host.to_nodeinfo()
        else:
            _LOGGER.warning("an error occurred when monitoring hosts (could not get information from PBS; please check PBS_SERVER and PBS_PBSNODES_COMMAND vars)")
                
        return nodeinfolist
    
    def get_jobinfolist(self):
        command = self._qstat + [ '-x', '@%s' % self._server_ip ]
        success, out_command = cpyutils.runcommand.runcommand(command, False, timeout = clueslib.configlib._CONFIGURATION_GENERAL.TIMEOUT_COMMANDS)

        if not success:
            _LOGGER.error("could not obtain information about PBS server %s (%s)" % (self._server_ip, out_command))
            return None
        
        jobinfolist = []
        if out_command.strip():
            jobs = _Jobs(out_command)
            
            if jobs is not None:
                for job in jobs.Job:
                    # Do not return running or finished jobs
                    job_info = job.to_jobinfo()
                    if job_info.state != Request.SERVED:
                        jobinfolist.append(job_info)
            else:
                _LOGGER.warning("an error occurred when monitoring hosts (could not get information from PBS; please check PBS_SERVER and PBS_QSTAT_COMMAND vars)")

        return jobinfolist

if __name__ == '__main__':
    pass
