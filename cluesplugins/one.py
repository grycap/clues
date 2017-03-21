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
import cpyutils.eventloop
import cpyutils.oneconnect
import cpyutils.config
import clueslib.node
import clueslib.helpers
import clueslib.request
from clueslib.platform import PowerManager_with_IPs
from clueslib.node import Node, NodeInfo, NodeList
import collections

import cpyutils.log
_LOGGER = cpyutils.log.Log("PLUGIN-ONE")

class powermanager(PowerManager_with_IPs):
    class VM_Node:
        def __init__(self, vm_id, nname, ip):
            self.vm_id = vm_id
            self.nname = nname
            self.ip = ip
            self.timestamp_monitoring = 0
            self.timestamp_seen = cpyutils.eventloop.now()
            self.timestamp_recovered = 0
            
        def monitored(self):
            self.timestamp_monitoring = cpyutils.eventloop.now()
            # _LOGGER.debug("monitored %s" % self.vm_id)
            
        def seen(self):
            self.timestamp_seen = cpyutils.eventloop.now()
            # _LOGGER.debug("seen %s" % self.vm_id)

        def recovered(self):
            self.timestamp_recovered = cpyutils.eventloop.now()

    def __init__(self, ONE_VIRTUAL_CLUSTER_XMLRPC = None, ONE_VIRTUAL_CLUSTER_AUTH = None, ONE_VIRTUAL_CLUSTER_XMLRPC_TIMEOUT = None, ONE_VIRTUAL_CLUSTER_HOSTS_FILE = None, ONE_VIRTUAL_CLUSTER_TEMPLATE_ID = None, ONE_VIRTUAL_CLUSTER_DROP_FAILING_VMS = None, ONE_VIRTUAL_CLUSTER_FORGET_MISSING_VMS = None, ONE_VIRTUAL_CLUSTER_GUESS_VMS_TIME = None, ONE_VIRTUAL_CLUSTER_TEMPLATE = None):
        #
        # NOTE: This fragment provides the support for global config files. It is a bit awful.
        #       I do not like it because it is like having global vars. But it is managed in
        #       this way for the sake of using configuration files
        #
        config_one = cpyutils.config.Configuration(
            "ONE VIRTUAL CLUSTER",
            {
                "ONE_VIRTUAL_CLUSTER_XMLRPC": "http://localhost:2633/RPC2",
                "ONE_VIRTUAL_CLUSTER_AUTH": "clues:cluespass",
                "ONE_VIRTUAL_CLUSTER_XMLRPC_TIMEOUT": 180,
                "ONE_VIRTUAL_CLUSTER_HOSTS_FILE": "virtualonecluster.hosts",
                "ONE_VIRTUAL_CLUSTER_TEMPLATE_ID": 1,
                "ONE_VIRTUAL_CLUSTER_DROP_FAILING_VMS": 120,
                "ONE_VIRTUAL_CLUSTER_FORGET_MISSING_VMS": 30,
                "ONE_VIRTUAL_CLUSTER_GUESS_VMS_TIME": 30,
                "ONE_VIRTUAL_CLUSTER_TEMPLATE":""
            }
        )

        self._ONE_VIRTUAL_CLUSTER_XMLRPC = clueslib.helpers.val_default(ONE_VIRTUAL_CLUSTER_XMLRPC, config_one.ONE_VIRTUAL_CLUSTER_XMLRPC)
        self._ONE_VIRTUAL_CLUSTER_AUTH = clueslib.helpers.val_default(ONE_VIRTUAL_CLUSTER_AUTH, config_one.ONE_VIRTUAL_CLUSTER_AUTH)
        self._ONE_VIRTUAL_CLUSTER_XMLRPC_TIMEOUT = clueslib.helpers.val_default(ONE_VIRTUAL_CLUSTER_XMLRPC_TIMEOUT, config_one.ONE_VIRTUAL_CLUSTER_XMLRPC_TIMEOUT)
        self._ONE_VIRTUAL_CLUSTER_HOSTS_FILE = clueslib.helpers.val_default(ONE_VIRTUAL_CLUSTER_HOSTS_FILE, config_one.ONE_VIRTUAL_CLUSTER_HOSTS_FILE)
        self._ONE_VIRTUAL_CLUSTER_TEMPLATE_ID = clueslib.helpers.val_default(ONE_VIRTUAL_CLUSTER_TEMPLATE_ID, config_one.ONE_VIRTUAL_CLUSTER_TEMPLATE_ID)
        self._ONE_VIRTUAL_CLUSTER_DROP_FAILING_VMS = clueslib.helpers.val_default(ONE_VIRTUAL_CLUSTER_DROP_FAILING_VMS, config_one.ONE_VIRTUAL_CLUSTER_DROP_FAILING_VMS)
        self._ONE_VIRTUAL_CLUSTER_FORGET_MISSING_VMS = clueslib.helpers.val_default(ONE_VIRTUAL_CLUSTER_FORGET_MISSING_VMS, config_one.ONE_VIRTUAL_CLUSTER_FORGET_MISSING_VMS)
        self._ONE_VIRTUAL_CLUSTER_GUESS_VMS_TIME = clueslib.helpers.val_default(ONE_VIRTUAL_CLUSTER_GUESS_VMS_TIME, config_one.ONE_VIRTUAL_CLUSTER_GUESS_VMS_TIME)
        self._ONE_VIRTUAL_CLUSTER_TEMPLATE = clueslib.helpers.val_default(ONE_VIRTUAL_CLUSTER_TEMPLATE, config_one.ONE_VIRTUAL_CLUSTER_TEMPLATE)

        self._load_ip_translate_structures(self._ONE_VIRTUAL_CLUSTER_HOSTS_FILE)
        if (self._nname_2_ip is None) or (self._ip_2_nname is None):
            _LOGGER.error("could not load the virtual cluster file for ONE (%s)... exiting" % self._ONE_VIRTUAL_CLUSTER_HOSTS_FILE)
            import sys
            sys.exit(-1)
        
        self._one = cpyutils.oneconnect.ONEConnect(self._ONE_VIRTUAL_CLUSTER_XMLRPC, self._ONE_VIRTUAL_CLUSTER_AUTH, timeout = self._ONE_VIRTUAL_CLUSTER_XMLRPC_TIMEOUT)
        
        # Structure for the recovery of nodes
        self._mvs_seen = {}
        
        if self._ONE_VIRTUAL_CLUSTER_FORGET_MISSING_VMS >= self._ONE_VIRTUAL_CLUSTER_DROP_FAILING_VMS:
            self._ONE_VIRTUAL_CLUSTER_FORGET_MISSING_VMS = self._ONE_VIRTUAL_CLUSTER_DROP_FAILING_VMS + 1
            _LOGGER.info("correcting values of configuration for ONE (we must forget vms that may have been powered off before trying to recover vms that accidentally failed)")

        self._timestamp_guess = cpyutils.eventloop.now() - self._ONE_VIRTUAL_CLUSTER_GUESS_VMS_TIME
        
    def _guess_vms(self):
        now = cpyutils.eventloop.now()

        if (now - self._timestamp_guess) >= self._ONE_VIRTUAL_CLUSTER_GUESS_VMS_TIME:
            self._timestamp_guess = now
            vms = self._one.get_vms()
            
            if vms is not None:
                running_ips = []
            
                # First: let's see which nodes are already running, and take note of those that we have not powered on
                for vm in vms:
                    if vm.STATE in [ cpyutils.oneconnect.VM.STATE_ACTIVE, cpyutils.oneconnect.VM.STATE_PENDING, cpyutils.oneconnect.VM.STATE_INIT ]:
                        ips = vm.get_ips()
                        for ip in ips:
                            if ip in self._ip_2_nname:
                                nname = self._ip_2_nname[ip]
                                if nname not in self._mvs_seen:
                                    self._mvs_seen[nname] = self.VM_Node(vm.ID, nname, ip)
                                    _LOGGER.info("guessing that running VM %s is node %s" % (vm.ID, nname))
                                self._mvs_seen[nname].seen()
    
                # Second: from the nodes that we have powered on, check which of them are still running
                for nname, node in self._mvs_seen.items():
                    if (now - node.timestamp_seen) > self._ONE_VIRTUAL_CLUSTER_FORGET_MISSING_VMS:
                        _LOGGER.debug("vm %s is not seen for a while... let's forget it" % nname)
                        del self._mvs_seen[nname]

    def lifecycle(self):
        self._guess_vms()

        monitoring_info = self._clues_daemon.get_monitoring_info()
        now = cpyutils.eventloop.now()

        # Two cases: (1) a VM that is on in the monitoring info, but it is not seen in ONE; and (2) a VM that is off in the monitoring info, but it is seen in ONE
        recover = []

        for node in monitoring_info.nodelist:
            if node.enabled:
                if node.state in [clueslib.node.Node.OFF, clueslib.node.Node.OFF_ERR]:
                    if self._ONE_VIRTUAL_CLUSTER_DROP_FAILING_VMS > 0:
                        if node.name in self._mvs_seen:
                            vm = self._mvs_seen[node.name]
                            if ((now - vm.timestamp_recovered) > self._ONE_VIRTUAL_CLUSTER_DROP_FAILING_VMS) and ((now - vm.timestamp_monitoring) > self._ONE_VIRTUAL_CLUSTER_DROP_FAILING_VMS):
                                _LOGGER.warning("node %s has a VM running but it is not detected by the monitoring system... will recover it (state: %s)" % (node.name, node.state))
                                vm.recovered()
                                recover.append(node.name)
                else:
                    if node.name not in self._mvs_seen:
                        # This may happen because it is launched by hand using other credentials than those for the user used for ONE (and he cannot manage the VMS)
                        _LOGGER.warning("node %s is detected by the monitoring system, but there is not any VM associated to it (are ONE credentials compatible to the VM?)" % node.name)
                    else:
                        self._mvs_seen[node.name].monitored()                        

        self._recover_ids(recover)
        return PowerManager_with_IPs.lifecycle(self)

    def _recover_ids(self, vms):
        for vm in vms:
            self.power_off(vm)

    def power_on(self, nname):
        if nname not in self._nname_2_ip:
            _LOGGER.error("could not power on node because its IP address is unknown")
            return False, None
        
        if nname in self._mvs_seen:
            running_vm = self._mvs_seen[nname]
            _LOGGER.warning("tried to power on node %s, while there is a VM (%d) that is supposed to be already powered on with that IP" % (nname, running_vm.vm_id))

            # We are failing, because we are calling for specific IPs
            return False, None

        ip = self._nname_2_ip[nname]
        current_template = self._ONE_VIRTUAL_CLUSTER_TEMPLATE
        current_template = current_template.replace("%%h", nname)
        current_template = current_template.replace("%%a", ip)
        
        success, result = self._one.create_vm_by_template(current_template)
        if not success:
            _LOGGER.error("could not power on the virtual node (ONE error: %s)" % result) 
            return False, None
        else:
            ips = result.get_ips()
            for ip in ips:
                if ip in self._ip_2_nname:
                    nname = self._ip_2_nname[ip]

                    # We discard the previous info, because this is real... the other is guessed or historical                    
                    self._mvs_seen[nname] = self.VM_Node(result.ID, nname, ip)
                    return True, nname
            
            _LOGGER.error("None of the IPs from the just created VM is registered in the virtual cluster")
            return True, nname
    
    def power_off(self, nname, force = False):
        if nname not in self._nname_2_ip:
            _LOGGER.error("could not power off node because its IP address is unknown")
            return False, None
        
        ip = self._nname_2_ip[nname]
        vms = self._one.get_vms()
        
        vm_id = None
        for vm in vms:
            if ip in vm.get_ips():
                vm_id = int(vm.ID)
                break
        if vm_id is None:
            _LOGGER.error("None of the running VMs has the IP that corresponds to node %s (%s)" % (nname, ip))
            return False, None
        
        # TODO: shutdown helps to detect the glitch (pbs_mom informs pbsnodes)
        if force:
            success = self._one.vm_delete(vm_id)
        else:
            success = self._one.vm_shutdown(vm_id)
            
        if success:
            if nname not in self._mvs_seen:
                _LOGGER.warning("tried to power off node %s, but it is supposed that is not any running VM for that IP" % (nname))
                self._mvs_seen[nname] = self.VM_Node(vm_id, nname, ip)
                
            return True, nname
        else:
            _LOGGER.error("an error happened when trying to power off node %s" % nname)
            return False, None
        
    def recover(self, nname):
        success, nname = self.power_off(nname, True)
        if success:
            return Node.OFF
        
        return None

class powermanager_template(powermanager):
    def power_on(self, nname):
        if nname not in self._nname_2_ip:
            _LOGGER.error("could not power on node because its IP address is unknown")
            return False, None
        
        if nname in self._mvs_seen:
            running_vm = self._mvs_seen[nname]
            _LOGGER.warning("tried to power on node %s, while there is a VM (%d) that is supposed to be already powered on with that IP" % (nname, running_vm.vm_id))

            # We are failing, because we are calling for specific IPs
            return False, None

        ip = self._nname_2_ip[nname]
        current_template = self._ONE_VIRTUAL_CLUSTER_TEMPLATE
        current_template = current_template.replace("%%h", nname)
        current_template = current_template.replace("%%a", ip)
        
        success, result = self._one.create_vm_by_template(current_template)
        if not success:
            _LOGGER.error("could not power on the virtual node (ONE error: %s)" % result) 
            return False, None
        else:
            ips = result.get_ips()
            for ip in ips:
                if ip in self._ip_2_nname:
                    nname = self._ip_2_nname[ip]

                    # We discard the previous info, because this is real... the other is guessed or historical                    
                    self._mvs_seen[nname] = self.VM_Node(result.ID, nname, ip)
                    return True, nname
            
            _LOGGER.error("None of the IPs from the just created VM is registered in the virtual cluster")
            return True, nname

class lrms_general(clueslib.platform.LRMS):
    @staticmethod
    def _host_to_nodeinfo(h):
        if (h.state != 'free') and (h.total_slots == 0):
            h.total_slots = -1
        if (h.state != 'free') and (h.memory_total== 0):
            h.memory_total = -1
        
        ni = NodeInfo(h.NAME, h.total_slots, h.free_slots, h.memory_total, h.memory_free, h.keywords)
        if h.state in [ 'free' ]:
            if h.free_slots == h.total_slots:
                ni.state = Node.IDLE
            else:
                ni.state = Node.USED
        if h.state in [ 'busy' ]:
            ni.state = Node.USED
        if h.state in [ 'down', 'error' ]:
            ni.state = Node.OFF
        return ni

    def __init__(self, ONE_XMLRPC, ONE_AUTH, ONE_XMLRPC_TIMEOUT):
        self._one = cpyutils.oneconnect.ONEConnect(ONE_XMLRPC, ONE_AUTH, ONE_XMLRPC_TIMEOUT)
        clueslib.platform.LRMS.__init__(self, "ONE_%s" % ONE_XMLRPC)
        
    def _find_host(self, nname):
        hosts = self._one.get_hosts()
        host_id = None
        for host in hosts:
            if host.NAME == nname:
                host_id = int(host.ID)
        return host_id

    '''        
    def power_on(self, nname):
        host_id = self._find_host(nname)
        if host_id is None:
            _LOGGER.warning("trying to power on host %s, but it is unkonwn" % nname)
            return False
        return self._one.host_enable(host_id)
        
    def power_off(self, nname):
        host_id = self._find_host(nname)
        if host_id is None:
            _LOGGER.warning("trying to power on host %s, but it is unkonwn" % nname)
            return False
        # TODO: this action disables the host at ONE level and is confident on the power-off method. It supposes that it will be powered off
        #   anyway, if it is powered on again and the node could not physically be powered-off, ONE will monitor it again and will continue
        #   working
        return self._one.host_disable(host_id)
    '''

    def get_nodeinfolist(self):
        hosts = self._one.get_hosts()
        nodeinfolist = collections.OrderedDict()
        if hosts is not None:
            for host in hosts:
                n_info = self._host_to_nodeinfo(host)
                nodeinfolist[n_info.name] = n_info
        else:
            _LOGGER.warning("an error occurred when monitoring hosts (could not get information from ONE; please check ONE_XMLRPC and ONE_AUTH vars)")
            return None
                
        return nodeinfolist
    
    def get_jobinfolist(self):
        vms = self._one.get_vms()
        jobinfolist = []
        
        if vms is not None:
            for vm in vms:
                current_host = None
                if vm.HISTORY_RECORDS is not None:
                    if len(vm.HISTORY_RECORDS.HISTORY) > 0:
                        current_host = vm.HISTORY_RECORDS.HISTORY[-1].HOSTNAME
                
                resources = clueslib.request.ResourcesNeeded(vm.TEMPLATE.CPU * 100, vm.TEMPLATE.MEMORY * 1024)
                nodes = []
                if current_host is not None:
                    nodes.append(current_host)
                    
                # job = clueslib.request.Request(resources, vm.ID, nodes)
                job = clueslib.request.JobInfo(resources, vm.ID, nodes)
                
                if vm.STATE in [ vm.STATE_INIT, vm.STATE_PENDING, vm.STATE_HOLD ]:
                    job.set_state(clueslib.request.Request.PENDING)
                else:
                    job.set_state(clueslib.request.Request.ATTENDED)
                jobinfolist.append(job)

        return jobinfolist
    
class lrms(lrms_general):
    def __init__(self):
        #
        # NOTE: this is a bit awful. I do not like it because it is like having global vars. But it is managed in this way for the sake of using configuration files
        #
        config_one = cpyutils.config.Configuration(
            "ONE LRMS",
            {
                "ONE_XMLRPC": "http://localhost:2633/RPC2", 
                "ONE_AUTH" : "clues:cluespass",
                "ONE_XMLRPC_TIMEOUT" : 180
            }
        )
        lrms_general.__init__(self, config_one.ONE_XMLRPC, config_one.ONE_AUTH, config_one.ONE_XMLRPC_TIMEOUT)
