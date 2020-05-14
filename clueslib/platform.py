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
import subprocess
from . import node
from . import helpers
import cpyutils.runcommand
from . import configlib
import collections

import cpyutils.log
_LOGGER = cpyutils.log.Log("PLATFORM")

class Platform(object):
    def __init__(self, lrms, power_manager):
        self._lrms = lrms
        self._pow_mgr = power_manager
        self._clues_daemon = None
        self._pow_off_msg = False
        self._pow_on_msg = False

    # Associates a clues daemon to the platform in order to be able to interact with ir
    def attach_clues_system(self, clues_daemon):
        self._clues_daemon = clues_daemon
        self._lrms._attach_clues_system(clues_daemon)
        if self._pow_mgr is not None:
            self._pow_mgr._attach_clues_system(clues_daemon)
        
    # Gets the list of nodes in the platform (mainly for monitoring purposes)
    def get_nodeinfolist(self):
        return self._lrms.get_nodeinfolist()

    # Gets the list of jobs in the platform (mainly for monitoring purposes)
    def get_jobinfolist(self):
        return self._lrms.get_jobinfolist()
    
    # Powers off a node in an ordered mode (calls the poweroff of the lrms and the poweroff of the powermanager)
    def power_off(self, nname):
        if self._pow_mgr is None:
            if not self._pow_off_msg:
                _LOGGER.debug("wanted to power off node %s but there is not any power manager stablished for the platform" % nname)
            # self._pow_off_msg = True
            return False, nname
        
        result, real_nname = self._pow_mgr.power_off(nname)
        if result:
            result = self._lrms.power_off(real_nname)
        return result, real_nname
    
    # Powers on a node in an ordered mode (calls the poweron of the lrms and the poweron of the powermanager)
    def power_on(self, nname):
        if self._pow_mgr is None:
            if not self._pow_on_msg:
                _LOGGER.debug("wanted to power on node %s but there is not any power manager stablished for the platform" % nname)
            # self._pow_on_msg = True
            return False, nname

        result, real_nname = self._pow_mgr.power_on(nname)
        if result:
            result = self._lrms.power_on(real_nname)
        return result, real_nname
    
    # Carries out the lifecycle of the platform (in case that it has any). The default implementation calls the lifecycles of the lrms and the powermanager
    # e.g. the lifecycle for the lrms will be useful in a simulator that should assign jobs, etc. or in the powermanager of VMs to detect stalled vms
    def lifecycle(self):
        if self._pow_mgr is None:
            return False
        
        return self._lrms.lifecycle() and self._pow_mgr.lifecycle()

    # Recovers the state of a node. The default implementation calls the recover of the powermanager
    # e.g. a powermanager can cut the power of a node and power on again to try to recover the node, or may delete a VM and launch it again
    #
    # Returns the state of the node Node.ON or Node.OFF
    def recover(self, nname):
        if self._pow_mgr is None:
            return False
        
        return self._pow_mgr.recover(nname)

class LRMS:
    def __init__(self, _id):
        self._clues_daemon = None
        self._id = _id
    def get_id(self):
        return self._id
    def get_nodeinfolist(self):
        node_list = collections.OrderedDict()
        return node_list
    def _attach_clues_system(self, clues_daemon):
        self._clues_daemon = clues_daemon
    def get_jobinfolist(self):
        return []    
    def power_off(self, nname):
        return True
    def power_on(self, nname):
        return True
    def lifecycle(self):
        return True

class PowerManager:
    def __init__(self):
        pass
    def _attach_clues_system(self, clues_daemon):
        self._clues_daemon = clues_daemon
    def power_on(self, nname):
        return False, nname
    def power_off(self, nname):
        return False, nname
    def lifecycle(self):
        return True
    def recover(self, nname):
        return False

class PowerManager_with_IPs(PowerManager):
    def _load_ip_translate_structures(self, filename):
        import cpyutils.config
        try:
            self._nname_2_ip
            self._ip_2_nname
        except:
            self._nname_2_ip, self._ip_2_nname = helpers.read_hosts_file(cpyutils.config.config_filename(filename))

class Node_cmdline(node.Node):
    MANDATORY_KW = ['clues_state', 'clues_total_slots', 'clues_name', 'clues_free_slots' ]
    OPTIONAL_KW = {'clues_memory_free':-1, 'clues_memory_total':-1 }
    
    @staticmethod
    def _ensure_kw(kw):
        for k in Node_cmdline.MANDATORY_KW:
            if k not in kw:
                return False
        for k,v in Node_cmdline.OPTIONAL_KW.items():
            if k not in kw:
                kw[k]=v
        return True
    
    @staticmethod
    def create_from_kw(kw):
        if not Node_cmdline._ensure_kw(kw): return None
        n = Node_cmdline(kw['clues_name'], float(kw['clues_total_slots']), float(kw['clues_free_slots']), float(kw['clues_memory_total']), float(kw['clues_memory_free']))
        if kw['clues_state'] == "free":
            n.state = Node_cmdline.IDLE
        elif kw['clues_state'] == "busy":
            n.state = Node_cmdline.USED
        elif kw['clues_state'] == "down":
            n.state = Node_cmdline.OFF
        elif kw['clues_state'] == "err":
            n.state = Node_cmdline.UNKNOWN
        else:
            n.state = Node_cmdline.UNKNOWN
            
        extra = {}
        for kw_id, value in kw.items():
            extra[kw_id] = value
        n.keywords = extra
        return n
    
    def __init__(self, name, slots_count, slots_free, memory_total, memory_free, keywords = {}):
        node.Node.__init__(self, name, slots_count, slots_free, memory_total, memory_free, keywords)
    
'''
class LRMS_cmdline(LRMS):
    def __init__(self, CMD_LINE_NODEINFOLIST, CMD_LINE_JOBINFOLIST):
        LRMS.__init__(self)
        self._cmd_nodeinfolist = CMD_LINE_NODEINFOLIST
        self._cmd_jobinfolist = CMD_LINE_JOBINFOLIST

    def get_nodeinfolist(self):
        node_list = {}
        
        result, output = cpyutils.runcommand.runcommand(self._cmd_nodeinfolist, timeout = configlib._CONFIGURATION_GENERAL.TIMEOUT_COMMANDS)
        if not result:
            return None
        
        ev = expeval.Eval()

        for nodeline in output.split("\n"):
            if nodeline.strip() != "":
                kws = ev.evaluate(nodeline)
                node = Node_cmdline.create_from_kw(kws)
                if node is None:
                    _LOGGER.error("could not obtain the information about one host")
                else:
                    if node.name in node_list:
                        _LOGGER.warning("the node with name %s is duplicated" % node.name)
                    node_list[node.name] = node
        
        return node_list

class PowerManager_cmdline(PowerManager):
    def __init__(self, CMD_LINE_POWERON, CMD_LINE_POWEROFF):
        PowerManager.__init__(self)
        self._cmd_poweroff = CMD_LINE_POWEROFF
        self._cmd_poweron = CMD_LINE_POWERON

    def power_off(self, nname):
        result, output = cpyutils.runcommand.runcommand("%s %s" % (self._cmd_poweroff, nname), timeout = configlib._CONFIGURATION_GENERAL.TIMEOUT_COMMANDS)
        return result, nname
    
    def power_on(self, nname):
        result, output = cpyutils.runcommand.runcommand("%s %s" % (self._cmd_poweron, nname), timeout = configlib._CONFIGURATION_GENERAL.TIMEOUT_COMMANDS)
        return result, nname
'''

class PowerManager_cmdline(PowerManager_with_IPs):
    def __init__(self, CMD_POWON, CMD_POWOFF, ips_file = None):
        self._have_ips = False
        if ips_file is not None:
            self._load_ip_translate_structures(ips_file)
            self._have_ips = True
            
        self._CMD_POWON = CMD_POWON
        self._CMD_POWOFF = CMD_POWOFF
        
    def power_on(self, nname):
        if self._have_ips:
            if nname not in self._nname_2_ip:
                _LOGGER.error("could not power on node because its IP address is unknown")
                return False, None
            
            ip = self._nname_2_ip[nname]
        else:
            ip = nname

        _ACTUAL_CMD_POWON = self._CMD_POWON.replace("%%a", ip)
        _ACTUAL_CMD_POWON = _ACTUAL_CMD_POWON.replace("%%h", nname)

        success, cout = cpyutils.runcommand.runcommand(_ACTUAL_CMD_POWON, True, timeout = configlib._CONFIGURATION_GENERAL.TIMEOUT_COMMANDS)
        if success:
            return True, nname
        else:
            _LOGGER.error("an error happened when trying to power on node %s" % nname)
            return False, None

    def power_off(self, nname):
        if self._have_ips:
            if nname not in self._nname_2_ip:
                _LOGGER.error("could not power off node because its IP address is unknown")
                return False, None
            
            ip = self._nname_2_ip[nname]
        else:
            ip = nname
            
        _ACTUAL_CMD_POWOFF = self._CMD_POWOFF.replace("%%a", ip)
        _ACTUAL_CMD_POWOFF = _ACTUAL_CMD_POWOFF.replace("%%h", nname)
        
        success, cout = cpyutils.runcommand.runcommand(_ACTUAL_CMD_POWOFF, True, timeout = configlib._CONFIGURATION_GENERAL.TIMEOUT_COMMANDS)
        if success:
            return True, nname
        else:
            _LOGGER.error("an error happened when trying to power off node %s" % nname)
            return False, None