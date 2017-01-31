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
import sys
from clueslib.platform import PowerManager_cmdline
import cpyutils.config
import clueslib.configlib as configlib


_LOGGER=logging.getLogger("[PMCOMMANDLINE]")

try:
    config_commandline
except:
    config_commandline = cpyutils.config.Configuration(
        "COMMANDLINE",
        {
            "COMMANDLINE_CMDLINE_POWON": "", 
            "COMMANDLINE_CMDLINE_POWOFF" : "",
            "COMMANDLINE_HOSTS_FILE" : "commandline.hosts"
        }
    )

class powermanager(PowerManager_cmdline):
    def __init__(self):
        PowerManager_cmdline.__init__(self, config_commandline.COMMANDLINE_CMDLINE_POWON, config_commandline.COMMANDLINE_CMDLINE_POWOFF, config_commandline.COMMANDLINE_HOSTS_FILE)

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
            if len(cout.split(' ')) == 1:
                # Assume that the command returned the name of the host
                return True, cout
            else:
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
            if len(cout.split(' ')) == 1:
                # Assume that the command returned the name of the host
                return True, cout
            else:
                return True, nname
        else:
            _LOGGER.error("an error happened when trying to power off node %s" % nname)
            return False, None