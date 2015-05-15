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

_LOGGER=logging.getLogger("[IPMI]")

try:
    config_ipmi
except:
    config_ipmi = cpyutils.config.Configuration(
        "IPMI",
        {
            "IPMI_CMDLINE_POWON": "", 
            "IPMI_CMDLINE_POWOFF" : "",
            "IPMI_HOSTS_FILE" : "ipmi.hosts"
        }
    )

class powermanager(PowerManager_cmdline):
    def __init__(self):
        PowerManager_cmdline.__init__(self, config_ipmi.IPMI_CMDLINE_POWON, config_ipmi.IPMI_CMDLINE_POWOFF, config_ipmi.IPMI_HOSTS_FILE)

        if (self._nname_2_ip is None) or (self._ip_2_nname is None):
            _LOGGER.error("could not load the IP addresses file for IPMI (%s)... exiting" % config_ipmi.IPMI_HOSTS_FILE)
            sys.exit(-1)
