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

try:
    _CONFIGURATION_CLUES
except:
    _CONFIGURATION_CLUES = cpyutils.config.Configuration("general",
        {
            "LOGGER_MARK": 1800,
            "DB_CONNECTION_STRING": "sqlite:///var/lib/clues2/clues.db",
            "DISABLED_HOSTS": "",
            "RETRIEVE_NODES_FROM_DB_ON_STARTUP": True
        })

try:
    _CONFIGURATION_MONITORING
except:
    _CONFIGURATION_MONITORING = cpyutils.config.Configuration("monitoring",
        {
            "MAX_WAIT_POWERON": 300,
            "MAX_WAIT_POWEROFF": 300,
            "PERIOD_MONITORING_NODES": 5,
            "PERIOD_MONITORING_JOBS": 0,                    # Deactivated by default
            "PERIOD_LIFECYCLE": 5,
            "PERIOD_MONITORING_NODES_FAIL_GRACE": 120,
            "PERIOD_MONITORING_JOBS_FAIL_GRACE": 120,
            "NEGATIVE_RESOURCES_MEANS_INFINITE": True,
            "DELAY_POWON": 10,
            "DELAY_POWOFF": 10,
            # "COOLDOWN_DISSAPEARED_JOBS": 120,
            "COOLDOWN_SERVED_JOBS": 120,
            "COOLDOWN_SERVED_REQUESTS": 120,
        })

class ConfigGeneral(cpyutils.config.Configuration):
    def parseconfig(self):
        import logging
        if self.LOG_FILE == "":
            self.LOG_FILE = None
            
        llevel = self.LOG_LEVEL.lower()
        if llevel == "debug":
            self.LOG_LEVEL = logging.DEBUG
        elif llevel == "info":
            self.LOG_LEVEL = logging.INFO
        elif llevel == "warning":
            self.LOG_LEVEL = logging.WARNING
        elif llevel == "error":
            self.LOG_LEVEL = logging.ERROR
        else:
            self.LOG_LEVEL = logging.DEBUG

try:
    _CONFIGURATION_GENERAL
except:
    _CONFIGURATION_GENERAL = ConfigGeneral(
        "general",
        {
            "TIMEOUT_COMMANDS":10,
            "CLUES_SECRET_TOKEN": "",
            "CLUES_PORT": 8000,
            # "CLUES_HOST": "localhost",
            "LOG_FILE":"",
            "LOG_LEVEL":"debug",
            "LRMS_CLASS": "",
            "POWERMANAGER_CLASS":""
        },
        callback = ConfigGeneral.parseconfig
    )
