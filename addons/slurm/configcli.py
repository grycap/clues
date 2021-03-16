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
cpyutils.config.set_paths([ './etc/', '~/clues2/etc/', '/etc/clues2/' ])
cpyutils.config.set_main_config_file("clues2.cfg")
cpyutils.config.set_config_filter(filter_="*.cfg")
#print "using configuration files at paths %s" % ",".join(cpyutils.config.existing_config_files())

try:
    config_client
except:
    config_client = cpyutils.config.Configuration(
        "client",
        {
                "CLUES_SECRET_TOKEN": "",
                "CLUES_XMLRPC":"http://localhost:8000/RPC2",
                "CLUES_REQUEST_WAIT_TIMEOUT":300,
                "LOG_FILE":"/var/log/clues2/clues2-cli.log",
                "LOG_LEVEL":"debug"
        }
    )
    config_client.maploglevel("LOG_LEVEL")
    import logging
    try:
        from xmlrpclib import ServerProxy
    except ImportError:
        from xmlrpc.client import ServerProxy
    logging.basicConfig(filename=config_client.LOG_FILE, level=config_client.LOG_LEVEL, format='%(asctime)-15s %(message)s')
    
def get_clues_proxy_from_config():
    global config_client
    return ServerProxy(config_client.CLUES_XMLRPC)

try:
    config_general
except:
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
    
    config_general = ConfigGeneral(
        "general",
        {
            "LOG_FILE":"",
            "LOG_LEVEL":"debug",
        },
        callback = ConfigGeneral.parseconfig
    )

'''
try:
    config_client
except:
    config_client = cpyutils.config.Configuration(
        "client",
        {
            "LOG_FILE":config_general.LOG_FILE,
            "CLUES_REMOTE_SERVER_SECRET_TOKEN":"",
            "CLUES_REMOTE_SERVER_PORT":8000,
            "CLUES_REMOTE_SERVER_HOST":"localhost",
            "CLUES_REMOTE_SERVER_INSECURE": False,
        }
    )
'''