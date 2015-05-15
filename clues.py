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
import xmlrpclib
import sys
import getpass
import logging
import configcli
import clueslib.helpers

def main_function():

    proxy = configcli.get_clues_proxy_from_config()
    sec_info = configcli.config_client.CLUES_SECRET_TOKEN

    try:
        proxy.version()
    except:
        print "Could not connect to CLUES server %s (please, check if it is running)" % configcli.config_client.CLUES_XMLRPC
        sys.exit()
    args = sys.argv[1:]
    if len(args) == 0:
        sys.exit()

    operation = (args[0].strip()).lower()
    parameters = args[1:]

    if operation == 'status':
        if (len(parameters) != 0) and (len(parameters) != 1):
            logging.error("usage: status [node]")
            sys.exit(-1)
        nname = ""
        if (len(parameters) == 1):
            nname = parameters[0]
        succeed, text = proxy.status(sec_info, "")
        if succeed:
            print text
        else:
            logging.error("Could not get the status of CLUES (%s)" % text)
            sys.exit(-1)

    if (operation == 'recover'):
        if (len(parameters) != 1):
            logging.error("usage: recover [node]")
            sys.exit(-1)
        nname = parameters[0]
        '''
        idle_txt = False
        if (len(parameters) == 2):
            idle_txt = parameters[1].lower()
        if idle_txt == 'idle':
            idle = True
        elif idle_txt == 'off':
            idle = False
        else:
            logging.error("usage: recover [node] [off|idle]")
            sys.exit(-1)
        ''' 
        succeed, text = proxy.recover_node(sec_info, nname)
        if not succeed:
            print text
            sys.exit(-1)
        else:
            print text

    if (operation == 'enable'):
        if (len(parameters) != 0) and (len(parameters) != 1):
            logging.error("usage: enable [node]")
            sys.exit(-1)
        nname = ""
        if (len(parameters) == 1):
            nname = parameters[0]
        succeed, text = proxy.enable_node(sec_info, nname, True)
        if not succeed:
            sys.exit(-1)
        else:
            print text

    if (operation == 'disable'):
        if (len(parameters) != 0) and (len(parameters) != 1):
            logging.error("usage: disable [node]")
            sys.exit(-1)
        nname = ""
        if (len(parameters) == 1):
            nname = parameters[0]
        succeed, text = proxy.enable_node(sec_info, nname, False)
        if not succeed:
            sys.exit(-1)
        else:
            print text

    if (operation == 'poweron'):
        if (len(parameters) < 1):
            logging.error("usage: poweron [node]")
            sys.exit(-1)
        for nname in parameters:
            succeed, text = proxy.poweron(sec_info, nname)
            if not succeed:
                print "node %s could not be powered on (%s)" % (nname, text)
                sys.exit(-1)
            else:
                print text

    if (operation == 'poweroff'):
        if (len(parameters) < 1):
            logging.error("usage: poweroff [node] [node] ...")
            sys.exit(-1)
        for nname in parameters:
            succeed, text = proxy.poweroff(sec_info, nname)
            if not succeed:
                print "node %s could not be powered off (%s)" % (nname, text)
                sys.exit(-1)
            else:
                print text

    if (operation == 'nodeinfo_xml') or (operation == 'nodeinfo'):
        if (len(parameters) < 1):
            logging.error("usage: nodeinfo [node] [node] ...")
            sys.exit(-1)
        nname = ""

        separator = ""
        if len(parameters) > 0:
            separator = "-"*40
            
        for nname in parameters:
            if (operation == 'nodeinfo_xml'):
                succeed, text = proxy.get_node_description(sec_info, nname, True)
            else:
                succeed, text = proxy.get_node_description(sec_info, nname, False)
            if not succeed:
                print "could not get the description of node %s (%s)" % (nname, text)
            else:
                print text
            if separator != "":
                print separator

    if (operation == 'shownode'):
        if (len(parameters) < 1):
            logging.error("usage: shownode [node] [node] ...")
            sys.exit(-1)
        nname = ""

        separator = ""
        if len(parameters) > 0:
            separator = "-"*40
            
        for nname in parameters:
            succeed, text = proxy.get_node_description(sec_info, nname, True)
            if not succeed:
                print "could not get the description of node %s (%s)" % (nname, text)
            else:
                from clueslib.node import Node
                node = Node.fromxml(Node("",0,0,0,0), text)
                retval = "Name: %s\n" % node.name
                
                # TODO: sale "no"
                if node.enabled:
                    retval += "Enabled: Yes\n"
                else:
                    retval += "Enabled: No\n"
                retval += "State: %s\n" % node.state2str[node.state]
                retval += "Time got current state: %s\n" % node.timestamp_state
                retval += "Slots (free/total): %s/%s\n" % (node.slots_free, node.slots_count)
                retval += "Memory (free/total): %s/%s\n" % (node.memory_free, node.memory_total)
                retval += "Times that failed power (on/off): %s/%s\n" % (node.power_on_operation_failed, node.power_off_operation_failed)
                retval += "Keywords: %s" % node.keywords
                print retval
            if separator != "":
                print separator

    if (operation == 'req_create'):
        if (len(parameters) != 2) and (len(parameters) != 3) and (len(parameters) != 4):
            logging.error("usage: req_create [cpu] [memory] [extra info] [number of nodes]")
            sys.exit(-1)
        text = ""
        try:
            cpu = int(parameters[0])
            memory = int(parameters[1])
            extra = ""
            if len(parameters) > 2:
                extra = parameters[2]
            nodecount = 1
            if len(parameters) == 4:
                nodecount = int(parameters[3])
        except:
            succeed = False
            logging.error("usage: req_create [cpu] [memory] [extra info] [number of nodes]")
            sys.exit(-1)

        succeed, text = proxy.request_create(sec_info, cpu, memory, nodecount, extra)
        if not succeed:
            print "request could not be created (%s)" % (text)
            sys.exit(-1)
        else:
            print text

    if (operation == 'req_wait'):
        if (len(parameters) != 1) and (len(parameters) != 2):
            logging.error("usage: req_wait [req_id] [timeout]")
            sys.exit(-1)
        text = ""
        try:
            req_id = int(parameters[0])
            timeout = -1
            if len(parameters) == 2:
                timeout = int(parameters[1])
        except:
            succeed = False
            logging.error("usage: req_wait [req_id] [timeout]")
            sys.exit(-1)

        succeed, text = proxy.request_wait(sec_info, req_id, timeout)
        if not succeed:
            print "request could not be waited (%s)" % (text)
            sys.exit(-1)
        else:
            if text:
                print "request %s is not currently in the queue" % req_id
            else:
                print "request %s is still pending" % req_id

    if operation == 'req_get':
        if (len(parameters) != 0):
            logging.error("usage: req_get")
            sys.exit(-1)
        succeed, text = proxy.get_requests(sec_info)
        if succeed:
            print text
        else:
            logging.error("Could not get the requests from CLUES (%s)" % text)
            sys.exit(-1)

    sys.exit(0)
    
if __name__ == '__main__':
    main_function()    
    