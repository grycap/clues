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
import time

_ERROR_IMPORTING=None

try:
    import configserver
    import cpyutils.rpcweb
    import cpyutils.eventloop
    import clueslib.request
    import clueslib.platform
    # import clueslib.schedulers_extra    
    # import clueslib.evaluate

    class clues_web_server(cpyutils.rpcweb.web_server):
        def _access_page(self):
            return "<html><body><form method=get>secret token<input type=text name=\"secret\" id=\"secret\"><input type=submit></body></html>"
        
        def _mainpage(self):
            import web
            query = web.ctx.query
            return "<html><head>\
            <style>body {font-family: arial;} </style>\
            <script src=\"http://code.jquery.com/jquery-latest.min.js\" type=\"text/javascript\"></script>\
                    <script language=\"javascript\" type=\"text/javascript\">\
                            var timeout = setInterval(reloadHosts, 5000);\
                            var timeout = setInterval(reloadRequests, 5000);\
                            var timeout = setInterval(reloadJobs, 2000);\
                            function reloadHosts () {$('#hosts').load('/status/hosts%s');}\
                            function reloadRequests () {$('#requests').load('/status/requests%s');}\
                            function reloadJobs () {$('#jobs').load('/status/jobs%s');}\
                    </script>\
                    <body><a href=\"/%s\">clear filter</a>\
                    <h1>Hosts</h1>\
                    <div id='hosts'>\
                    </div>\
                    <h1>Requests</h1>\
                    <div id='requests'>\
                    </div>\
                    <h1>Jobs</h1>\
                    <div id='jobs'>\
                    </div>\
                    %%s</body>\
                    <script language=\"javascript\" type=\"text/javascript\">reloadHosts();reloadJobs();reloadRequests();</script>\
                    </html>" % (query, query, query, query)
        
        def _status(self, path, secret, ref = ""):
            
            global CLUES_DAEMON
            if path == 'hosts':
                succeed, nodeinfo = rawstatus(secret, "")
    
                if not succeed:
                    return "info not available"
    
                nodes = nodeinfo.get_list()
                node_rows = []
                en_str = { True:'enabled', False:'disabled'}
                now = cpyutils.eventloop.now()
                for k in sorted(nodes):
                    n = nodes[k]
                    days = int((now - n.timestamp_state) / 86400)
                    node_str="<tr><td><a href=\"/host/%s%s\">%s</a></td><td>%s</td><td>%s</td><td>%sd %s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (n.name, ref, n.name[:24], (n.state2str[n.state])[:8], en_str[n.enabled], days, time.strftime("%Hh%M\'%S\"", time.gmtime(now - n.timestamp_state)), str(n.slots_count-n.slots_free), str(n.memory_total-n.memory_free), str(n.slots_count), str(n.memory_total))
                    node_rows.append(node_str)
                return "<table>%s</table>" % "\n".join(node_rows)
            if path == 'jobs':
                return "%s<br>%s" % (cpyutils.eventloop.now(), str(CLUES_DAEMON.get_job_list()).replace("\n","<br>"))

            if path == 'requests':
                return str(CLUES_DAEMON.get_requests_list()).replace("\n","<br>")
            
            return "Not available"

        def _get_var(self, varname):
            import web
            secret = ""
            
            for vv in web.ctx.query.lstrip("?").split("&"):
                va = vv.split("=")
                key = None
                val = None
                if len(va) == 2:
                    key, val = va
                if key == varname:
                    secret = val

            return secret            
        
        def _get_host(self, secret, hostname):
            success, result = get_node_description(secret, hostname, False)
            if success:
                return result.replace(";","<BR>")
            return "Could not get info"
        
        def GET(self, url):
            import web
            path = web.ctx.path.lstrip("/").split("/")
            
            secret = self._get_var("secret")
            if (secret == "") or (secret is None):
                return self._access_page()
            
            if len(path) > 1 and path[0] == 'status':
                return self._status("/".join(path[1:]), secret, web.ctx.query)
            
            if path[0]=='host':
                # print web.ctx.path
                # print self._mainpage() % self._get_host("/".join(path[1:]))
                return self._mainpage() % self._get_host(self._get_var("secret"), "/".join(path[1:]))

            return self._mainpage() % ""
            
            pass
                    
            vv = web.ctx.path.lstrip("/").split("/")
            hdetail = None
            if len(vv) == 2:
                if vv[0] == 'host':
                    hdetail = vv[1]

            if hdetail is not None:
                success, hdetail = get_node_description(secret, hdetail, False)
            else:
                hdetail=""

            success, requests = get_requests(secret)

            global CLUES_DAEMON

            retval="<html><body><table>%s</table>\n%s\n%s\n%s\n</body></html>" % ("\n".join(node_rows), requests.replace("\n","<BR>"), hdetail.replace(";", "<BR>"), str(CLUES_DAEMON).replace("\n","<BR>"))
            return retval
        
except Exception, e:
    _ERROR_IMPORTING = e
    pass

VERSION = "2.0.3b"
_LOGGER = logging.getLogger("[SERVER]")
_LOGGER.level = logging.DEBUG

def GET_CLUES_DAEMON():
    global CLUES_DAEMON
    print CLUES_DAEMON
    return CLUES_DAEMON

class Authentication_secret(object):
    def __init__(self, SECRET):
        self._SECRET = SECRET
    
    def check_secret(self, secret):
        if secret == self._SECRET:
            return True, "Secret key correct"
        return False, "Error checking the secret key. Please check the configuration file and the CLUES_SECRET_TOKEN setting"

AUTH_ENGINE = Authentication_secret("")

def version():
    return VERSION

def login(sec_info):
    global AUTH_ENGINE
    succeed, explain = AUTH_ENGINE.check_secret(sec_info)

    if succeed:
        return True, sec_info
    
    return False, "Could not authorize user"

def rawstatus(sec_info, nname):
    global AUTH_ENGINE
    succeed, explain = AUTH_ENGINE.check_secret(sec_info)
    if not succeed:
        return False, explain
    
    global CLUES_DAEMON
    nodelist = CLUES_DAEMON.get_nodelist()
    return True, nodelist

def status(sec_info, nname):
    #global AUTH_ENGINE
    #succeed, explain = AUTH_ENGINE.check_secret(sec_info)
    #if not succeed:
    #    return False, explain
    #
    #global CLUES_DAEMON
    #nodelist = CLUES_DAEMON.get_nodelist()
    succeed, nodelist = rawstatus(sec_info, nname)
    if not succeed:
        return False, nodelist
    
    fmt_hea = "%-24s   %8s   %8s   %11s   %14s   %15s"
    fmt_str = "%-24s   %8s   %8s   %11s   %4s,%-9s    %3s,%-9s"
    
    legend = fmt_hea % ("node", "state", "enabled", "time stable", "(cpu,mem) used", "(cpu,mem) total")
    head = "-"*24 + "---" + "-"*8 + "---" + "-"*8 + "---" + "-"*11 + "---" + "-"*14 + "---" + "-"*15
    retval = "%s\n%s\n" % (legend, head)
    
    now = cpyutils.eventloop.now()
    head = "-"*24 + "---" + "-"*8 + "---" + "-"*8 + "---"
    en_str = { True:'enabled', False:'disabled'}
    
    nodes = nodelist.get_list()
    
    for k in sorted(nodes):
        n = nodes[k]
        n_str = fmt_str % (n.name[:24], (n.state2str[n.state])[:8], en_str[n.enabled], time.strftime("%Hh%M\'%S\"", time.gmtime(now - n.timestamp_state)), str(n.slots_count-n.slots_free), str(n.memory_total-n.memory_free), str(n.slots_count), str(n.memory_total))
        retval = "%s%s\n" % (retval, n_str)

    return True, retval

def poweron(sec_info, node):
    global AUTH_ENGINE

    succeed, explain = AUTH_ENGINE.check_secret(sec_info)
    # _LOGGER.debug("poweron result: %s" % succeed)
    if not succeed:
        return False, explain

    global CLUES_DAEMON
    success, nname = CLUES_DAEMON.power_on(node, True)
    if not success:
        return False, "could not power on the node %s" % node
    else:
        return True, "node %s powered on" % nname

def poweroff(sec_info, node):
    global AUTH_ENGINE

    succeed, explain = AUTH_ENGINE.check_secret(sec_info)
    # _LOGGER.debug("poweroff result: %s" % succeed)
    if not succeed:
        return False, explain

    global CLUES_DAEMON
    success, nname = CLUES_DAEMON.power_off(node, True)
    
    if not success:
        return False, "could not power off the node %s" % node
    else:
        return True, "node %s powered off" % nname

def recover_node(sec_info, node):
    global AUTH_ENGINE

    succeed, explain = AUTH_ENGINE.check_secret(sec_info)
    # _LOGGER.debug("recover_node result: %s" % succeed)
    if not succeed:
        return False, explain

    global CLUES_DAEMON
    success, msg = CLUES_DAEMON.recover_node(node)
    
    if not success:
        return False, "could not recover the node %s (%s)" % (node, msg)
    else:
        return True, msg

def enable_node(sec_info, nname, enable):
    global AUTH_ENGINE

    succeed, explain = AUTH_ENGINE.check_secret(sec_info)
    # _LOGGER.debug("enable_node result: %s" % succeed)
    if not succeed:
        return False, explain

    global CLUES_DAEMON
    succeed, explain = CLUES_DAEMON.enable_host(nname, enable)

    if enable:
        opres = ("enabled", "enable")
    else:
        opres = ("disabled", "disable")
    if succeed:
        return True, "node %s successfully %s" % (nname, opres[0])
    else:
        return False, "could not %s node %s (%s)" % (opres[1], nname, explain)

def request_create(sec_info, cpu, memory, nodes, request_string, job_id = -1, job_nodes = []):
    global AUTH_ENGINE

    succeed, explain = AUTH_ENGINE.check_secret(sec_info)
    # _LOGGER.debug("request_create result: %s" % succeed)
    if not succeed:
        return False, explain

    global CLUES_DAEMON

    # TODO: it is pending to include other feaures for the nodes (i.e. keywords or expression to evaluate the nodes)
    if job_id < 0:
        job_id = None
        job_nodes = []
        
    r = clueslib.request.Request(clueslib.request.ResourcesNeeded(cpu, memory, [ request_string ], nodecount = nodes), job_id, job_nodes)
    r_id = CLUES_DAEMON.request(r)

    return True, r_id

def request_wait(sec_info, req_id, timeout):
    global AUTH_ENGINE

    succeed, explain = AUTH_ENGINE.check_secret(sec_info)
    # _LOGGER.debug("request_wait result: %s (explain: %s)" % (succeed, explain))
    if not succeed:
        return False, explain

    global CLUES_DAEMON
    start_time = cpyutils.eventloop.now()
    
    # TODO: should we launch this wait in other thread, just to avoid hanging the server?
    
    while CLUES_DAEMON.request_in_queue(req_id):
        current_time = cpyutils.eventloop.now()
        if (timeout > 0) and ((current_time - start_time) > timeout):
            return True, False
        
        time.sleep(0.5)
    
    return True, True

def get_node_description(sec_info, nname, xml = True):
    global AUTH_ENGINE

    succeed, explain = AUTH_ENGINE.check_secret(sec_info)
    # _LOGGER.debug("get_node_description result: %s" % succeed)
    if not succeed:
        return False, explain

    global CLUES_DAEMON
    node = CLUES_DAEMON.get_node(nname)
    
    if node is None:
        return False, "node is not managed by CLUES"
    
    if xml:
        return True, node.toxml()
    else:
        return True, node.tokeywordstr()

def get_requests(sec_info):
    global AUTH_ENGINE

    succeed, explain = AUTH_ENGINE.check_secret(sec_info)
    # _LOGGER.debug("get_request result: %s" % succeed)
    if not succeed:
        return False, explain

    global CLUES_DAEMON
    return True, str(CLUES_DAEMON.get_requests_list())

#--------------------

def qsub(cpu, memory, duration):
    global LRMS
    j_id = LRMS.submit_job(duration, cpu, memory)
    return j_id

def qstat():
    global LRMS
    return str(LRMS)

def pbsnodes():
    global LRMS
    nodelist = node.NodeList(LRMS.get_nodeinfolist())
    return str(nodelist)

def cluesnodes():
    global CLUES_DAEMON
    nodelist = CLUES_DAEMON.get_nodelist()
    return str(nodelist)

def hardpoweron(node):
    global POW_MGR
    if not POW_MGR.power_on(node):
        return "could not power on the node %s" % node
    else:
        return "node %s powered on" % node

def hardpoweroff(node):
    global POW_MGR
    if not POW_MGR.power_off(node):
        return "could not power off the node %s" % node
    else:
        return "node %s powered off" % node

def remove_node(node):
    global LRMS
    if not LRMS.delete_node(node):
        return "could not remove the node %s (perhaps it does not exist?)" % node
    else:
        return "node %s has been removed from the infrastructure" % node

def create_node(node, slots, memory):
    global LRMS
    if not LRMS.create_node(node, slots, memory):
        return "could not create the node %s (prehaps already exists a node with this name?)" % node
    else:
        return "node %s has been created" % node

def cluesd_info():
    global CLUES_DAEMON
    return str(CLUES_DAEMON)

def generate_activity(t):
    cpyutils.eventloop.get_eventloop().add_event(t, "FAKE Event - like a ping", stealth = False)
    return True

'''
def request_create(cpu, memory):
    global CLUES_DAEMON
    r = request.Request(request.ResourcesNeeded(cpu, memory, nodecount = 1))
    r_id = CLUES_DAEMON.request(r)

    # TODO 5: event-driven
    # cpyutils.eventloop.get_eventloop().add_event(0, "Scheduling due to new request", CLUES_DAEMON.schedule)
    return r_id

def request_wait(req_id, timeout):
    global CLUES_DAEMON
    start_time = cpyutils.eventloop.now()
    
    while CLUES_DAEMON.request_in_queue(req_id):
        current_time = cpyutils.eventloop.now()
        if (timeout > 0) and ((current_time - start_time) > timeout):
            return False
        
        time.sleep(0.1)
    
    return True
'''

def get_events():
    return str(cpyutils.eventloop.get_eventloop())

'''
def status():
    global PLATFORM
    nodelist = PLATFORM.get_nodelist()
    return str(nodelist)
    # return True
'''

def main_loop():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-s", "--simmulated-time", dest="RT_MODE", action="store_false", default=True, help="runs app in real time")
    (options, args) = parser.parse_args()

    try:
        LOG_FILE=configserver._CONFIGURATION_GENERAL.LOG_FILE
        LOG_LEVEL=configserver._CONFIGURATION_GENERAL.LOG_LEVEL
    except:
        LOG_FILE=None
        LOG_LEVEL=logging.DEBUG

    logging.basicConfig(filename=LOG_FILE,level=LOG_LEVEL,format="[%(levelname)s] %(asctime)-15s %(message)s")

    if _ERROR_IMPORTING is not None:
        if isinstance(_ERROR_IMPORTING, ImportError):
            logging.critical("Error starting CLUES because there is a missing class: %s" % str(_ERROR_IMPORTING))
        else:
            logging.critical("Error starting CLUES: %s" % str(_ERROR_IMPORTING))
        sys.exit(-1)

    cpyutils.eventloop.create_eventloop(options.RT_MODE)
    global AUTH_ENGINE
    AUTH_ENGINE = Authentication_secret(configserver._CONFIGURATION_GENERAL.CLUES_SECRET_TOKEN)

    # -------------------------------------------------------------------------------
    # This is how we can integrate to a ONE deployment
    #   i.e. monitoring the hosts
    # -------------------------------------------------------------------------------
    # import plugins.one
    # LRMS = plugins.one.lrms()
    
    # -------------------------------------------------------------------------------
    # This is how we can integrate to a PBS deployment
    #   i.e. monitoring the hosts
    # -------------------------------------------------------------------------------
    # import plugins.pbs
    # LRMS = plugins.pbs.lrms()
    
    '''
    import plugins.pbs
    LRMS = plugins.pbs.lrms()
    import plugins.one
    POW_MGR = plugins.one.powermanager()
    '''
    if configserver._CONFIGURATION_GENERAL.LRMS_CLASS == "":
        logging.error("a LRMS class must be defined for CLUES, you should use LRMS_CLASS in the clues2.cfg file")
        exit(-1)
    
    if configserver._CONFIGURATION_GENERAL.POWERMANAGER_CLASS == "":
        logging.warning("a POWERMANAGER class must be defined for CLUES, you should use POWERMANAGER_CLASS in the clues2.cfg file")
        configserver._CONFIGURATION_GENERAL.POWERMANAGER_CLASS = None
    
    import importlib
    try:
        LRMS_CLASS=importlib.import_module(configserver._CONFIGURATION_GENERAL.LRMS_CLASS)
        _LOGGER.info("activating lrms: \"%s\"" % configserver._CONFIGURATION_GENERAL.LRMS_CLASS)
    except Exception, e:
	logging.error(e)
        logging.error("could not import the LRMS class stated in LRMS_CLASS (%s) from the clues2.cfg file" % configserver._CONFIGURATION_GENERAL.LRMS_CLASS)
        exit(-1)

    if configserver._CONFIGURATION_GENERAL.POWERMANAGER_CLASS is not None:
        try:
            POWERMANAGER_CLASS=importlib.import_module(configserver._CONFIGURATION_GENERAL.POWERMANAGER_CLASS)
            _LOGGER.info("activating powermanager: \"%s\"" % configserver._CONFIGURATION_GENERAL.POWERMANAGER_CLASS)
        except Exception, e:
            logging.error(e)
            logging.error("could not import the POWERMANAGER class stated in POWERMANAGER_CLASS (%s) from the clues2.cfg file" % configserver._CONFIGURATION_GENERAL.POWERMANAGER_CLASS)
            exit(-1)
    else:
        logging.warning("it is not defined any POWERMANAGER class in the configuration file. CLUES will not be able to power on or off the nodes")

    global LRMS
    try:
        LRMS=LRMS_CLASS.lrms()
    except Exception, e:
	logging.error(e)
        logging.error("cannot use %s as a plugin for LRMS in CLUES, because the 'lrms' class is not defined" % configserver._CONFIGURATION_GENERAL.LRMS_CLASS)
        exit(-1)

    logging.debug("LRMS %s initialized" % LRMS.get_id())

    POW_MGR = None
    if configserver._CONFIGURATION_GENERAL.POWERMANAGER_CLASS is not None:
        try:
            POW_MGR=POWERMANAGER_CLASS.powermanager()
        except Exception, e:
            logging.error(e)
            logging.error("cannot use %s as a plugin for POWERMANAGER in CLUES, because the 'powermanager' class is not defined" % configserver._CONFIGURATION_GENERAL.POWERMANAGER_CLASS)
            exit(-1)
    
    import clueslib.platform
    server = cpyutils.rpcweb.get_dispatcher()
    '''
    server.register_function(qsub)
    server.register_function(qstat)
    server.register_function(pbsnodes)
    server.register_function(cluesnodes)
    server.register_function(hardpoweron)
    server.register_function(hardpoweroff)
    server.register_function(create_node)
    server.register_function(remove_node)
    server.register_function(cluesd_info)
    server.register_function(generate_activity)
    '''

    # Production
    server.register_function(version)
    server.register_function(login)
    server.register_function(status)
    server.register_function(poweron)
    server.register_function(poweroff)
    server.register_function(enable_node)
    server.register_function(recover_node)
    server.register_function(request_create)
    server.register_function(request_wait)
    server.register_function(get_requests)
    server.register_function(get_node_description)

    # rpcweb.start_server(clues_web_server, port=8000)
    cpyutils.rpcweb.start_server_in_thread(clues_web_server, port = configserver._CONFIGURATION_GENERAL.CLUES_PORT)

    global PLATFORM
    PLATFORM = clueslib.platform.Platform(LRMS, POW_MGR)

    from clueslib.cluesd import CluesDaemon
    global CLUES_DAEMON

    # Here we will instantiate the schedulers
    active_schedulers = []
    scheduler_classes = configserver.config_scheduling.SCHEDULER_CLASSES.split(",")
    for sclass in scheduler_classes:
        sclass = sclass.strip()
        valid_scheduler = False
        try:
            _LOGGER.debug("loading class %s for scheduling" % sclass)
            class_name = clueslib.helpers.str_to_class(sclass)
            valid_scheduler = True
        except NameError:
            _LOGGER.error("CLUES does not have access to class \"%s\" to create the scheduler" % sclass)
        except TypeError:
            _LOGGER.error("\"%s\" is not a valid class to create the scheduler" % sclass)

        if valid_scheduler:
            current_scheduler = class_name()
            if not isinstance(current_scheduler, clueslib.schedulers.CLUES_Scheduler):
                _LOGGER.error("ignoring scheduler \"%s\" because it is not a valid class to create the scheduler" % sclass)
            else:
                _LOGGER.info("Activating scheduler \"%s\"" % sclass)
                active_schedulers.append(current_scheduler)
    CLUES_DAEMON = CluesDaemon(PLATFORM, active_schedulers)
    _LOGGER.debug("CLUES server starting")

    CLUES_DAEMON.loop(options.RT_MODE)

if __name__ == '__main__':
    main_loop()