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
import string
import random
import cpyutils.evaluate
import sys
import types
try:
    from xmlrpclib import ServerProxy
except:
    from xmlrpc.client import ServerProxy


import cpyutils.log
_LOGGER = cpyutils.log.Log("HELPERS")

def read_hosts_file(filename):
    _nname_2_ip = {}
    _ip_2_nname = {}
    try:
        for line in open(filename):
            line = line.split('#',1)[0]
            line = line.strip()
            if line != "":
                line = line.replace("\t"," ")
                line_split = line.split(" ")
                lineparts = [ x for x in line_split if x != "" ]
                if len(lineparts) == 2:
                    (ip, nname) = lineparts
                    _nname_2_ip[nname] = ip
                    _ip_2_nname[ip] = nname
                else:
                    _LOGGER.warning("malformed host entry")
    except IOError:
        _LOGGER.error("cannot read hosts entries for file %s" % filename)
        return None, None
        
    return _nname_2_ip, _ip_2_nname

def str_to_bool(enabled):
    return (enabled == "True")

def bool_to_str(enabled):
    if enabled: return "True"
    return "False"

def gen_passwd(size):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(size))    

def dict_j_2_r_id(dict1):
    result = {}
    for n_id, jobs in dict1.items():
        result[n_id] = [ j.id for j in jobs ]
    return result

def val_default(value, default = None):
    if value is None:
        return default
    return value

def merge_dicts_of_lists(dict1, dict2):
    result = {}
    for n_id, jobs in dict1.items():
        if n_id in dict2:
            r = dict1[n_id] + [ x for x in dict2[n_id] if x not in dict1[n_id] ]
        else:
            r = dict1[n_id]
        if len(r) > 0:
            result[n_id] = r
            
    for n_id, jobs in dict2.items():
        if n_id not in dict1:
            result[n_id] = dict2[n_id]
    return result

'''
class CommentlessFile(file):
    def readline(self):
        line = super(CommentlessFile, self).readline()
        if line:
            line = line.split('#', 1)[0].strip()
            return line + '\n'
        else:
            return ''
'''

def get_server_proxy_from_cmdline(config_general):
    XMLRPC_SERVER = 'http://%s:%s/RPC2' %(config_general.CLUES_REMOTE_SERVER_HOST, config_general.CLUES_REMOTE_SERVER_PORT)
    
    from optparse import OptionParser, OptionParser
    parser = OptionParser()
    parser.disable_interspersed_args()
    parser.add_option("-s", "--server", dest="XMLRPC_SERVER", default=None, help="the server")
    parser.add_option("-k", "--secret-key", dest="SECRET", default=None, help="secret key to authenticate against CLUES")
    parser.add_option("-i", "--insecure", action="store_true", dest="INSECURE", default=None, help="set to insecure (assume that clues won't need to authenticate)")
    (options, args) = parser.parse_args()
    
    if options.SECRET is None:
        options.SECRET = config_general.CLUES_REMOTE_SERVER_SECRET_TOKEN
            
    if options.XMLRPC_SERVER is None:
        options.XMLRPC_SERVER = XMLRPC_SERVER
        
    if options.INSECURE is None:
        options.INSECURE = config_general.CLUES_REMOTE_SERVER_INSECURE

    proxy = ServerProxy(options.XMLRPC_SERVER)

    try:
        version = proxy.version()
    except:
        return False, "Could not connect to CLUES server %s (please, check if it is running)" % options.XMLRPC_SERVER, None, None
    
    if options.INSECURE:
        sec_info = ""
    else:
        sec_info = options.SECRET
    
    success, sec_info = proxy.login(sec_info)
    return success, sec_info, proxy, args

def get_server_proxy_from_config(config_client):
    XMLRPC_SERVER = 'http://%s:%s/RPC2' %(config_client.CLUES_REMOTE_SERVER_HOST, config_client.CLUES_REMOTE_SERVER_PORT)
    proxy = ServerProxy(XMLRPC_SERVER)

    try:
        version = proxy.version()
    except:
        return False, "Could not connect to CLUES server %s (please, check if it is running)" % XMLRPC_SERVER, None
    
    if config_client.CLUES_REMOTE_SERVER_INSECURE:
        sec_info = ""
    else:
        sec_info = config_client.CLUES_REMOTE_SERVER_SECRET_TOKEN
    
    success, sec_info = proxy.login(sec_info)
    return success, sec_info, proxy

class SerializableXML():
    def toxml(self):
        retval = []
        for v in self.__dict__:
            if isinstance(self.__dict__[v], dict):
                retval.append("<%s>%s</%s>" % (v, cpyutils.evaluate.TypedDict2Str(self.__dict__[v]), v))
            else:
                retval.append("<%s>%s</%s>" % (v, self.__dict__[v], v))
        return "<node>%s</node>" % ("".join(retval))
    
    def tokeywordstr(self):
        retval = []
        for v in self.__dict__:
            if isinstance(self.__dict__[v], dict):
                retval.append("%s=%s" % (v, cpyutils.evaluate.TypedDict2Str(self.__dict__[v])))
            else:
                retval.append("%s=%s" % (v, self.__dict__[v]))
        return (";".join(retval))

    @staticmethod
    def fromxml(reference_object, xml_str):
        import cpyutils.xmlobject
        n = cpyutils.xmlobject.XMLObject(xml_str)
        
        for v in reference_object.__dict__:
            n.values.append(v)
            
        n._parse(xml_str, None)
        for v in reference_object.__dict__:
            if v in n.__dict__:
                if isinstance(reference_object.__dict__[v], bool):
                    val = n.__dict__[v].lower()
                    reference_object.__dict__[v] = (( val == "true") or (val == "1"))
                elif isinstance(reference_object.__dict__[v], int):
                    reference_object.__dict__[v] = int(float(n.__dict__[v]))
                elif isinstance(reference_object.__dict__[v], float):
                    reference_object.__dict__[v] = float(n.__dict__[v])
                else:
                    reference_object.__dict__[v] = n.__dict__[v]
        return reference_object

def str_to_class(field):
    container_module = field.strip().split(".")
    module_name = ".".join(container_module[0:-1])
    if module_name == "":
        raise NameError("The class is empty. The format should be <module>.<class>.")

    import importlib
    module = importlib.import_module(module_name)

    class_name = container_module[-1]
    try:
        # identifier = getattr(sys.modules[module_name], class_name)
        identifier = getattr(module, class_name)
    except AttributeError:
        raise NameError("%s doesn't exist." % field)
    try:
        if isinstance(identifier, (types.ClassType, types.TypeType)):
            return identifier
    except:
        if isinstance(identifier, type):
            return identifier

    raise TypeError("%s is not a class." % field)