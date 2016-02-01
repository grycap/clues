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
import one

import cpyutils.log
_LOGGER = cpyutils.log.Log("PLUGIN-ONE")

class powermanager(one.powermanager):
    def power_on(self, nname):
        if nname not in self._nname_2_ip:
            _LOGGER.error("could not power on node because its IP address is unknown")
            return False, None
        
        if nname in self._mvs_seen:
            running_vm = self._mvs_seen[nname]
            _LOGGER.warning("tried to power on node %s, while there is a VM (%d) that is supposed to be already powered on with that IP" % (nname, running_vm.vm_id))

            # We are not failing, because ONE will power on other VM with a different IP, and this PM will return the new host
        
        success, result = self._one.create_vm(self._ONE_VIRTUAL_CLUSTER_TEMPLATE_ID)
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

class lrms(one.lrms):
    pass