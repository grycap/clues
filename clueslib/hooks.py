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
import cpyutils.log
import cpyutils.runcommand
import os.path
from clueslib.configlib import _CONFIGURATION_HOOKS

cpyutils.log.Log.setup()
_LOGGER = cpyutils.log.Log("HOOKS")

class HookSystem:
    def _folder(self, folder):
        if (folder is None or folder == ""): return "."
        folder = os.path.expandvars(os.path.expanduser(folder))
        if (os.path.isdir(folder)): return folder
        _LOGGER.error("invalid folder %s" % folder)
        return None

    def _check_command_and_run(self, workingfolder, command, parameters):
        if (command is not None) and (command != ""):

            workingfolder = self._folder(workingfolder)
            if workingfolder is None:
                return False, None

            command = os.path.normpath(os.path.expandvars(os.path.expanduser(command)))
            if (not os.path.isabs(command)):
                command = os.path.join(workingfolder, command)

            _LOGGER.debug("hook %s is being invoked with parameters %s" % (command, parameters))

            command = [ command ] + parameters
            timeout = _CONFIGURATION_HOOKS.TIMEOUT_COMMAND
            if timeout is not None and timeout > 0:
                result, output = cpyutils.runcommand.runcommand(command, timeout = timeout, cwd = workingfolder, shell = False)
            else:
                result, output = cpyutils.runcommand.runcommand(command, cwd = workingfolder, shell = False)

            if result:
                _LOGGER.debug("ouput of command %s:\n%s" % (command, output))
            return result, output

        return True, None

    def _run_hook(self, hook, node, *parameters):
        parameters = [ str(p) for p in list(parameters)]
        result, output = self._check_command_and_run(_CONFIGURATION_HOOKS.WORKING_FOLDER, hook, [ node ] + list(parameters))
        return result

    def pre_poweron(self, node, *parameters):
        return self._run_hook(_CONFIGURATION_HOOKS.PRE_POWERON, node, *parameters)

    def post_poweron(self, node, *parameters):
        return self._run_hook(_CONFIGURATION_HOOKS.POST_POWERON, node, *parameters)

    def pre_poweroff(self, node, *parameters):
        return self._run_hook(_CONFIGURATION_HOOKS.PRE_POWEROFF, node, *parameters)

    def post_poweroff(self, node, *parameters):
        return self._run_hook(_CONFIGURATION_HOOKS.POST_POWEROFF, node, *parameters)

    def poweredon(self, node, *parameters):
        return self._run_hook(_CONFIGURATION_HOOKS.POWEREDON, node, *parameters)

    def poweredoff(self, node, *parameters):
        return self._run_hook(_CONFIGURATION_HOOKS.POWEREDOFF, node, *parameters)

    def unexpected_poweron(self, node, *parameters):
        return self._run_hook(_CONFIGURATION_HOOKS.UNEXPECTED_POWERON, node, *parameters)

    def unexpected_poweroff(self, node, *parameters):
        return self._run_hook(_CONFIGURATION_HOOKS.UNEXPECTED_POWEROFF, node, *parameters)

    def onerr(self, node, *parameters):
        return self._run_hook(_CONFIGURATION_HOOKS.ONERR, node, *parameters)

    def offerr(self, node, *parameters):
        return self._run_hook(_CONFIGURATION_HOOKS.OFFERR, node, *parameters)

    def unknown(self, node, *parameters):
        return self._run_hook(_CONFIGURATION_HOOKS.UNKNOWN, node, *parameters)

    def idle(self, node, *parameters):
        return self._run_hook(_CONFIGURATION_HOOKS.IDLE, node, *parameters)

    def used(self, node, *parameters):
        return self._run_hook(_CONFIGURATION_HOOKS.USED, node, *parameters)

    def request(self, request):
        requests_string = ';'.join(request.resources.resources.requests)
        parameters = (request.resources.resources.slots, request.resources.resources.memory, request.resources.taskcount, request.resources.maxtaskspernode, requests_string)
        return self._run_hook(_CONFIGURATION_HOOKS.REQUEST, str(request.id), *parameters)
try:
    HOOKS
except:
    HOOKS = HookSystem()