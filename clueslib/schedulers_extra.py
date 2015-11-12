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
import cpyutils.eventloop
import logging
from node import Node
import request
import schedulers
from schedulers import CLUES_Scheduler

_LOGGER = logging.getLogger("[SCHED]")

class CLUES_Scheduler_Recover_OFF_ERR(CLUES_Scheduler):
    def schedule(self, requests_queue, monitoring_info, candidates_on, candidates_off):
        now = cpyutils.eventloop.now()
        if now - self._timestamp_run < self.EXTRA_NODES_PERIOD:
            # We are not running now
            return True

        return True
    def __init__(self):
        #
        # Falta incorporar el metodo RECOVER para la plataforma
        #
        CLUES_Scheduler.__init__(self, "Tries to recover those nodes that are in _ERR status by powering them on or off")
        cpyutils.config.read_config("scheduling",
            {
                "RECOVER_NODES_PERIOD": 300
            },
            self)
        self._timestamp_run = cpyutils.eventloop.now()