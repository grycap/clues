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

import logging

from clueslib.schedulers import config_scheduling
from clueslib.configlib import _CONFIGURATION_CLUES, _CONFIGURATION_MONITORING
from clueslib.configlib import _CONFIGURATION_GENERAL
    
