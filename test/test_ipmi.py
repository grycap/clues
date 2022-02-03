#!/usr/bin/env python
#
# CLUES - Cluster Energy Saving System
# Copyright (C) 2018 - GRyCAP - Universitat Politecnica de Valencia
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
import unittest
import sys
import os
from mock.mock import patch

sys.path.append("..")
sys.path.append(".")

from cluesplugins.ipmi import powermanager, config_ipmi


class TestIPMI(unittest.TestCase):
    """ Class to test IPMI """
    def __init__(self, *args):
        """Init test class."""
        unittest.TestCase.__init__(self, *args)

    def setUp(self):
        config_ipmi.IPMI_CMDLINE_POWON = "poweron %%h"
        config_ipmi.IPMI_CMDLINE_POWOFF = "poweroff %%a"
        config_ipmi.IPMI_HOSTS_FILE = "./ipmi.hosts"

        with open(config_ipmi.IPMI_HOSTS_FILE, 'w') as f:
            f.write("10.0.0.1 node1\n10.0.0.2 node2\n")

    def tearDown(self):
        try:
            os.unlink(config_ipmi.IPMI_HOSTS_FILE)
        except Exception:
            pass

    @patch('cpyutils.runcommand.runcommand')
    def test_power_on(self, runcommand):
        runcommand.return_value = True, ""
        test_ipmi = powermanager()
        res = test_ipmi.power_on("node1")
        self.assertEqual(res, (True, 'node1'))
        self.assertEqual(runcommand.call_args_list[0][0][0], 'poweron node1')

    @patch('cpyutils.runcommand.runcommand')
    def test_power_off(self, runcommand):
        runcommand.return_value = True, ""
        test_ipmi = powermanager()
        res = test_ipmi.power_off("node2")
        self.assertEqual(res, (True, 'node2'))
        self.assertEqual(runcommand.call_args_list[0][0][0], 'poweroff 10.0.0.2')


if __name__ == "__main__":
    unittest.main()
