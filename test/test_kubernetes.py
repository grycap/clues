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
import json
from mock.mock import MagicMock, patch


sys.path.append("..")
sys.path.append(".")

from cluesplugins.kubernetes import lrms
from clueslib.request import Request
from clueslib.node import Node


class TestKubernetes(unittest.TestCase):

    TESTS_PATH = os.path.dirname(os.path.abspath(__file__))

    """ Class to test IM """
    def __init__(self, *args):
        """Init test class."""
        unittest.TestCase.__init__(self, *args)

    @patch("requests.request")
    def test_get_nodeinfolist(self, request):
        kube = lrms()

        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = json.load(open(os.path.join(self.TESTS_PATH, 'test-files/nodes.json')))
        request.return_value = resp
        nodes = kube.get_nodeinfolist()
        self.assertEqual(len(nodes), 3)
        self.assertEqual(nodes['gpuwn-1.localdomain'].slots_count, 8)
        self.assertEqual(nodes['gpuwn-1.localdomain'].slots_free, 8.0)
        self.assertEqual(nodes['gpuwn-1.localdomain'].memory_total, 16713633792)
        self.assertEqual(nodes['gpuwn-1.localdomain'].memory_free, 16713633792)
        self.assertEqual(nodes['gpuwn-1.localdomain'].state, Node.IDLE)
        self.assertEqual(len(nodes['gpuwn-1.localdomain'].keywords), 8)
        self.assertEqual(nodes['gpuwn-1.localdomain'].keywords["pods_free"].value, 110)
        self.assertEqual(nodes['gpuwn-1.localdomain'].keywords["nodeName"].value, 'gpuwn-1.localdomain')
        self.assertEqual(nodes['gpuwn-1.localdomain'].keywords["nvidia.com/gpu"].value, 2)

    @patch("requests.request")
    def test_get_jobinfolist(self, request):
        kube = lrms()

        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = json.load(open(os.path.join(self.TESTS_PATH, 'test-files/pods.json')))
        request.return_value = resp
        jobs = kube.get_jobinfolist()
        self.assertEqual(len(jobs), 3)
        self.assertEqual(jobs[1].job_id, "643ed2c4-8a98-4648-97e4-55aa856b984d")
        self.assertEqual(jobs[1].state, Request.SERVED)
        self.assertEqual(jobs[1].resources.resources.slots, 0.25)
        self.assertEqual(jobs[1].resources.resources.memory, 134217728)
        self.assertEqual(jobs[1].resources.resources.requests, [('(pods_free > 0) && (nodeName = "wn-2.localdomain")' +
                                                                 ' && (nvidia.com/gpu = 1)')])


if __name__ == "__main__":
    unittest.main()
