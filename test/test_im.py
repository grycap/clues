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
from mock.mock import MagicMock, patch
from radl.radl_parse import parse_radl

sys.path.append("..")
sys.path.append(".")

from cluesplugins.im import powermanager
from clueslib.node import Node


class TestIM(unittest.TestCase):

    TESTS_PATH = os.path.dirname(os.path.abspath(__file__))

    """ Class to test IM """
    def __init__(self, *args):
        """Init test class."""
        unittest.TestCase.__init__(self, *args)

    def test_read_auth_data(self):
        res = powermanager._read_auth_data(os.path.join(self.TESTS_PATH, 'test-files/auth.dat'))
        self.assertEqual(res[0], {'type': 'InfrastructureManager', 'username': 'user', 'password': 'pass'})
        self.assertEqual(res[1], {'host': 'server:2633',
                                  'id': 'one',
                                  'password': 'pass',
                                  'type': 'OpenNebula',
                                  'username': 'user'})

    @patch("cluesplugins.im.powermanager._read_auth_data")
    @patch("cluesplugins.im.powermanager._get_server")
    @patch("cpyutils.db.DB.create_from_string")
    def test_get_inf_id(self, createdb, get_server, read_auth):
        server = MagicMock()
        server.GetInfrastructureList.return_value = (True, ['infid1'])
        get_server.return_value = server

        read_auth.return_value = {'type': 'InfrastructureManager', 'username': 'user', 'password': 'pass'}

        db = MagicMock()
        db.sql_query.return_value = True, "", []
        createdb.return_value = db

        test_im = powermanager()
        res = test_im._get_inf_id()
        self.assertEqual(res, "infid1")

    @patch("cluesplugins.im.powermanager.recover")
    @patch("cluesplugins.im.powermanager._read_auth_data")
    @patch("cluesplugins.im.powermanager._get_inf_id")
    @patch("cluesplugins.im.powermanager._get_server")
    @patch("cpyutils.db.DB.create_from_string")
    @patch("cpyutils.eventloop.now")
    def test_get_vms(self, now, createdb, get_server, get_inf_id, read_auth, recover):
        get_inf_id.return_value = "infid"
        read_auth.return_value = {'type': 'InfrastructureManager', 'username': 'user', 'password': 'pass'}
        now.return_value = 100

        server = MagicMock()
        server.GetInfrastructureInfo.return_value = (True, ['0', '1'])
        radl = """system wn (
            net_interface.0.dns_name = 'node-#N#' and
            state = 'configured'
        )"""
        server.GetVMInfo.return_value = (True, radl)
        get_server.return_value = server

        db = MagicMock()
        db.sql_query.return_value = True, "", []
        createdb.return_value = db

        test_im = powermanager()

        test_im._clues_daemon = MagicMock()
        node = MagicMock()
        node.enabled = True
        node.state = Node.IDLE
        test_im._clues_daemon.get_node.return_value = node
        res = test_im._get_vms()
        self.assertEqual(len(res), 1)
        self.assertEqual(res['node-1'].vm_id, '1')
        self.assertEqual(res['node-1'].last_state, "configured")
        self.assertEqual(res['node-1'].timestamp_seen, 100)
        self.assertEqual(res['node-1'].timestamp_created, 100)

        now.return_value = 200
        res2 = test_im._get_vms()
        self.assertEqual(res2['node-1'], res['node-1'])
        self.assertEqual(res2['node-1'].timestamp_seen, 200)

        # Test the node is unconfigured
        radl = """system wn (
            net_interface.0.dns_name = 'node-#N#' and
            state = 'unconfigured'
        )"""
        server.GetVMInfo.return_value = (True, radl)
        server.GetVMContMsg.return_value = (True, "ERROR!")
        res = test_im._get_vms()
        # Recover must be called
        self.assertEqual(recover.call_count, 1)
        self.assertEqual(recover.call_args_list[0][0], ("node-1", node))

        node.enabled = False
        res = test_im._get_vms()
        # Recover must NOT be called again in this case
        self.assertEqual(recover.call_count, 1)

        node.enabled = True
        radl = """system wn (
            net_interface.0.dns_name = 'node-#N#' and
            state = 'unconfigured' and
            ec3_additional_vm = 'true'
        )"""
        server.GetVMInfo.return_value = (True, radl)
        res = test_im._get_vms()
        # Recover must NOT be called again in this case
        self.assertEqual(recover.call_count, 1)

    @patch("cluesplugins.im.powermanager._read_auth_data")
    @patch("cluesplugins.im.powermanager._get_inf_id")
    @patch("cluesplugins.im.powermanager._get_server")
    @patch("cpyutils.db.DB.create_from_string")
    @patch("cpyutils.eventloop.now")
    def test_get_radl(self, now, createdb, get_server, get_inf_id, read_auth):
        get_inf_id.return_value = "infid"
        read_auth.return_value = {'type': 'InfrastructureManager', 'username': 'user', 'password': 'pass'}
        now.return_value = 100

        server = MagicMock()
        server.GetInfrastructureInfo.return_value = (True, ['0', '1'])
        radl = """system wn (
            net_interface.0.dns_name = 'node-#N#' and
            ec3_class = 'wn'
        )"""
        server.GetVMInfo.return_value = (True, radl)
        infra_radl = """system wn (
            net_interface.0.dns_name = 'node-#N#'
        )"""
        server.GetInfrastructureRADL.return_value = (True, infra_radl)
        get_server.return_value = server

        db = MagicMock()
        db.sql_query.return_value = True, "", []
        createdb.return_value = db

        test_im = powermanager()
        res = test_im._get_radl('node-2')

        radl_res = parse_radl(res)
        self.assertEqual(radl_res.systems[0].name, 'node-2')
        self.assertEqual(radl_res.systems[0].getValue('net_interface.0.dns_name'), 'node-2')
        self.assertEqual(radl_res.systems[0].getValue('ec3_class'), 'wn')
        self.assertEqual(radl_res.deploys[0].id, 'node-2')
        self.assertEqual(radl_res.deploys[0].vm_number, 1)

    @patch("cluesplugins.im.powermanager._get_radl")
    @patch("cluesplugins.im.powermanager._get_vms")
    @patch("cluesplugins.im.powermanager._read_auth_data")
    @patch("cluesplugins.im.powermanager._get_inf_id")
    @patch("cluesplugins.im.powermanager._get_server")
    @patch("cpyutils.db.DB.create_from_string")
    def test_power_on(self, createdb, get_server, get_inf_id, read_auth, get_vms, get_radl):
        get_inf_id.return_value = "infid"
        read_auth.return_value = {'type': 'InfrastructureManager', 'username': 'user', 'password': 'pass'}

        server = MagicMock()
        server.AddResource.return_value = (True, ['2'])
        get_server.return_value = server

        db = MagicMock()
        db.sql_query.return_value = True, "", []
        createdb.return_value = db

        test_im = powermanager()

        get_vms.return_value = {'node-1': None}
        radl = """system node-1 (
            net_interface.0.dns_name = 'node-#N#'
        )"""
        get_radl.return_value = radl

        res, nname = test_im.power_on('node-1')
        self.assertFalse(res)
        self.assertEqual(nname, 'node-1')
        res, nname = test_im.power_on('node-2')
        self.assertTrue(res)

    @patch("cluesplugins.im.powermanager._get_inf_id")
    @patch("cluesplugins.im.powermanager._get_server")
    @patch("cluesplugins.im.powermanager._read_auth_data")
    @patch("cpyutils.db.DB.create_from_string")
    @patch("cpyutils.eventloop.now")
    def test_power_off(self, now, createdb, read_auth, get_server, get_inf_id):
        get_inf_id.return_value = "infid"
        read_auth.return_value = {'type': 'InfrastructureManager', 'username': 'user', 'password': 'pass'}
        now.return_value = 100

        server = MagicMock()
        server.StopVM.return_value = (True, ['2'])
        server.RemoveResource.return_value = (True, ['2'])
        get_server.return_value = server

        db = MagicMock()
        db.sql_query.return_value = True, "", []
        createdb.return_value = db

        test_im = powermanager()
        vm = MagicMock()
        vm.vm_id = "vmid"
        radl = """system node-1 (
            net_interface.0.dns_name = 'node-#N#'
        )"""
        vm.radl = parse_radl(radl)
        test_im._mvs_seen = {'node-1': vm}
        res, nname = test_im.power_off('node-1')
        self.assertTrue(res)
        self.assertEqual(nname, 'node-1')
        self.assertEqual(server.RemoveResource.call_count, 1)
        self.assertEqual(server.StopVM.call_count, 0)

        radl = """system node-1 (
            net_interface.0.dns_name = 'node-#N#' and
            ec3_reuse_nodes = 'true'
        )"""
        vm.radl = parse_radl(radl)
        res, nname = test_im.power_off('node-1')
        self.assertTrue(res)
        self.assertEqual(nname, 'node-1')
        self.assertEqual(server.StopVM.call_count, 1)
        self.assertEqual(server.RemoveResource.call_count, 1)

    @patch("cluesplugins.im.uuid1")
    @patch("cluesplugins.im.powermanager._get_inf_id")
    @patch("cluesplugins.im.powermanager._get_server")
    @patch("cluesplugins.im.powermanager._read_auth_data")
    @patch("cluesplugins.im.powermanager._get_vms")
    @patch("cpyutils.db.DB.create_from_string")
    @patch("cpyutils.eventloop.now")
    def test_lifecycle(self, now, createdb, get_vms, read_auth, get_server, get_inf_id, uuid1):
        now.return_value = 100

        vm = MagicMock()
        vm.vm_id = "vmid"
        radl = """system node-1 (
            net_interface.0.dns_name = 'node-#N#' and
            state = 'configured'
        )"""
        vm.radl = parse_radl(radl)
        vm.timestamp_recovered = 0
        get_vms.return_value = {'node-1': vm}

        db = MagicMock()
        db.sql_query.return_value = True, "", []
        createdb.return_value = db

        test_im = powermanager()
        test_im._clues_daemon = MagicMock()
        monit = MagicMock()
        node = MagicMock()
        node.name = "node-1"
        node.state = Node.IDLE
        node.enabled = True
        node.timestamp_state = 100
        monit.nodelist = [node]
        test_im._clues_daemon.get_monitoring_info.return_value = monit

        # 1st case: all ok
        test_im.lifecycle()
        self.assertEqual(vm.recovered.call_count, 0)

        # 2nd case: node is OFF in clues
        node.state = Node.OFF
        node.timestamp_state = 60
        test_im.lifecycle()
        self.assertEqual(vm.recovered.call_count, 1)

        # 3rd case: golden image
        node.state = Node.IDLE
        radl = """system node-1 (
            net_interface.0.dns_name = 'node-#N#' and
            state = 'configured' and
            ec3_golden_images = 1 and
            ec3_class = 'wn' and
            disk.0.os.credentials.password = 'pass'
        )"""
        vm.radl = parse_radl(radl)
        auth = {'type': 'InfrastructureManager', 'username': 'user', 'password': 'pass'}
        read_auth.return_value = auth
        get_inf_id.return_value = "infid"
        server = MagicMock()
        server.CreateDiskSnapshot.return_value = (True, 'image')
        get_server.return_value = server
        uuid1.return_value = "uuid"

        test_im._store_golden_image = MagicMock()

        test_im.lifecycle()
        self.assertEqual(vm.recovered.call_count, 1)
        self.assertEqual(server.CreateDiskSnapshot.call_count, 1)
        self.assertEqual(server.CreateDiskSnapshot.call_args_list[0][0], ('infid', 'vmid', 0, 'im-uuid', True, auth))
        self.assertEqual(test_im._store_golden_image.call_args_list[0][0], ('wn', 'image', 'pass'))


if __name__ == "__main__":
    unittest.main()
