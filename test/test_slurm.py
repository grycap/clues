import unittest
import os
from mock import MagicMock, patch
import sys

sys.path.append("..")
sys.path.append(".")

from cpyutils.evaluate import TypedClass, TypedList
from clueslib.node import NodeInfo
from clueslib.request import Request
from cluesplugins import slurm


def read_file(file_name):
    tests_path = os.path.dirname(os.path.abspath(__file__))
    abs_file_path = os.path.join(tests_path, file_name)
    return open(abs_file_path, 'r').read()


class TestSLURMPlugin(unittest.TestCase):

    def __init__(self, *args):
        """Init test class."""
        unittest.TestCase.__init__(self, *args)

    def test_infer_clues_node_state(self):
        self.assertEqual(slurm.infer_clues_node_state('IDLE'), NodeInfo.IDLE)
        self.assertEqual(slurm.infer_clues_node_state('FAIL'), NodeInfo.ERROR)
        self.assertEqual(slurm.infer_clues_node_state('FAILING'), NodeInfo.ERROR)
        self.assertEqual(slurm.infer_clues_node_state('ERROR'), NodeInfo.ERROR)
        self.assertEqual(slurm.infer_clues_node_state('NoResp'), NodeInfo.ERROR)
        self.assertEqual(slurm.infer_clues_node_state('DOWN'), NodeInfo.OFF)
        self.assertEqual(slurm.infer_clues_node_state('DRAIN'), NodeInfo.OFF)
        self.assertEqual(slurm.infer_clues_node_state('MAINT'), NodeInfo.OFF)
        self.assertEqual(slurm.infer_clues_node_state('ALLOCATED'), NodeInfo.USED)
        self.assertEqual(slurm.infer_clues_node_state('ALLOC'), NodeInfo.USED)
        self.assertEqual(slurm.infer_clues_node_state('COMPLETING'), NodeInfo.USED)
        self.assertEqual(slurm.infer_clues_node_state('MIXED'), NodeInfo.USED)

    def test_infer_clues_job_state(self):
        self.assertEqual(slurm.infer_clues_job_state('PENDING'), Request.PENDING)
        self.assertEqual(slurm.infer_clues_job_state('RUNNING'), Request.ATTENDED)
        self.assertEqual(slurm.infer_clues_job_state('COMPLETED'), Request.ATTENDED)

    @patch('cluesplugins.slurm.runcommand')
    def test_get_partition(self, runcommand):
        lrms = slurm.lrms()
        runcommand.return_value = True, read_file('./test-files/slurm_partitions.txt').encode()
        self.assertEqual(lrms._get_partition('vnode-1'), ['debug'])

    def test_init_lrms_empty(self):
        lrms = slurm.lrms()
        self.assertEqual(lrms._server_ip, 'slurmserverpublic')

    def test_init_lrms(self):
        lrms = slurm.lrms('test_ip')
        self.assertEqual(lrms._server_ip, 'test_ip')

    @patch('cluesplugins.slurm.runcommand')
    def test_get_nodeinfolist(self, runcommand):
        lrms = slurm.lrms()
        runcommand.side_effect = [(True, read_file('./test-files/slurm_nodes.txt').encode()),
                                  (True, read_file('./test-files/slurm_partitions.txt').encode()),
                                  (True, read_file('./test-files/slurm_partitions.txt').encode())]
        node_info = lrms.get_nodeinfolist()
        self.assertEqual(len(node_info), 2)
        self.assertEqual(node_info['vnode-1'].name, 'vnode-1')
        self.assertEqual(node_info['vnode-1'].slots_count, 1)
        self.assertEqual(node_info['vnode-1'].slots_free, 1)
        self.assertEqual(node_info['vnode-1'].memory_total, 1073741824.0)
        self.assertEqual(node_info['vnode-1'].memory_free, 1073741824.0)
        self.assertEqual(node_info['vnode-1'].state, NodeInfo.IDLE)
        self.assertEqual(len(node_info['vnode-1'].keywords), 2)
        self.assertEqual(node_info['vnode-1'].keywords['hostname'], TypedClass.auto('vnode-1'))
        self.assertEqual(node_info['vnode-1'].keywords['queues'], TypedList([TypedClass.auto('debug')]))
        self.assertEqual(node_info['vnode-2'].name, 'vnode-2')
        self.assertEqual(len(node_info['vnode-2'].keywords), 2)
        self.assertEqual(node_info['vnode-2'].keywords['queues'], TypedList([TypedClass.auto('debug')]))
        self.assertEqual(node_info['vnode-2'].state, NodeInfo.OFF)

    @patch('cluesplugins.slurm.runcommand')
    def test_get_jobinfolist(self, runcommand):
        lrms = slurm.lrms()
        runcommand.return_value = True, read_file('./test-files/slurm_jobs.txt').encode()
        jobs_info = lrms.get_jobinfolist()
        self.assertEqual(len(jobs_info), 2)
        self.assertEqual(jobs_info[0].job_id, '2')
        self.assertEqual(jobs_info[0].job_nodes_ids, ['vnode-1'])
        self.assertEqual(jobs_info[0].resources.resources.slots, 1)
        self.assertEqual(jobs_info[0].resources.resources.memory, 1073741824.0)
        self.assertEqual(jobs_info[0].resources.resources.requests, ['"debug" in queues'])
        self.assertEqual(jobs_info[1].job_id, '3')

if __name__ == '__main__':
    unittest.main()
