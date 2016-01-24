import sys

sys.path.append("/home/calfonso/Programacion/git/")
sys.path.append("/home/calfonso/Programacion/git/clues/")

import cpyutils.eventloop
import clueslib.platform
import cpyutils.log

_LOGGER = cpyutils.log.Log("SIM")
cpyutils.log.Log.setup()

'''
class LRMS:
    def __init__(self, _id):
        self._clues_daemon = None
        self._id = _id
    def get_id(self):
        return self._id
    def get_nodeinfolist(self):
        node_list = {}
        return {}
    def _attach_clues_system(self, clues_daemon):
        self._clues_daemon = clues_daemon
    def get_jobinfolist(self):
        return []    
    def power_off(self, nname):
        return True
    def power_on(self, nname):
        return True
    def lifecycle(self):
        return True

class PowerManager:
    def __init__(self):
        pass
    def _attach_clues_system(self, clues_daemon):
        self._clues_daemon = clues_daemon
    def power_on(self, nname):
        return False, nname
    def power_off(self, nname):
        return False, nname
    def lifecycle(self):
        return True
    def recover(self, nname):
        return False
'''

class Job:
    __current_id = 0
    INIT = 0
    RUNNING = 1
    END = 2
    ERR = 3
    ABORT = 4
    
    STR2STATE = { INIT : 'init', RUNNING: 'running', END: 'finish', ERR: 'error', ABORT: 'abort'}
    
    @staticmethod
    def get_id():
        Job.__current_id += 1
        return Job.__current_id

    def __init__(self, cores, memory, seconds, nodecount = 1, name = None):
        if name is None:
            name = "job%.04d" % Job.get_id()
        self.name = name
        self.cores = cores
        self.memory = memory
        self.nodecount = nodecount
        self.seconds = seconds
        self.timestamp_creation = cpyutils.eventloop.now()
        self.timestamp_start = None
        self.timestamp_finish = None
        self.state = Job.INIT
        self.timestamp_assigned = None
        self.nodes_assigned = []
        
    def clone(self):
        j = Job(self.cores, self.memory, self.seconds, self.nodecount, self.name)
        j.timestamp_creation = self.timestamp_creation
        j.timestamp_start = self.timestamp_start
        j.timestamp_finish = self.timestamp_finish
        j.timestamp_assigned = self.timestamp_assigned
        j.state = self.state
        j.nodes_assigned = self.nodes_assigned[:]
        return j
        
    def _ensure_started(self):
        if self.timestamp_start is None:
            raise Exception("job %s has not been started yet" % self.name)
        
    def _ensure_running(self):
        self._ensure_started()
        if self.state != self.RUNNING:
            raise Exception("job %s is not running" % self.name)
        
    def _ensure_not_started(self):
        if self.timestamp_start is not None:
            raise Exception("job %s has already been started" % self.name)

    def pending_nodecount(self):
        return self.nodecount - len(self.nodes_assigned)
        
    def assign(self, node_list):
        self._ensure_not_started()
        self.timestamp_assigned = cpyutils.eventloop.now()
        for n_id in node_list:
            self.nodes_assigned.append(n_id)

    def can_start(self):
        if len(self.nodes_assigned) < self.nodecount: return False
        return True

    def disassign(self, node_list):
        for n_id in node_list:
            if n_id not in self.nodes_assigned:
                raise Exception("job %s not assigned to node %s" % (self.name, n_id))
            self.nodes_assigned.remove(n_id)
        
    def start(self):
        self._ensure_not_started()
        self.timestamp_start = cpyutils.eventloop.now()
        self.state = Job.RUNNING
        cpyutils.eventloop.get_eventloop().add_event(self.seconds, desc = "job %s finished" % self.name)
        cpyutils.eventloop.get_eventloop().add_event(self.seconds + 1, desc = "job %s finished + sched" % self.name, stealth = True)
        # TODO: add event to finalize the job? maybe is better to wait for the LRMS loop

    def finish(self):
        self._ensure_running()
        if self.timestamp_finish is not None:
            raise Exception("job %s has already finished" % self.name)
        self.timestamp_finish = cpyutils.eventloop.now()
        self.state = Job.END

    def abort(self):
        self.finish()
        self.state = Job.ABORT

    def fail(self):
        self.finish()
        self.state = Job.ERR
    
    def execution_finished(self):
        if self.timestamp_start is None:
            return False
        t = cpyutils.eventloop.now()
        if (t - self.timestamp_start) > self.seconds:
            return True
        return False
    
    def remaining_time(self):
        if self.timestamp_start is None:
            return None

        t = cpyutils.eventloop.now()
        return self.seconds - (t - self.timestamp_start)
    
    def __str__(self):
        r = self.remaining_time()
        if r is None:
            r_str = None
        else:
            r_str = "%.2f" % r
        return "[JOB (%s)](%8s) %.1f cores, %.1f memory, %.1f seconds (remaining: %s)" % (self.name, self.STR2STATE[self.state], self.cores, self.memory, self.seconds, r_str)

class Node:
    __current_id = -1
    ON = 0
    OFF = 1
    POW_ON = 2
    POW_OFF = 3
    ERR = 4
    
    @staticmethod
    def get_id():
        Node.__current_id += 1
        return Node.__current_id
    
    def __init__(self, cores, memory, name = None):
        if name is None:
            name = "node%.02d" % Node.get_id()
        self.name = name
        self.total_cores = cores
        self.total_memory = memory
        self.state = Node.ON
        self.cores = cores
        self.memory = memory
        
    def clone(self):
        n = Node(self.total_cores, self.total_memory, self.name)
        n.memory = self.memory
        n.cores = self.cores
        n.state = self.state
        return n

    def copy(self):
        return Node(self.cores, self.memory)

    def poweroff(self):
        self.state = Node.POW_OFF
        cpyutils.eventloop.get_eventloop().add_event(5, desc = "node %s powered off" % self.name, callback = self._poweroff)
        
    def _poweroff(self):
        self.state = Node.OFF
        
    def meets_requirements(self, j):
        if self.state != Node.ON: return False
        if self.cores < j.cores: return False
        if self.memory < j.memory: return False
        return True
        
    def assign_job(self, j):
        if self.state != Node.ON:
            raise Exception("cannot assign job %s node %s because node is off" % (j.name, self.name))
        self.memory -= j.memory
        self.cores -= j.cores
        
    def disassign_job(self, j):
        self.memory += j.memory
        self.cores += j.cores
        # j.disassign([self.name])
        
    def __str__(self):
        return "[NODE (%s)] %.1f cores, %.1f memory" % (self.name, self.cores, self.memory)
    
class NodePool():
    @staticmethod
    def create_pool(node, count):
        np = NodePool()
        for n in range(0, count):
            np.add(node.copy())
        return np
    
    def __init__(self):
        self.nodes = {}

    def clone(self):
        n_dict = {}
        for n_id, n in self.nodes.items():
            n_dict[n_id] = n.clone()
        np = NodePool()
        np.nodes = n_dict
        return np

    def __iter__(self):
        for _,item in self.nodes.items():
            yield item
        
    def add(self, node):
        if node.name in self.nodes:
            raise Exception("node %s already exists" % node.name)
        self.nodes[node.name] = node
        
    def get_node(self, node_name):
        if node_name not in self.nodes:
            raise Exception("node %s does not exist" % node_name)
        return self.nodes[node_name]

    def __getitem__(self, node_name):
        return self.get_node(node_name)
        
    def __str__(self):
        retval = ""
        for n_id in sorted(self.nodes.keys()):
            n = self.nodes[n_id]
            retval = "%s%s\n" % (retval, str(n))
        return retval

class LRMS_FIFO(clueslib.platform.LRMS):
    def __init__(self, nodepool):
        self.nodepool = nodepool
        self.jobs = {}
        self.jobs_queue = []
        self.jobs_running = []
        self.job2nodes = {}
        self.sched_period = 1
        self.sched_last = cpyutils.eventloop.now()
        self.jobs_ended = []

    def qsub(self, job):
        if job.name in self.jobs:
            raise Exception("job %s already in the queue" % job.name)
        self.jobs[job.name] = job
        self.jobs_queue.append(job.name)
        
    def __str__(self):
        return "%s\n%s\n%s\n%s" % ("Node Pool\n" + "-"*50, str(self.nodepool), "Queue\n" + "-"*50, self.qstat())
    
    def qstat(self):
        retval = ""
        
        jobs = sorted([x for x in self.jobs.itervalues()], key = lambda x: x.timestamp_creation)
        for j in jobs:
            retval = "%s%s\n" % (retval, self.job2str(j))
        
        #for j in self.jobs_ended:
        #    retval = "%s%s\n" % (retval, self.job2str(j))
        #for j in self.jobs_running:
        #    retval = "%s%s\n" % (retval, self.job2str(self.jobs[j]))
        #for j in self.jobs_queue:
        #    retval = "%s%s\n" % (retval, self.job2str(self.jobs[j]))
        #    # retval = "%s%s\n" % (retval, str(self.jobs[j]))
        return retval
    
    def start_jobs(self):
        ''' this function checks whether the jobs have start or not
            * this simmulates the 'stage in' and other phases.
        '''
        for j_id, nodelist in self.job2nodes.items():
            j = self.jobs[j_id]
            if j.state == Job.INIT:
                if j.can_start():
                    j.start()
                    _LOGGER.debug("job %s started" % j.name)
    
    def purge_jobs(self):
        for j_id, nodelist in self.job2nodes.items():
            j = self.jobs[j_id]
            if j.execution_finished():
                _LOGGER.debug("job %s has finished its execution" % j.name)
                j.finish()
                for n_id in nodelist:
                    self.nodepool[n_id].disassign_job(j)
                    _LOGGER.debug("job %s dissasigned from node %s" % (j.name, n_id))
                del self.job2nodes[j_id]
                self.jobs_ended.append(j)
                self.jobs_running.remove(j_id)
    
    def sched(self):
        for j_id in self.jobs_queue[:]:
            j = self.jobs[j_id]
            j_clone = j.clone()
            nodepool = self.nodepool.clone()
            nodes_assigned = []
            while j_clone.pending_nodecount() > 0:
                assigned = False
                for n in nodepool:
                    if n.meets_requirements(j_clone):
                        n.assign_job(j_clone)
                        j_clone.assign([n.name])
                        nodes_assigned.append(n.name)
                        assigned = True
                        _LOGGER.debug("job %s can be assigned to node %s" % (j_id, n.name))
                        break
                if not assigned:
                    _LOGGER.debug("could not find enough nodes for job %s" % j_id)
                    return
                    
            if j_clone.pending_nodecount() == 0:
                for n_id in nodes_assigned:
                    self.nodepool[n_id].assign_job(j)
                self.job2nodes[j_id] = nodes_assigned
                j.assign(nodes_assigned)

                _LOGGER.debug("job %s assigned to nodes %s" % (j_id, nodes_assigned))
                self.jobs_queue.remove(j_id)
                self.jobs_running.append(j_id)
            else:
                _LOGGER.debug("could not assign job %s to nodes" % (j_id))
                return
    
    def lifecycle(self, force_sched = False):
        t = cpyutils.eventloop.now()
        if force_sched or ((t - self.sched_last) >= self.sched_period):
            self.purge_jobs()
            self.sched_last = t
            self.sched()
            self.start_jobs()
        print self.qstat()
        
    def job2str(self, j):
        j2s = {Job.INIT:'Q', Job.RUNNING:'R', Job.ABORT: 'A', Job.END:'F', Job.ERR:'E'}
        retval = "%10s" % (j.name)
        retval = "%s - %s" % (retval, j2s[j.state])
        retval = "%s - %2s %5s" % (retval, j.cores, j.memory)
        return retval

if __name__ == '__main__':
    # cpyutils.eventloop.create_eventloop(True)
    cpyutils.eventloop.create_eventloop(False)
    np = NodePool.create_pool(Node(4,4096), 4)
    np['node01'].poweroff()
    np['node02'].poweroff()
    # np['node03'].poweroff()
    # np['node04'].poweroff()
    
    lrms = LRMS_FIFO(np)
    lrms.qsub(Job(2, 512, 3))
    lrms.qsub(Job(2, 512, 6, 2))
    lrms.qsub(Job(2, 512, 2))
    # print lrms
    
    #np = NodePool()
    #n1 = Node(2,1024)
    #j1 = Job(1,512,10)
    #n1.assign_job(j1)
    #np.add(n1)
    #np.add(Node(2,1024))
    #np.add(Node(2,1024))
    #np.add(Node(2,1024))
    #print np
    #print j1
    #
    
    # lrms = LRMS_FIFO("fifo")
    cpyutils.eventloop.get_eventloop().add_periodical_event(1, 0, desc = "lifecycle", callback = lrms.lifecycle, arguments = [], stealth = True)
    cpyutils.eventloop.get_eventloop().loop()
    
