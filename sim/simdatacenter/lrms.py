import cpyutils.eventloop
import clues.clueslib.platform
import collections
from clues.clueslib.node import NodeInfo
from .node import Node
from .job import Job
from clueslib.request import ResourcesNeeded, JobInfo, Request

_LOGGER = cpyutils.log.Log("DC-LRMS")

class LRMS_FIFO(clues.clueslib.platform.LRMS):
    def get_jobinfolist(self):
        _LOGGER.debug("called to JOBINFOLIST")
        _LOGGER.debug("\n%s" % self.qstat())
        jobinfolist = []
        for j_id, job in self.jobs.items():
            if job.state not in [Job.END, Job.ERR, Job.ABORT]:
                resources = ResourcesNeeded(job.cores, job.memory, [], job.nodecount)
                # resources, job_id, nodes_ids
                ji = JobInfo(resources, job.name, [])
                if job.state in [ Job.CREATED, Job.INIT ]:
                    ji.state = Request.PENDING
                else:
                    ji.state = Request.SERVED
                jobinfolist.append(ji)
                _LOGGER.debug("job %s, resources: %s" % (j_id, resources))
        return jobinfolist

    def get_nodeinfolist(self):
        nodeinfolist = collections.OrderedDict()
        for node in self.nodepool:
            n_info = NodeInfo(node.name, node.total_cores, node.cores, node.total_memory, node.memory, {})
            nodeinfolist[n_info.name] = n_info
            if node.state in [ Node.OFF, Node.ERR ]:
                n_info.state = NodeInfo.OFF
            elif node.state in [ Node.ON ]:
                if node.cores != node.total_cores or node.memory != node.total_memory:
                    n_info.state = NodeInfo.USED
                else:
                    n_info.state = NodeInfo.IDLE
            elif node.state in [ Node.POW_ON, Node.POW_OFF ]:
                n_info.state = NodeInfo.OFF
        return nodeinfolist
    
    def __init__(self, nodepool):
        clues.clueslib.platform.LRMS.__init__(self, "LRMS-FIFO")
        self.nodepool = nodepool
        self.jobs = {}
        self.jobs_queue = []
        self.jobs_running = []
        self.job2nodes = {}
        self.sched_period = 1
        self.sched_last = 0 # cpyutils.eventloop.now()
        self.jobs_ended = []

    def qsub(self, job, info):
        if job.name in self.jobs:
            raise Exception("job %s already in the queue" % job.name)
        job.queue()
        self.jobs[job.name] = job
        self.jobs_queue.append(job.name)
        _LOGGER.debug("job %s submitted. %s" % (job.name, info))
        cpyutils.eventloop.get_eventloop().add_event(cpyutils.eventloop.Event(self.sched_period, description = "job %s to be scheduled" % job.name))
        
    def __str__(self):
        return "%s\n%s\n%s\n%s" % ("Node Pool\n" + "-"*50, str(self.nodepool), "Queue\n" + "-"*50, self.qstat())
    
    def qstat(self):
        retval = ""
        
        jobs = sorted([self.jobs[x] for x in self.jobs], key = lambda x: x.timestamp_creation)
        for j in jobs:
            retval = "%s%s\n" % (retval, self.job2str(j))
        
        return retval
    
    def start_jobs(self):
        ''' this function checks whether the jobs have start or not
            * this simmulates the 'stage in' and other phases.
        '''
        n_started = 0
        for j_id, nodelist in self.job2nodes.items():
            j = self.jobs[j_id]
            if j.state == Job.INIT:
                if j.can_start():
                    j.start()
                    _LOGGER.info("job %s started" % (j.name))
                    n_started += 1
        return n_started
    
    def purge_jobs(self):
        n_purged = 0
        for j_id in list(self.job2nodes.keys()):
            nodelist = self.job2nodes[j_id]
            j = self.jobs[j_id]
            if j.execution_finished():
                j.finish()
                # _LOGGER.info("job %s has finished its execution\n%s" % (j.name, j.summary()))

                for n_id in nodelist:
                    self.nodepool[n_id].disassign_job(j)

                del self.job2nodes[j_id]
                self.jobs_ended.append(j)
                self.jobs_running.remove(j_id)
                n_purged += 1

        return n_purged
    
    def sched(self):
        n_assigned = 0
        for j_id in self.jobs_queue[:]:
            j = self.jobs[j_id]
            if j.state == Job.INIT:
                j_clone = j.clone()
                nodepool = self.nodepool.clone()
                nodes_assigned = []
                while j_clone.pending_nodecount() > 0:
                    assigned = False
                    for n in nodepool:
                        # _LOGGER.debug('node: %s %s' % (n.meets_requirements(j_clone) , n))
                        while n.meets_requirements(j_clone) and not assigned:
                            n.assign_job(j_clone)
                            j_clone.assign([n.name])
                            nodes_assigned.append(n.name)
                            assigned = True
                            break
                        if assigned: break
                    if not assigned:
                        # _LOGGER.debug("could not find enough nodes for job %s" % j_id)
                        return n_assigned
                        
                if j_clone.pending_nodecount() == 0:
                    for n_id in nodes_assigned:
                        self.nodepool[n_id].assign_job(j)
                    self.job2nodes[j_id] = nodes_assigned
                    j.assign(nodes_assigned)
    
                    _LOGGER.info("job %s assigned to nodes %s" % (j_id, nodes_assigned))
                    self.jobs_queue.remove(j_id)
                    self.jobs_running.append(j_id)
                    n_assigned += 1
                else:
                    # _LOGGER.debug("could not assign job %s to nodes" % (j_id))
                    return n_assigned
            
        return n_assigned
    
    def lifecycle(self, force_sched = False):
        t = cpyutils.eventloop.now()
        force_sched = True
        if force_sched or ((t - self.sched_last) >= self.sched_period):
            jobs_purged = self.purge_jobs()
            if jobs_purged > 0:
                _LOGGER.debug("%d jobs purged" % (jobs_purged))
            self.sched_last = t
            jobs_assigned = self.sched()
            if jobs_assigned > 0:
                _LOGGER.debug("%d jobs assigned to nodes" % (jobs_assigned))
            self.start_jobs()
            #if jobs_started > 0 or jobs_purged > 0:
            #    _LOGGER.debug("state of the LRMS:\n%s\nstate of the platform:\n%s" % (self.qstat(), str(self.nodepool)))
        return True
        # print self.qstat()
        
    def job2str(self, j):
        j2s = {Job.CREATED: '-', Job.INIT:'Q', Job.RUNNING:'R', Job.ABORT: 'A', Job.END:'F', Job.ERR:'E'}
        retval = "%10s" % (j.name)
        retval = "%s - %s" % (retval, j2s[j.state])
        retval = "%s - %2s %5s" % (retval, j.cores, j.memory)
        if j.name in self.job2nodes:
            retval = "%s : %s" % (retval, "".join(self.job2nodes[j.name]))
        return retval
    
    def summary(self):
        retval = ""
        for j in self.jobs_ended:
            retval = "%s%s\n" % (retval, j.summary())
        return retval