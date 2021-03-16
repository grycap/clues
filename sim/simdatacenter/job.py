import cpyutils.eventloop

def float2str(value):
    if value is None: return "None"
    return "%.2f" % value

class Job:
    __current_id = 0
    CREATED = 5
    INIT = 0
    RUNNING = 1
    END = 2
    ERR = 3
    ABORT = 4
    
    STR2STATE = { CREATED: 'creat', INIT : 'init', RUNNING: 'running', END: 'finish', ERR: 'error', ABORT: 'abort'}
    
    @staticmethod
    def get_id():
        Job.__current_id += 1
        return Job.__current_id

    def __init__(self, cores, memory, seconds, nodecount = 1, name = None):
        self.id = Job.get_id()
        if name is None:
            name = "job%.04d" % self.id
        self.name = name
        self.cores = cores
        self.memory = memory
        self.nodecount = nodecount
        self.seconds = seconds
        self.timestamp_queued = None
        self.timestamp_creation = cpyutils.eventloop.now()
        self.timestamp_start = None
        self.timestamp_finish = None
        self.state = Job.CREATED
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
        
    def queue(self):
        self._ensure_not_started()
        self.timestamp_queued = cpyutils.eventloop.now()
        self.state = Job.INIT
        
    def start(self):
        self._ensure_not_started()
        self.timestamp_start = cpyutils.eventloop.now()
        self.state = Job.RUNNING
        cpyutils.eventloop.get_eventloop().add_event(cpyutils.eventloop.Event(self.seconds, description = "job %s finished" % self.name))

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
    
    def summary(self):
        return "%s, %s, %s, %s, %s" % (str(self), float2str(self.timestamp_creation), float2str(self.timestamp_queued), float2str(self.timestamp_start), float2str(self.timestamp_finish))
