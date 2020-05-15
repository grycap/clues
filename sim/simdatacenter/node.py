import cpyutils.eventloop
import cpyutils.log
import random
import collections

_LOGGER = cpyutils.log.Log("DC-NODE")

class Node:
    __current_id = -1
    ON = 0
    OFF = 1
    POW_ON = 2
    POW_OFF = 3
    ERR = 4
    
    STATE2STR = {ON:'on', OFF:'off', POW_ON:'p-on', POW_OFF: 'p-off', ERR:'err'}
    
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
        self.state = Node.OFF
        self.cores = cores
        self.memory = memory
        self.min_poweron = 5
        self.min_poweroff = 5
        self.max_poweron = 10
        self.max_poweroff = 10
        
    def clone(self):
        n = Node(self.total_cores, self.total_memory, self.name)
        n.memory = self.memory
        n.cores = self.cores
        n.state = self.state
        n.min_poweron = self.min_poweron
        n.max_poweron = self.max_poweron
        n.min_poweroff = self.min_poweroff
        n.max_poweroff = self.max_poweroff
        return n

    def copy(self):
        n = Node(self.cores, self.memory)
        n.min_poweron = self.min_poweron
        n.max_poweron = self.max_poweron
        n.min_poweroff = self.min_poweroff
        n.max_poweroff = self.max_poweroff
        return n        

    def power_off(self):
        self.state = Node.POW_OFF
        elapsed = self.min_poweroff + random.random()* (self.max_poweroff - self.min_poweroff)
        cpyutils.eventloop.get_eventloop().add_event(cpyutils.eventloop.Event(elapsed, description = "node %s powered off (event set in %s, to happen %s later)" % (self.name, cpyutils.eventloop.now(), elapsed), callback = self._power_off))
        _LOGGER.debug("powering off node %s" % self.name)
        return True
        
    def _power_off(self):
        self.state = Node.OFF

    def power_on(self):
        if self.state in [ Node.POW_OFF ]:
            return False
        if self.state in [ Node.ON, Node.POW_ON ]:
            return True
        self.state = Node.POW_ON
        elapsed = self.min_poweron + random.random()* (self.max_poweron - self.min_poweron)
        _LOGGER.debug("node %s will power on in %s" % (self.name, elapsed))
        cpyutils.eventloop.get_eventloop().add_event(cpyutils.eventloop.Event(elapsed, description = "node %s powered on (event set in %s, to happen %s later)" % (self.name, cpyutils.eventloop.now(), elapsed), callback = self._power_on))
        return True
        
    def _power_on(self):
        if random.random() > 0.95:
            _LOGGER.error("node %s failed to power on" % self.name)
            self.state = Node.OFF
        else:
            self.state = Node.ON
        
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
        return "[NODE (%s)] %.1f cores, %.1f memory - %s" % (self.name, self.cores, self.memory, self.STATE2STR[self.state])
    
class NodePool():
    @staticmethod
    def create_pool(node, count):
        np = NodePool()
        for n in range(0, count):
            np.add(node.copy())
        return np
    
    def __init__(self):
        self.nodes = collections.OrderedDict()

    def clone(self):
        n_dict = collections.OrderedDict()
        for n_id, n in list(self.nodes.items()):
            n_dict[n_id] = n.clone()
        np = NodePool()
        np.nodes = n_dict
        return np

    def __iter__(self):
        for _,item in list(self.nodes.items()):
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
    def nodenames(self):
        return list(self.nodes.keys())
