#!/usr/bin/env python
import sys
import clues.configserver as configserver
import clueslib.configlib
import cpyutils.eventloop
import clueslib.platform
import cpyutils.log
import clues.configcli as configcli
import collections
# import simdatacenter
import random
import clues.cluesserver as cluesserver

import cluessim as simdatacenter


try:
    raw_input
except:
    raw_input=input

cpyutils.log.include_timestamp(True)
cpyutils.log.Log.setup()
_LOGGER = cpyutils.log.Log("SIM")
_LOGGER.setup_log(cpyutils.log.logging.DEBUG)

class Event():
  def __init__(self, t):
    self.t = t
  def do(self, lrms, powermanager, nodepool):
    raise Exception("not implemented")
  def schedule(self, lrms, powermanager, nodepool):
    raise Exception("not implemented")

class LaunchJob(Event):
  # This is a job launcher. It is used to program a "qsub" of a job. This class is able to emulate the workflow of CLUES: 
  #   1. creating a request
  #   2. waiting for it to be attended
  #   3. submitting the job to que queue (once the request is attended or after a timeout)
  #
  #   @param t is the time in which the qsub should happen
  #   @param job is the job to submit
  #   @param makerequest create the request and wait for it prior to executing the "qsub" or not
  def __init__(self, t, job, makerequest = True):
    Event.__init__(self, t)
    self._job = job
    self.timeout = 180
    self.proxy = configcli.get_clues_proxy_from_config()
    self.req_id = None
    self.sec_info = configcli.config_client.CLUES_SECRET_TOKEN
    self.makerequest = makerequest

  def _launch_job(self, lrms):
    if self.makerequest:
      succeed, req_id = self.proxy.request_create(self.sec_info, self._job.cores, self._job.memory, self._job.nodecount, self._job.nodecount, "")
      if succeed:
        self.req_id = req_id
        self._wait_job_and_launch(lrms)
      else:
        _LOGGER.error("could not launch job %s" % self._job)
    else:
      lrms.qsub(self._job, "new job")

  def _wait_job_and_launch(self, lrms):
    succeed, req_in_queue = self.proxy.request_pending(self.sec_info, self.req_id)
    if not succeed:
      raise Exception(_LOGGER.error("could check the state of the request %s" % req_in_queue))
    else:
      if (not req_in_queue) or (self.timeout <= 0):
        lrms.qsub(self._job, "request id: %s" % self.req_id)
      else:
        self.timeout -= 0.5
        cpyutils.eventloop.get_eventloop().add_event(cpyutils.eventloop.Event(0.5, description = "wait for request %s" % self.req_id, callback = self._wait_job_and_launch, parameters = [lrms], mute = True))

  def schedule(self, lrms, powermanager, nodepool):
    cpyutils.eventloop.get_eventloop().add_event(cpyutils.eventloop.Event(self.t, description = "submit job", callback = self._launch_job, parameters = [lrms], threaded_callback = False))

  def __repr__(self):
    return "@%d: (Job: %.2f cores, %.2f mem, %.2f sec)" % (self.t, self._job.cores, self._job.memory, self._job.seconds)

class PoweronNode(Event):
  def __init__(self, t, nodename):
    Event.__init__(self, t)
    self._nodename = nodename
  def do(self, lrms, powermanager, nodepool):
    powermanager.power_on(self._nodename)

class PoweroffNode(Event):
  def __init__(self, t, nodename):
    Event.__init__(self, t)
    self._nodename = nodename
  def do(self, lrms, powermanager, nodepool):
    powermanager.power_off(self._nodename)

class AddNode(Event):
  def __init__(self, t, node):
    Event.__init__(self, t)
    self._node = node
  def do(self, lrms, powermanager, nodepool):
    nodepool.add(self._node)
  def __repr__(self):
    return "@%s: (Node: %s, %.2f cores, %.2f mem)" % (self.t, self._node.name, self._node.cores, self._node.total_memory)

class RemoveNode(Event):
  def __init__(self, t, nodename):
    Event.__init__(self, t)
    self._nodename = nodename

def get_value(fields, pos, name, default = None, to_float = True):
  value = default
  if len(fields) > pos:
    if fields[pos] != "":
      value = fields[pos]
      if to_float:
        try:
          value = float(value)
        except:
          value = default
          _LOGGER.error("invalid value for %s" % name)
  return value

def parse_command(t, fields):
  max_slots = 4
  max_memory = 4096

  command = fields[1]
  if command == "node":
    _LOGGER.debug("creating a node")
    name = get_value(fields, 2, "name", "node", to_float=False)
    cores = get_value(fields, 3, "cores")
    memory = get_value(fields, 4, "memory")
    count = get_value(fields, 5, "count", 1)
    if cores is None:
      cores = random.choice([1,2,4,8,16])
    if memory is None:
      memory = random.choice([512,1024,2048,4096,8192])
    if count != 1:
      count = int(count)
    if count == 1:
      return [AddNode(t, simdatacenter.Node(cores, memory, name))]
    else:
      nodes = []
      for i in range(0, count):
        nodes.append(AddNode(t, simdatacenter.Node(cores, memory, "%s%.02d" % (name, i))))
      return nodes

    AddNode(t, simdatacenter.Node(cores, memory, name))
  elif command == "job":
    slots = get_value(fields, 2, "slots")
    memory = get_value(fields, 3, "memory")
    seconds = get_value(fields, 4, "seconds")
    nodecount = get_value(fields, 5, "nodecount", 1)
    control = ""
    if len(fields)>5: control = fields[6]
    if slots is None:
      slots = random.randint(1, max_slots)
    if memory is None:
      memory = random.randint(1, max_memory)
    if seconds is None:
      seconds = random.randint(1, max_memory)
    if control is None:
      control = ""
    control = control.split(',')
    makerequest = True
    for ckey in control:
      if ckey != '':
        if ckey == 'norequest':
          makerequest = False
        else:
          _LOGGER.error('invalid control key %s' % ckey)
          sys.exit(-1)
    if len(fields) > 6:
      _LOGGER.warning("ignoring other arguments for job")

    _LOGGER.debug("creating a job: slots:%d memory:%d nodecount:%d duration: %d" % (slots, memory, nodecount, seconds))
    return [LaunchJob(t, simdatacenter.Job(slots, memory, seconds, nodecount), makerequest)]
  else:
    print("do not know what to do with command %s" % command)
  
if __name__ == "__main__":
  from optparse import OptionParser
  parser = OptionParser()
  parser.add_option("-f", "--simulation-file", dest="SIM_FILE", default=None, help="file to load simulation data")
  parser.add_option("-d", "--database-file", dest="OUT_FILE", default=None, help="the database to use (in which the simulation is stored)")
  parser.add_option("-s", "--simmulated-time", dest="RT_MODE", action="store_false", default=True, help="runs app in real time")
  parser.add_option("-r", "--random-seed", dest="RANDOM_SEED", default=None, help="the seed to initialize the random number generator")
  parser.add_option("-t", "--truncate-database", dest="TRUNCATE", default=False, action="store_true", help="WARNING: truncates the database file (for simulation purposes only)")
  parser.add_option("-F", "--force-truncate", dest="FORCETRUNCATE", default=False, action="store_true", help="force confirmation for -t flag")
  parser.add_option("-n", "--no-end", dest="END", default=True, action="store_false", help="do not end simulation (useful to have a running platform to monitor)")

  (options, args) = parser.parse_args()

  if options.SIM_FILE is None:
    print("nothing to do")
    sys.exit(0)

  if options.OUT_FILE is None:
    print("please set the database file")
    print("(will not use the configuration file to avoid overwritting data)")
    sys.exit(1)

  if options.TRUNCATE:
    print("WARNING: this option will remove your existing data in file %s" % options.OUT_FILE)
    if not options.FORCETRUNCATE:
      confirmation = raw_input("Please confirm that you know what you are doing (yes/N)")
      if confirmation != "yes":
        print("aborting")
        sys.exit(0)
    try:
      filehandler = open(options.OUT_FILE, "w")
      filehandler.truncate()
      filehandler.close()
    except:
      pass

  for i in [ "-f", "--simulation-file", "-d", "--database-file"]:
    while sys.argv.count(i):
      pos = sys.argv.index(i)
      del sys.argv[pos+1]
      del sys.argv[pos]

  for i in [ "-t", "--truncate-database", "-F", "--force-truncate", "-tF", "-Ft", "-n", "--no-end" ]:
    while sys.argv.count(i):
      pos = sys.argv.index(i)
      del sys.argv[pos]

  with open(options.SIM_FILE) as jobfile:
    content = jobfile.readlines()
  content = [x.strip() for x in content]

  time_started = False
  previous_actions = []
  events = []

  for line in content:
    line = line.split('#')[0]
    fields = line.split(";")
    if len(fields) < 2:
      # print("ignoring line")
      continue

    T=fields[0]
    command=fields[1]
    if T == "":
      if time_started:
        print("missing time")
      else:
        print("%s happened before anything start" % command)
    else:
      time_started = True
      T=int(T)

    fields[0]=T
    eventlist = parse_command(T, fields)
    if eventlist is not None:
      if not time_started:
        previous_actions = previous_actions + eventlist
      else:
        events = events + eventlist

  nodepool = simdatacenter.NodePool()
  lrms = simdatacenter.LRMS_FIFO(nodepool)
  powermanager = simdatacenter.PowerManager_dummy(nodepool)

  for action in previous_actions:
    action.do(lrms, powermanager, nodepool)

  def queue_jobs(lrms):
    if options.END:
      _LOGGER.debug("setting end after 10 seconds without new events")
      cpyutils.eventloop.get_eventloop().set_endless_loop(False)
      cpyutils.eventloop.get_eventloop().limit_time_without_new_events(10)
      for event in events:
        event.schedule(lrms, powermanager, nodepool)

  print(nodepool)
  print("-"*100, "\n", cpyutils.eventloop.get_eventloop())
  
  configserver._CONFIGURATION_CLUES.DB_CONNECTION_STRING = "sqlite://%s" % options.OUT_FILE
  clueslib.configlib._CONFIGURATION_MONITORING.PERIOD_MONITORING_JOBS = 10
  clueslib.configlib._CONFIGURATION_MONITORING.COOLDOWN_SERVED_REQUESTS = 30
  configserver.config_scheduling.SCHEDULER_CLASSES = "clueslib.schedulers.CLUES_Scheduler_PowOn_Requests,clueslib.schedulers.CLUES_Scheduler_Reconsider_Jobs, clueslib.schedulers.CLUES_Scheduler_PowOff_IDLE"
  cluesserver.main_loop(lrms, powermanager, queue_jobs, [lrms])
