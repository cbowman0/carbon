#!/usr/bin/env python
"""Copyright 2009 Chris Davis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import sys
import imp
from os.path import dirname, join, abspath, exists, split
from optparse import OptionParser
import os, time

# Figure out where we're installed
BIN_DIR = dirname(abspath(__file__))
ROOT_DIR = dirname(BIN_DIR)
CONF_DIR = join(ROOT_DIR, 'conf')
default_relayrules = join(CONF_DIR, 'relay-rules.conf')

# Make sure that carbon's 'lib' dir is in the $PYTHONPATH if we're running from
# source.
LIB_DIR = join(ROOT_DIR, 'lib')
sys.path.insert(0, LIB_DIR)

try:
  from twisted.internet import epollreactor
  epollreactor.install()
except ImportError:
  pass

from twisted.internet import stdio, reactor, defer
from twisted.internet.task import LoopingCall
from twisted.protocols.basic import LineReceiver
from carbon.routers import ConsistentHashingRouter, RelayRulesRouter
from carbon.client import CarbonClientManager
from carbon.conf import settings
from carbon import log, events


option_parser = OptionParser(usage="%prog [options] <host:port:instance> <host:port:instance> ...")
option_parser.add_option('--debug', action='store_true', help="Log debug info to stdout")
option_parser.add_option('--keyfunc', help="Use a custom key function (path/to/module.py:myFunc)")
option_parser.add_option('--datadir', help="Directory containing files to load", default=None)
option_parser.add_option('--datafile-prefix', help="Load files which start with this prefix, ignore the rest", default='')
option_parser.add_option('--pidfile', help="Create a pidfile upon launch", default=None)
option_parser.add_option('--replication', type='int', default=1, help='Replication factor')
option_parser.add_option('--routing', default='consistent-hashing',
  help='Routing method: "consistent-hashing" (default) or "relay"')
option_parser.add_option('--relayrules', default=default_relayrules,
  help='relay-rules.conf file to use for relay routing')

options, args = option_parser.parse_args()

if not args:
  print 'At least one host:port destination required\n'
  option_parser.print_usage()
  raise SystemExit(1)

if options.routing not in ('consistent-hashing', 'relay'):
  print "Invalid --routing value, must be one of:"
  print "  consistent-hashing"
  print "  relay"
  raise SystemExit(1)

destinations = []
for arg in args:
  parts = arg.split(':', 2)
  host = parts[0]
  port = int(parts[1])
  if len(parts) > 2:
    instance = parts[2]
  else:
    instance = None
  destinations.append( (host, port, instance) )

if options.debug:
  log.logToStdout()
  log.setDebugEnabled(True)
  defer.setDebugging(True)

if options.routing == 'consistent-hashing':
  router = ConsistentHashingRouter(options.replication)
elif options.routing == 'relay':
  if exists(options.relayrules):
    config_dir, relayrules = split(abspath(options.relayrules))
    settings.use_config_directory(config_dir)
    router = RelayRulesRouter(relayrules)
  else:
    print "relay rules file %s does not exist" % options.relayrules
    raise SystemExit(1)

client_manager = CarbonClientManager(router)
reactor.callWhenRunning(client_manager.startService)

if options.keyfunc:
  router.setKeyFunctionFromModule(options.keyfunc)

firstConnectAttempts = [client_manager.startClient(dest) for dest in destinations]
firstConnectsAttempted = defer.DeferredList(firstConnectAttempts)


class StdinMetricsReader(LineReceiver):
  delimiter = '\n'

  def lineReceived(self, line):
    #log.msg("[DEBUG] lineReceived(): %s" % line)
    try:
      (metric, value, timestamp) = line.split()
      datapoint = (float(timestamp), float(value))
      assert datapoint[1] == datapoint[1] # filter out NaNs
      while not client_manager.sendDatapoint(metric, datapoint):
        log.msg("Failed to send datapoint, retrying")
        time.sleep(0.1)
    except:
      log.err(None, 'Dropping invalid line: %s' % line)

  def connectionLost(self, reason):
    log.msg('stdin disconnected')
    def startShutdown(results):
      log.msg("startShutdown(%s)" % str(results))
      allStopped = client_manager.stopAllClients()
      allStopped.addCallback(shutdown)
    firstConnectsAttempted.addCallback(startShutdown)

  def startShutdown(results):
    log.msg("startShutdown(%s)" % str(results))
    allStopped = client_manager.stopAllClients()
    allStopped.addCallback(shutdown) 
    firstConnectsAttempted.addCallback(startShutdown)


class FileLoader(object):
  def __init__(self, datadir, datafile_prefix=''):
    self.filelist = []
    self.datadir = datadir
    self.datafile_prefix = datafile_prefix
    self.current_file = None
    self.sleep_for = 0
    self.lines_to_reinject = []

  def backoff(self, cycles):
    self.sleep_for = cycles

  def in_backoff(self):
    if not bool(self.sleep_for):
      return False
    time.sleep(0.01)
    if self.sleep_for > 1:
      self.sleep_for -= 1
      return True
    lines_to_reinject = self.lines_to_reinject
    self.lines_to_reinject = []
    for line in lines_to_reinject:
      self.sendDatapoint(line)
    self.sleep_for -= 1
    return False

  def getNextFile(self):
    while len(self.filelist) == 0:
      self.filelist = [f for f in os.listdir(self.datadir) if (os.path.splitext(f)[1] == '.log') and f.startswith(self.datafile_prefix) ][0:100]
      if len(self.filelist) < 10:
        # relax if there is not that much data
        time.sleep(1)
    return os.path.join(self.datadir, self.filelist.pop())

  def acquireLock(self, file):
    try:
      os.link(file, "%s.lock" % file)
    except:
      return False
    else:
      return True

  def releaseLock(self, file):
    try:
      os.remove("%s.lock" % file)
    except:
      return False
    else:
      return True

  def sendDatapoint(self, line):
    try:
      (metric, value, timestamp) = line.strip().split("\t")
      metric = metric.strip().replace(' ', '_')
      datapoint = (float(timestamp), float(value))
      if not client_manager.sendDatapoint(metric, datapoint):
        if len(self.lines_to_reinject) > 20:
          self.backoff(4)
          log.msg("Failed to send datapoints: backing off and retrying, current metric:%s" %  metric)
        self.lines_to_reinject.append(line)
    except:
      log.err(None, 'Dropping invalid line: %s' % line)

  def loadDataFile(self):
    try:
      if self.current_file is None:
        fname = self.getNextFile()
        if os.path.splitext(fname)[1] != '.log':
          return
        if self.acquireLock(fname):
          log.msg("Feeding %s" % fname)
          try:
            self.current_file = open(fname)
          except:
            #XXX Log this.
            return
        else:
          return
      while True:
        if self.in_backoff():
          return
        line = self.current_file.readline()
        if not line:
          break
        self.sendDatapoint(line)
      self.current_file.close()
      try:
        os.remove(self.current_file.name)
        self.releaseLock(self.current_file.name)
        self.current_file = None
      except OSError:
        self.current_file = None
        pass
    except IndexError:
      log.debug("No data, time.sleeping 1 sec")
      time.sleep(1)

  def sendDatapoint(self, *args, **kwargs):
    raise TypeError, "sendDatapoint is an abstract method"

if options.datadir is None:
  stdio.StandardIO( StdinMetricsReader() )
else:
  fl = FileLoader(options.datadir, options.datafile_prefix)
  if options.pidfile is not None:
    pid_f = open(options.pidfile, 'w')
    pid_f.write(str(os.getpid()) + "\n")
    pid_f.close()
  lc = LoopingCall(fl.loadMetricsFile)
  lc.start(0)

exitCode = 0
def shutdown(results):
  global exitCode
  for success, result in results:
    if not success:
      exitCode = 1
      break
  if reactor.running:
    reactor.stop()

reactor.run()
raise SystemExit(exitCode)
