import traceback
from twisted.internet import reactor
from twisted.internet.protocol import DatagramProtocol
from twisted.internet.error import ConnectionDone
from twisted.protocols.basic import LineOnlyReceiver, Int32StringReceiver
from carbon import log, events, state
from carbon.conf import settings
from carbon.util import pickle, get_unpickler
# For file loader
import os, time
from twisted.internet import defer, inotify
from twisted.internet.task import deferLater
from twisted.python import filepath


class MetricReceiver:
  """ Base class for all metric receiving protocols, handles flow
  control events and connection state logging.
  """
  def connectionMade(self):
    self.peerName = self.getPeerName()
    log.listener("%s connection with %s established" % (self.__class__.__name__, self.peerName))

    if state.metricReceiversPaused:
      self.pauseReceiving()

    state.connectedMetricReceiverProtocols.add(self)
    events.pauseReceivingMetrics.addHandler(self.pauseReceiving)
    events.resumeReceivingMetrics.addHandler(self.resumeReceiving)

  def getPeerName(self):
    if hasattr(self.transport, 'getPeer'):
      peer = self.transport.getPeer()
      return "%s:%d" % (peer.host, peer.port)
    else:
      return "peer"

  def pauseReceiving(self):
    self.transport.pauseProducing()

  def resumeReceiving(self):
    self.transport.resumeProducing()

  def connectionLost(self, reason):
    if reason.check(ConnectionDone):
      log.listener("%s connection with %s closed cleanly" % (self.__class__.__name__, self.peerName))
    else:
      log.listener("%s connection with %s lost: %s" % (self.__class__.__name__, self.peerName, reason.value))

    state.connectedMetricReceiverProtocols.remove(self)
    events.pauseReceivingMetrics.removeHandler(self.pauseReceiving)
    events.resumeReceivingMetrics.removeHandler(self.resumeReceiving)

  def metricReceived(self, metric, datapoint):
    if datapoint[1] == datapoint[1]: # filter out NaN values
      events.metricReceived(metric, datapoint)


class MetricLineReceiver(MetricReceiver, LineOnlyReceiver):
  delimiter = '\n'

  def lineReceived(self, line):
    try:
      metric, value, timestamp = line.strip().split()
      datapoint = ( float(timestamp), float(value) )
    except:
      log.listener('invalid line received from client %s, ignoring' % self.peerName)
      return

    self.metricReceived(metric, datapoint)


class MetricDatagramReceiver(MetricReceiver, DatagramProtocol):
  def datagramReceived(self, data, (host, port)):
    for line in data.splitlines():
      try:
        metric, value, timestamp = line.strip().split()
        datapoint = ( float(timestamp), float(value) )

        self.metricReceived(metric, datapoint)
      except:
        log.listener('invalid line received from %s, ignoring' % host)


class MetricPickleReceiver(MetricReceiver, Int32StringReceiver):
  MAX_LENGTH = 2 ** 20

  def connectionMade(self):
    MetricReceiver.connectionMade(self)
    self.unpickler = get_unpickler(insecure=settings.USE_INSECURE_UNPICKLER)

  def stringReceived(self, data):
    try:
      datapoints = self.unpickler.loads(data)
    except:
      log.listener('invalid pickle received from %s, ignoring' % self.peerName)
      return

    for (metric, datapoint) in datapoints:
      #Expect proper types since it is coming in pickled.
      self.metricReceived(metric, datapoint)

class MetricFileLoader(MetricReceiver):
  def __init__(self, datadir, datafile_prefix=''):
    self.notifier = inotify.INotify()
    self.filelist = []
    self.datadir = datadir
    self.datafile_prefix = datafile_prefix
    self.current_file = None
    self.pid = os.getpid()
    self.paused = False
    self.data = None
    self.lookForFiles = True

    self.notifier.startReading()
    checkMask = inotify.IN_ISDIR | inotify.IN_CREATE
    self.notifier.watch(filepath.FilePath(self.datadir), mask=checkMask, callbacks=[self.watchDirCallback])

  def pauseReceiving(self):
    self.paused = True

  def resumeReceiving(self):
    self.paused = False

  def watchDirCallback(self, watch, path, mask):
    self.lookForFiles = True

  @defer.inlineCallbacks
  def getNextFile(self):
    if self.current_file is not None:
      return

    if self.paused:
      return

    log.listener("In getNextFile")
    if len(self.filelist) < 5:
      log.listener("Refreshing low file list")
      self.filelist = [f for f in os.listdir(self.datadir) if (os.path.splitext(f)[1] == '.log') and f.startswith(self.datafile_prefix) ][0:100]

    while self.current_file is None and len(self.filelist) > 0:
      log.listener("Working on %d potential files" % len(self.filelist))
      file = os.path.join(self.datadir, self.filelist.pop())
      try:
        yield os.rename(file, "%s.%s" % (file, self.pid))
      except:
         pass
      file += ".%s" % self.pid
      if os.path.isfile(file):
        log.listener("Feeding %s" % file)
        try:
          self.current_file = open(file)
          yield self.current_file
        except:
          log.listener("Failed to open locked file %s" % file)
          self.current_file = None

      # repopulate list if we fell all the way through without obtaining a file to work on.
      if len(self.filelist) == 0:
        log.listener("Refreshing empty file list")
        self.filelist = [f for f in os.listdir(self.datadir) if (os.path.splitext(f)[1] == '.log') and f.startswith(self.datafile_prefix) ][0:100]

    # if no file was obtained, then mark the lookForFiles as false so we wait for a new file to exist.
    if self.current_file is None:
      self.lookForFiles = False


  def consumeCurrentFile(self):
    while True:
      line = self.current_file.readline()
      if line:
        try:
          metric, value, timestamp = line.strip().split()
          datapoint = ( float(timestamp), float(value) )
          self.metricReceived(metric, datapoint)
        except:
          log.listener('invalid line: %s, ignoring' % line)
      else:
        log.listener("Done with %s" % self.current_file.name)
        self.current_file.close()
        try:
          os.remove(self.current_file.name)
        except OSError:
          pass
        self.current_file = None
        break
    self.lookForFiles = True
    return

  @defer.inlineCallbacks
  def loadDataFile(self):
    if self.current_file is not None:
      self.consumeCurrentFile()

    if self.current_file is None and self.lookForFiles:
      yield self.getNextFile()

    # If the current iteration didn't find a file, then sleep a second
    if self.current_file is None:
      log.listener("No current file found.  Sleeping.")
      yield deferLater(reactor, 1, lambda : None)

    return


class CacheManagementHandler(Int32StringReceiver):
  def connectionMade(self):
    peer = self.transport.getPeer()
    self.peerAddr = "%s:%d" % (peer.host, peer.port)
    log.query("%s connected" % self.peerAddr)
    self.unpickler = get_unpickler(insecure=settings.USE_INSECURE_UNPICKLER)

  def connectionLost(self, reason):
    if reason.check(ConnectionDone):
      log.query("%s disconnected" % self.peerAddr)
    else:
      log.query("%s connection lost: %s" % (self.peerAddr, reason.value))

  def stringReceived(self, rawRequest):
    request = self.unpickler.loads(rawRequest)
    if request['type'] == 'cache-query':
      metric = request['metric']
      datapoints = MetricCache.getDatapoints(metric)
      result = dict(datapoints=datapoints)
      log.query('[%s] cache query for \"%s\" returned %d values' % (self.peerAddr, metric, len(datapoints)))
      instrumentation.increment('writer.cache_queries')

    elif request['type'] == 'bulk-cache-query':
      query_results = {}
      for metric in request['metrics']:
        query_results[metric] = MetricCache.getDatapoints(metric)
      log.query('[%s] bulk-cache-query for %d metrics' % (self.peerAddr, len(query_results)))
      instrumentation.increment('writer.cache_queries')
      result = dict(results=query_results)

    elif request['type'] == 'get-metadata':
      try:
        value = state.database.get_metadata(request['metric'], request['key'])
        result = dict(value=value)
      except:
        log.err()
        result = dict(error=traceback.format_exc())

    elif request['type'] == 'set-metadata':
      try:
        old_value = state.database.set_metadata(request['metric'],
                                                request['key'],
                                                request['value'])
        result = dict(old_value=old_value, new_value=request['value'])
      except:
        log.err()
        result = dict(error=traceback.format_exc())
    else:
      result = dict(error="Invalid request type \"%s\"" % request['type'])

    response = pickle.dumps(result, protocol=-1)
    self.sendString(response)


# Avoid import circularities
from carbon.cache import MetricCache
from carbon import instrumentation
