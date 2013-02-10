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
from twisted.internet import defer
from twisted.internet.task import deferLater


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
    self.filelist = []
    self.datadir = datadir
    self.datafile_prefix = datafile_prefix
    self.current_file = None
    self.sleep_for = 0
    self.lines_to_reinject = []

  def _sleep(self, secs):
    return deferLater(reactor, secs, lambda : None)

  @defer.inlineCallbacks
  def getNextFile(self):
    if len(self.filelist) == 0:
      self.filelist = [f for f in os.listdir(self.datadir) if (os.path.splitext(f)[1] == '.log') and f.startswith(self.datafile_prefix) ][0:100]

    if len(self.filelist) > 0:
      log.listener("Working on %d files" % len(self.filelist))
      defer.returnValue(os.path.join(self.datadir, self.filelist.pop()))
    else:
      log.listener("No files found.  Sleeping 3...")
      yield deferLater(reactor, 3, lambda: None) 

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

  @defer.inlineCallbacks
  def loadDataFile(self):
    try:
      if self.current_file is None:
        fname = yield self.getNextFile()
        if fname is None:
          return
        if os.path.splitext(fname)[1] != '.log':
          return
        log.listener("Attempting to lock %s" % fname)
        if self.acquireLock(fname):
          log.listener("Feeding %s" % fname)
          try:
            self.current_file = open(fname)
          except:
            return
        else:
          return
      while True:
        line = self.current_file.readline()
        if not line:
          break
        try:
          metric, value, timestamp = line.strip().split()
          datapoint = ( float(timestamp), float(value) )
          self.metricReceived(metric, datapoint)
        except:
          log.listener('invalid line received from client %s, ignoring' % self.peerName)

      self.current_file.close()
      try:
        log.msg("Done with %s" % fname)
        os.remove(self.current_file.name)
        self.releaseLock(self.current_file.name)
        self.current_file = None
      except OSError:
        self.current_file = None
        pass
    except IndexError:
      log.debug("No data")


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
