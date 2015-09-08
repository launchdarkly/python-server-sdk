from builtins import next
from builtins import filter
from builtins import object
import ldclient
from ldclient.twisted import TwistedLDClient
import pytest
from twisted.internet import defer

try:
    import queue
except:
    import Queue as queue

client = TwistedLDClient("API_KEY", ldclient.Config("http://localhost:3000"))

user = {
  u'key': u'xyz', 
  u'custom': { 
    u'bizzle': u'def' 
    }
  }

class MockConsumer(object):
    def __init__(self):
      self._running = False

    def stop(self):
      self._running = False

    def start(self):
      self._running = True

    def is_alive(self):
      return self._running

    def flush(self):
        return defer.succeed(True)


def mock_consumer():
  return MockConsumer()  

def noop_consumer():
  return    

def mock_toggle(key, user, default):
    hash = minimal_feature = {
      u'key': u'feature.key', 
      u'salt': u'abc',
      u'on': True,
      u'variations': [
        {
          u'value': True,
          u'weight': 100,
          u'targets': []
        },
        {
          u'value': False,
          u'weight': 0,
          u'targets': []
        }
      ]
    }
    val = ldclient._evaluate(hash, user)
    if val is None:
        return defer.succeed(default)
    return defer.succeed(val)

def setup_function(function):
  client.set_online()
  client._queue = queue.Queue(10)
  client._consumer = mock_consumer()

@pytest.fixture(autouse=True)
def noop_check_consumer(monkeypatch):
  monkeypatch.setattr(client, '_check_consumer', noop_consumer)

@pytest.fixture(autouse=True)
def no_remote_toggle(monkeypatch):
  monkeypatch.setattr(client, '_toggle', mock_toggle)

def test_set_offline():
  client.set_offline()
  assert client.is_offline() == True

def test_set_online():
  client.set_offline()
  client.set_online()
  assert client.is_offline() == False

@pytest.inlineCallbacks
def test_toggle():
  result = yield client.toggle('xyz', user, default=None)
  assert result == True

@pytest.inlineCallbacks
def test_toggle_offline():
  client.set_offline()
  assert (yield client.toggle('xyz', user, default=None)) == None

@pytest.inlineCallbacks
def test_toggle_event():
  val = yield client.toggle('xyz', user, default=None)
  def expected_event(e):
    return e['kind'] == 'feature' and e['key']  == 'xyz' and e['user'] == user and e['value'] == True
  assert expected_event(client._queue.get(False))

@pytest.inlineCallbacks
def test_toggle_event_offline():
  client.set_offline()
  yield client.toggle('xyz', user, default=None)
  assert client._queue.empty()


def test_identify():
  client.identify(user)
  def expected_event(e):
    return e['kind'] == 'identify' and e['key']  == u'xyz' and e['user'] == user
  assert expected_event(client._queue.get(False))

def test_identify_offline():
  client.set_offline()
  client.identify(user)
  assert client._queue.empty()

def test_track():
  client.track('my_event', user, 42)
  def expected_event(e):
    return e['kind'] == 'custom' and e['key']  == 'my_event' and e['user'] == user and e['data'] == 42
  assert expected_event(client._queue.get(False))

def test_track_offline():
  client.set_offline()
  client.track('my_event', user, 42)
  assert client._queue.empty()

def drain(queue):
    while not queue.empty():
      queue.get()
      queue.task_done()
    return

@pytest.inlineCallbacks
def test_flush_empties_queue():
  client.track('my_event', user, 42)
  client.track('my_event', user, 33)
  drain(client._queue)
  yield client.flush()
  assert client._queue.empty()


@pytest.inlineCallbacks
def test_flush_offline_does_not_empty_queue():
  client.track('my_event', user, 42)
  client.track('my_event', user, 33)
  client.set_offline()
  yield client.flush()
  assert not client._queue.empty()