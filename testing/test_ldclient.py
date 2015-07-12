from builtins import next
from builtins import filter
from builtins import object
import ldclient
import pytest

try:
    import queue
except:
    import Queue as queue

client = ldclient.LDClient("API_KEY", ldclient.Config("http://localhost:3000"))

user = {
  u'key': u'xyz', 
  u'custom': { 
    u'bizzle': u'def' 
    }
  }

class MockConsumer(object):
    def __init__(self):
      self.sent = []

    def send(self, events):
      self.sent.extend(events)

def mock_consumer():
  return MockConsumer()      

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
        return default
    return val      

def setup_function(function):
  client.set_online()
  client._queue = queue.Queue(10)

@pytest.fixture(autouse=True)
def use_mock_consumer(monkeypatch):
  monkeypatch.setattr(client, '_check_consumer', mock_consumer)

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

def test_toggle():
  assert client.toggle('xyz', user, default=None) == True

def test_toggle_offline():
  client.set_offline()
  assert client.toggle('xyz', user, default=None) == None

def test_toggle_event():
  client.toggle('xyz', user, default=None)
  def expected_event(e):
    return e['kind'] == 'feature' and e['key']  == 'xyz' and e['user'] == user and e['value'] == True
  assert expected_event(client._queue.get(False))

def test_toggle_event_offline():
  client.set_offline()
  client.toggle('xyz', user, default=None)
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


'''



def test_flush_empties_queue():
  client.track('my_event', user, 42)
  client.track('my_event', user, 33)
  client.flush()
  assert len(mock_buffered_consumer.queue) == 0

def test_flush_sends_events():
  client.track('my_event', user, 42)
  client.track('my_event', user, 33)
  client.flush()

  def expected_event1(e):
    return e['kind'] == 'custom' and e['key']  == 'my_event' and e['user'] == user and e['data'] == 42
  def expected_event2(e):
    return e['kind'] == 'custom' and e['key']  == 'my_event' and e['user'] == user and e['data'] == 33

  assert (
    next(filter(expected_event1, mock_consumer.sent), None) is not None and 
    next(filter(expected_event2, mock_consumer.sent), None) is not None
    )

def test_flush_offline():
  client.track('my_event', user, 42)
  client.track('my_event', user, 33)
  client.set_offline()
  client.flush()

  def expected_event1(e):
    return e['kind'] == 'custom' and e['key']  == 'my_event' and e['user'] == user and e['data'] == 42
  def expected_event2(e):
    return e['kind'] == 'custom' and e['key']  == 'my_event' and e['user'] == user and e['data'] == 33

  assert (
    next(filter(expected_event1, mock_consumer.sent), None) is None and 
    next(filter(expected_event2, mock_consumer.sent), None) is None
    )
'''