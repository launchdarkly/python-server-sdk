from builtins import next
from builtins import filter
from builtins import object
import ldclient


class MockConsumer(object):
    def __init__(self):
      self.sent = []

    def send(self, events):
      self.sent.extend(events)

class MockBufferedConsumer(ldclient.BufferedConsumer):
    def __init__(self, consumer):
        super(MockBufferedConsumer, self).__init__(consumer, 500, 5)

    def _should_flush(self):
        return False

# LDClient mock that has all the real functionality, except it always uses a static feature
class StaticLDClient(ldclient.LDClient):
    def __init__(self, api_key, config = None, consumer = None):
        self._toggle_count = 0
        super(StaticLDClient, self).__init__(api_key, config, consumer)

    def _toggle(self, key, user, default):
        self._toggle_count += 1
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

mock_consumer = MockConsumer()
mock_buffered_consumer = MockBufferedConsumer(mock_consumer)
client = StaticLDClient('', consumer = mock_buffered_consumer)


user = {
  u'key': u'xyz', 
  u'custom': { 
    u'bizzle': u'def' 
    }
  }

def setup_function(function):
  client.set_online()
  client.flush()
  mock_consumer.sent = []

def test_set_offline():
  client.set_offline()
  assert client.is_offline() == True

def test_set_online():
  client.set_offline()
  client.set_online()
  assert client.is_offline() == False

def test_toggle():
  assert client.toggle('xyz', user, default=None) == True

def test_toggle_event():
  client.toggle('xyz', user, default=None)
  def expected_event(e):
    return e['kind'] == 'feature' and e['key']  == 'xyz' and e['user'] == user and e['value'] == True
  assert next(filter(expected_event, mock_buffered_consumer.queue), None) is not None


def test_toggle_offline():
  client.set_offline()
  assert client.toggle('xyz', user, default=None) == None

def test_toggle_event_offline():
  client.set_offline()
  client.toggle('xyz', user, default=None)
  def expected_event(e):
    return e['kind'] == 'feature' and e['key']  == 'xyz' and e['user'] == user and e['value'] == True
  assert next(filter(expected_event, mock_buffered_consumer.queue), None) is None

def test_identify():
  client.identify(user)
  def expected_event(e):
    return e['kind'] == 'identify' and e['key']  == u'xyz' and e['user'] == user
  assert next(filter(expected_event, mock_buffered_consumer.queue), None) is not None

def test_identify_offline():
  client.set_offline()
  client.identify(user)
  def expected_event(e):
    return e['kind'] == 'identify' and e['key']  == u'xyz' and e['user'] == user
  assert next(filter(expected_event, mock_buffered_consumer.queue), None) is None

def test_track():
  client.track('my_event', user, 42)
  def expected_event(e):
    return e['kind'] == 'custom' and e['key']  == 'my_event' and e['user'] == user and e['data'] == 42
  assert next(filter(expected_event, mock_buffered_consumer.queue), None) is not None

def test_track_offline():
  client.set_offline()
  client.track('my_event', user, 42)
  def expected_event(e):
    return e['kind'] == 'custom' and e['key']  == 'my_event' and e['user'] == user and e['data'] == 42
  assert next(filter(expected_event, mock_buffered_consumer.queue), None) is None

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
