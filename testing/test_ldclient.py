from builtins import object
from ldclient.client import LDClient, Config
from ldclient.interfaces import FeatureRequester
import pytest

try:
    import queue
except:
    import Queue as queue


class MockFeatureRequester(FeatureRequester):

    def __init__(self, *_):
        pass

    def get(self, key, callback):
        return callback({
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
        })


client = LDClient("API_KEY", Config("http://localhost:3000",
                                                      feature_requester_class=MockFeatureRequester))

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
        pass


def mock_consumer():
    return MockConsumer()


def noop_consumer():
    return


def setup_function(function):
    client.set_online()
    client._queue = queue.Queue(10)
    client._consumer = mock_consumer()


@pytest.fixture(autouse=True)
def noop_check_consumer(monkeypatch):
    monkeypatch.setattr(client, '_check_consumer', noop_consumer)


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
        return e['kind'] == 'feature' and e['key'] == 'xyz' and e['user'] == user and e['value'] == True

    assert expected_event(client._queue.get(False))


def test_toggle_event_offline():
    client.set_offline()
    client.toggle('xyz', user, default=None)
    assert client._queue.empty()


def test_identify():
    client.identify(user)

    def expected_event(e):
        return e['kind'] == 'identify' and e['key'] == u'xyz' and e['user'] == user

    assert expected_event(client._queue.get(False))


def test_identify_offline():
    client.set_offline()
    client.identify(user)
    assert client._queue.empty()


def test_track():
    client.track('my_event', user, 42)

    def expected_event(e):
        return e['kind'] == 'custom' and e['key'] == 'my_event' and e['user'] == user and e['data'] == 42

    assert expected_event(client._queue.get(False))


def test_track_offline():
    client.set_offline()
    client.track('my_event', user, 42)
    assert client._queue.empty()


def test_defaults():
    client = LDClient("API_KEY", Config("http://localhost:3000", defaults={"foo": "bar"}))
    client.set_offline()
    assert "bar" == client.toggle('foo', user, default=None)


def test_defaults_and_online():
    client = LDClient("API_KEY", Config("http://localhost:3000", defaults={"feature.key": "bar"},
                                                          feature_requester_class=MockFeatureRequester))
    assert True == client.toggle('feature.key', user, default=None)


def test_no_defaults():
    client.set_offline()
    assert "bar" == client.toggle('foo', user, default="bar")


def drain(queue):
    while not queue.empty():
        queue.get()
        queue.task_done()
    return


def test_flush_empties_queue():
    client.track('my_event', user, 42)
    client.track('my_event', user, 33)
    drain(client._queue)
    client.flush()
    assert client._queue.empty()


def test_flush_offline_does_not_empty_queue():
    client.track('my_event', user, 42)
    client.track('my_event', user, 33)
    client.set_offline()
    client.flush()
    assert not client._queue.empty()