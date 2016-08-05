from builtins import object
from ldclient.client import LDClient, Config
from ldclient.interfaces import FeatureRequester
from mock import patch
import pytest
from testing.sync_util import wait_until

try:
    import queue
except:
    import Queue as queue


class MockFeatureRequester(FeatureRequester):

    def __init__(self, *_):
        pass

    def get(self, key, callback):
        if key == "feature.key":
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
        else:
            return callback(None)


client = LDClient("API_KEY", Config("http://localhost:3000",
                                    feature_requester_class=MockFeatureRequester))

user = {
    u'key': u'xyz',
    u'custom': {
        u'bizzle': u'def'
    }
}

numeric_key_user = {}

sanitized_numeric_key_user = {
    u'key': '33',
    u'custom': {
        u'bizzle': u'def'
    }
}


class MockConsumer(object):

    def __init__(self, *_):
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
    global numeric_key_user
    numeric_key_user = {
        u'key': 33,
        u'custom': {
            u'bizzle': u'def'
        }
    }
    client.set_online()
    client._queue = queue.Queue(10)
    client._consumer = mock_consumer()


@pytest.fixture(autouse=True)
def noop_check_consumer(monkeypatch):
    monkeypatch.setattr(client, '_check_consumer', noop_consumer)


def wait_for_event(c, cb):
    e = c._queue.get(False)
    return cb(e)


def test_set_offline():
    client.set_offline()
    assert client.is_offline() == True


def test_set_online():
    client.set_offline()
    client.set_online()
    assert client.is_offline() == False


def test_toggle():
    assert client.toggle('feature.key', user, default=None) == True


def test_toggle_offline():
    client.set_offline()
    assert client.toggle('feature.key', user, default=None) == None


def test_toggle_event():
    client.toggle('feature.key', user, default=None)

    def expected_event(e):
        return e['kind'] == 'feature' and e['key'] == 'feature.key' and e['user'] == user and e['value'] == True and e['default'] == None

    assert expected_event(client._queue.get(False))

def test_sanitize_user():
    client._sanitize_user(numeric_key_user)
    assert numeric_key_user == sanitized_numeric_key_user

def test_toggle_event_numeric_user_key():
    client.toggle('feature.key', numeric_key_user, default=None)

    def expected_event(e):
        return e['kind'] == 'feature' and e['key'] == 'feature.key' and e['user'] == sanitized_numeric_key_user and e['value'] == True and e['default'] == None

    assert expected_event(client._queue.get(False))


def test_toggle_event_offline():
    client.set_offline()
    client.toggle('feature.key', user, default=None)
    assert client._queue.empty()


@patch('ldclient.newrelicwrapper.annotate_transaction')
def test_toggle_newrelic(annotate_mock):
    client.toggle('feature.key', user, default=None)
    annotate_mock.assert_called_once_with('feature.key', True)


def test_identify():
    client.identify(user)

    def expected_event(e):
        return e['kind'] == 'identify' and e['key'] == u'xyz' and e['user'] == user

    assert expected_event(client._queue.get(False))


def test_identify_numeric_key_user():
    client.identify(numeric_key_user)

    def expected_event(e):
        return e['kind'] == 'identify' and e['key'] == '33' and e['user'] == sanitized_numeric_key_user

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


def test_track_numeric_key_user():
    client.track('my_event', numeric_key_user, 42)

    def expected_event(e):
        return e['kind'] == 'custom' and e['key'] == 'my_event' and e['user'] == sanitized_numeric_key_user and e['data'] == 42

    assert expected_event(client._queue.get(False))


def test_track_offline():
    client.set_offline()
    client.track('my_event', user, 42)
    assert client._queue.empty()


def test_defaults():
    client = LDClient("API_KEY", Config(
        "http://localhost:3000", defaults={"foo": "bar"}))
    client.set_offline()
    assert "bar" == client.toggle('foo', user, default=None)


def test_defaults_and_online():
    client = LDClient("API_KEY", Config("http://localhost:3000", defaults={"foo": "bar"},
                                        feature_requester_class=MockFeatureRequester,
                                        consumer_class=MockConsumer))
    assert "bar" == client.toggle('foo', user, default="jim")
    assert wait_for_event(client, lambda e: e['kind'] == 'feature' and e[
                          'key'] == u'foo' and e['user'] == user)


def test_defaults_and_online_no_default():
    client = LDClient("API_KEY", Config("http://localhost:3000", defaults={"foo": "bar"},
                                        feature_requester_class=MockFeatureRequester,
                                        consumer_class=MockConsumer))
    assert "jim" == client.toggle('baz', user, default="jim")
    assert wait_for_event(client, lambda e: e['kind'] == 'feature' and e[
                          'key'] == u'baz' and e['user'] == user)


def test_exception_in_retrieval():
    class ExceptionFeatureRequester(FeatureRequester):

        def __init__(self, *_):
            pass

        def get(self, key, callback):
            raise Exception("blah")

    client = LDClient("API_KEY", Config("http://localhost:3000", defaults={"foo": "bar"},
                                        feature_requester_class=ExceptionFeatureRequester,
                                        consumer_class=MockConsumer))
    assert "bar" == client.toggle('foo', user, default="jim")
    assert wait_for_event(client, lambda e: e['kind'] == 'feature' and e[
                          'key'] == u'foo' and e['user'] == user)


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
