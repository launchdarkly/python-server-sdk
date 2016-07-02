from builtins import object
from ldclient.client import LDClient, Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.interfaces import FeatureRequester, FeatureStore
import pytest
from testing.sync_util import wait_until

try:
    import queue
except:
    import Queue as queue


class MockFeatureStore(FeatureStore):
    def delete(self, key, version):
        pass

    @property
    def initialized(self):
        pass

    def init(self, features):
        pass

    def all(self):
        pass

    def upsert(self, key, feature):
        pass

    def __init__(self, *_):
        pass

    def get(self, key):
        if key == "feature.key":
            return {
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
        else:
            return None


client = LDClient("API_KEY", Config("http://localhost:3000", feature_store=MockFeatureStore()))
offline_client = LDClient("API_KEY", Config("http://localhost:3000", feature_store=MockFeatureStore(), offline=True))

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


class MockFeatureRequester(FeatureRequester):
    def __init__(self, *_):
        pass

    def get_all(self):
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
    client._queue = queue.Queue(10)
    client._event_consumer = mock_consumer()


@pytest.fixture(autouse=True)
def noop_check_consumer(monkeypatch):
    monkeypatch.setattr(client, '_check_consumer', noop_consumer)


def wait_for_event(c, cb):
    e = c._queue.get(False)
    return cb(e)


def test_toggle():
    assert client.toggle('feature.key', user, default=None) == True


def test_toggle_offline():
    assert offline_client.toggle('feature.key', user, default=None) == None


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
        return e['kind'] == 'feature' and e['key'] == 'feature.key' and e['user'] == sanitized_numeric_key_user and e[
                                                                                                                        'value'] == True and \
               e['default'] == None

    assert expected_event(client._queue.get(False))


def test_toggle_event_offline():
    offline_client.toggle('feature.key', user, default=None)
    assert offline_client._queue.empty()


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
    assert offline_client._queue.empty()


def test_track():
    client.track('my_event', user, 42)

    def expected_event(e):
        return e['kind'] == 'custom' and e['key'] == 'my_event' and e['user'] == user and e['data'] == 42

    assert expected_event(client._queue.get(False))


def test_track_numeric_key_user():
    client.track('my_event', numeric_key_user, 42)

    def expected_event(e):
        return e['kind'] == 'custom' and e['key'] == 'my_event' and e['user'] == sanitized_numeric_key_user and e[
                                                                                                                    'data'] == 42

    assert expected_event(client._queue.get(False))


def test_track_offline():
    offline_client.track('my_event', user, 42)
    assert offline_client._queue.empty()


def test_defaults():
    client = LDClient("API_KEY", Config(
        "http://localhost:3000", defaults={"foo": "bar"}, offline=True))
    assert "bar" == client.toggle('foo', user, default=None)


def test_defaults_and_online():
    expected = "bar"
    my_client = LDClient("API_KEY", Config("http://localhost:3000", defaults={"foo": expected},
                                           event_consumer_class=MockConsumer, feature_requester_class=MockFeatureRequester,
                                           feature_store=InMemoryFeatureStore()))
    actual = my_client.toggle('foo', user, default="originalDefault")
    assert actual == expected
    assert wait_for_event(my_client, lambda e: e['kind'] == 'feature' and e['key'] == u'foo' and e['user'] == user)


def test_defaults_and_online_no_default():
    client = LDClient("API_KEY", Config("http://localhost:3000", defaults={"foo": "bar"},
                                        event_consumer_class=MockConsumer, feature_requester_class=MockFeatureRequester))
    assert "jim" == client.toggle('baz', user, default="jim")
    assert wait_for_event(client, lambda e: e['kind'] == 'feature' and e['key'] == u'baz' and e['user'] == user)


def test_exception_in_retrieval():
    class ExceptionFeatureRequester(FeatureRequester):
        def __init__(self, *_):
            pass

        def get_all(self):
            raise Exception("blah")

    client = LDClient("API_KEY", Config("http://localhost:3000", defaults={"foo": "bar"},
                                        feature_store=InMemoryFeatureStore(),
                                        feature_requester_class=ExceptionFeatureRequester,
                                        event_consumer_class=MockConsumer))
    assert "bar" == client.toggle('foo', user, default="jim")
    assert wait_for_event(client, lambda e: e['kind'] == 'feature' and e['key'] == u'foo' and e['user'] == user)


def test_no_defaults():
    assert "bar" == offline_client.toggle('foo', user, default="bar")


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
