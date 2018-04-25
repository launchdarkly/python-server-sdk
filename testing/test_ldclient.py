from builtins import object
from ldclient.client import LDClient, Config
from ldclient.event_processor import NullEventProcessor
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.interfaces import FeatureRequester, FeatureStore, UpdateProcessor
from ldclient.versioned_data_kind import FEATURES
import pytest
from testing.sync_util import wait_until

try:
    import queue
except:
    import Queue as queue


class MockEventProcessor(object):
    def __init__(self, *_):
        self._running = False
        self._events = []
        mock_event_processor = self

    def stop(self):
        self._running = False

    def start(self):
        self._running = True

    def is_alive(self):
        return self._running

    def send_event(self, event):
        self._events.append(event)

    def flush(self):
        pass


class MockUpdateProcessor(UpdateProcessor):
    def __init__(self, config, store, ready):
        ready.set()

    def start(self):
        pass

    def stop(self):
        pass

    def is_alive(self):
        return True

    def initialized(self):
        return True


client = LDClient(config=Config(base_uri="http://localhost:3000",
                                event_processor_class = MockEventProcessor, update_processor_class = MockUpdateProcessor))
offline_client = LDClient(config=
                          Config(sdk_key="secret", base_uri="http://localhost:3000",
                                 offline=True))
no_send_events_client = LDClient(config=
                                 Config(sdk_key="secret", base_uri="http://localhost:3000",
                                 update_processor_class = MockUpdateProcessor, send_events=False))

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


def setup_function(function):
    global numeric_key_user
    numeric_key_user = {
        u'key': 33,
        u'custom': {
            u'bizzle': u'def'
        }
    }


def make_client(store):
    return LDClient(config=Config(sdk_key = 'SDK_KEY',
                                  base_uri="http://localhost:3000",
                                  event_processor_class=MockEventProcessor,
                                  update_processor_class=MockUpdateProcessor,
                                  feature_store=store))


def get_first_event(c):
    return c._event_processor._events.pop(0)


def test_ctor_both_sdk_keys_set():
    with pytest.raises(Exception):
        config = Config(sdk_key="sdk key a", offline=True)
        LDClient(sdk_key="sdk key b", config=config)


def test_client_has_null_event_processor_if_offline():
    assert isinstance(offline_client._event_processor, NullEventProcessor)


def test_client_has_null_event_processor_if_send_events_off():
    assert isinstance(no_send_events_client._event_processor, NullEventProcessor)


def test_toggle_offline():
    assert offline_client.variation('feature.key', user, default=None) is None


def test_sanitize_user():
    client._sanitize_user(numeric_key_user)
    assert numeric_key_user == sanitized_numeric_key_user


def test_identify():
    client.identify(user)

    e = get_first_event(client)
    assert e['kind'] == 'identify' and e['key'] == u'xyz' and e['user'] == user


def test_identify_numeric_key_user():
    client.identify(numeric_key_user)

    e = get_first_event(client)
    assert e['kind'] == 'identify' and e['key'] == '33' and e['user'] == sanitized_numeric_key_user


def test_track():
    client.track('my_event', user, 42)

    e = get_first_event(client)
    assert e['kind'] == 'custom' and e['key'] == 'my_event' and e['user'] == user and e['data'] == 42


def test_track_numeric_key_user():
    client.track('my_event', numeric_key_user, 42)

    e = get_first_event(client)
    assert e['kind'] == 'custom' and e['key'] == 'my_event' and e['user'] == sanitized_numeric_key_user \
       and e['data'] == 42


def test_defaults():
    my_client = LDClient(config=Config(base_uri="http://localhost:3000",
                                       defaults={"foo": "bar"},
                                       offline=True))
    assert "bar" == my_client.variation('foo', user, default=None)


def test_defaults_and_online():
    expected = "bar"
    my_client = LDClient(config=Config(base_uri="http://localhost:3000",
                                       defaults={"foo": expected},
                                       event_processor_class=MockEventProcessor,
                                       update_processor_class=MockUpdateProcessor,
                                       feature_store=InMemoryFeatureStore()))
    actual = my_client.variation('foo', user, default="originalDefault")
    assert actual == expected
    e = get_first_event(my_client)
    assert e['kind'] == 'feature' and e['key'] == u'foo' and e['user'] == user


def test_defaults_and_online_no_default():
    my_client = LDClient(config=Config(base_uri="http://localhost:3000",
                                       defaults={"foo": "bar"},
                                       event_processor_class=MockEventProcessor,
                                       update_processor_class=MockUpdateProcessor))
    assert "jim" == my_client.variation('baz', user, default="jim")
    e = get_first_event(my_client)
    assert e['kind'] == 'feature' and e['key'] == u'baz' and e['user'] == user


def test_no_defaults():
    assert "bar" == offline_client.variation('foo', user, default="bar")


def test_event_for_existing_feature():
    feature = {
        u'key': u'feature.key',
        u'salt': u'abc',
        u'on': True,
        u'variations': ['a', 'b'],
        u'fallthrough': {
            u'variation': 1
        },
        u'trackEvents': True
    }
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    assert 'b' == client.variation('feature.key', user, default='c')
    e = get_first_event(client)
    assert (e['kind'] == 'feature' and
        e['key'] == 'feature.key' and
        e['user'] == user and
        e['value'] == 'b' and
        e['variation'] == 1 and
        e['default'] == 'c' and
        e['trackEvents'] == True)


def test_event_for_unknown_feature():
    store = InMemoryFeatureStore()
    store.init({FEATURES: {}})
    client = make_client(store)
    assert 'c' == client.variation('feature.key', user, default='c')
    e = get_first_event(client)
    assert (e['kind'] == 'feature' and
        e['key'] == 'feature.key' and
        e['user'] == user and
        e['value'] == 'c' and
        e['variation'] == None and
        e['default'] == 'c')


def test_event_for_existing_feature_with_no_user():
    feature = {
        u'key': u'feature.key',
        u'salt': u'abc',
        u'on': True,
        u'variations': ['a', 'b'],
        u'fallthrough': {
            u'variation': 1
        },
        u'trackEvents': True
    }
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    assert 'c' == client.variation('feature.key', None, default='c')
    e = get_first_event(client)
    assert (e['kind'] == 'feature' and
        e['key'] == 'feature.key' and
        e['user'] == None and
        e['value'] == 'c' and
        e['variation'] == None and
        e['default'] == 'c' and
        e['trackEvents'] == True)


def test_event_for_existing_feature_with_no_user_key():
    feature = {
        u'key': u'feature.key',
        u'salt': u'abc',
        u'on': True,
        u'variations': ['a', 'b'],
        u'fallthrough': {
            u'variation': 1
        },
        u'trackEvents': True
    }
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    bad_user = { u'name': u'Bob' }
    assert 'c' == client.variation('feature.key', bad_user, default='c')
    e = get_first_event(client)
    assert (e['kind'] == 'feature' and
        e['key'] == 'feature.key' and
        e['user'] == bad_user and
        e['value'] == 'c' and
        e['variation'] == None and
        e['default'] == 'c' and
        e['trackEvents'] == True)


def test_all_flags():
    feature = {
        u'key': u'feature.key',
        u'salt': u'abc',
        u'on': True,
        u'variations': ['a', 'b'],
        u'fallthrough': {
            u'variation': 1
        }
    }
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = LDClient(config=Config(sdk_key = 'SDK_KEY',
                                    base_uri="http://localhost:3000",
                                    event_processor_class=MockEventProcessor,
                                    update_processor_class=MockUpdateProcessor,
                                    feature_store=store))
    result = client.all_flags(user)
    assert (len(result) == 1 and
        result.get('feature.key') == 'b')


def test_secure_mode_hash():
    user = {'key': 'Message'}
    assert offline_client.secure_mode_hash(user) == "aa747c502a898200f9e4fa21bac68136f886a0e27aec70ba06daf2e2a5cb5597"
