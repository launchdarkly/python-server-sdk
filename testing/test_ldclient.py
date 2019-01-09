from builtins import object
from ldclient.client import LDClient, Config
from ldclient.event_processor import NullEventProcessor
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.interfaces import UpdateProcessor
from ldclient.versioned_data_kind import FEATURES, SEGMENTS
import pytest
from testing.stub_util import CapturingFeatureStore, MockEventProcessor, MockUpdateProcessor
from testing.sync_util import wait_until

try:
    import queue
except:
    import Queue as queue


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


def make_off_flag_with_value(key, value):
    return {
        u'key': key,
        u'version': 100,
        u'salt': u'',
        u'on': False,
        u'variations': [value],
        u'offVariation': 0
    }


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
    feature = make_off_flag_with_value('feature.key', 'value')
    feature['trackEvents'] = True
    feature['debugEventsUntilDate'] = 1000
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    assert 'value' == client.variation('feature.key', user, default='default')
    e = get_first_event(client)
    assert (e['kind'] == 'feature' and
        e['key'] == 'feature.key' and
        e['user'] == user and
        e['version'] == feature['version'] and
        e['value'] == 'value' and
        e['variation'] == 0 and
        e.get('reason') is None and
        e['default'] == 'default' and
        e['trackEvents'] == True and
        e['debugEventsUntilDate'] == 1000)


def test_event_for_existing_feature_with_reason():
    feature = make_off_flag_with_value('feature.key', 'value')
    feature['trackEvents'] = True
    feature['debugEventsUntilDate'] = 1000
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    assert 'value' == client.variation_detail('feature.key', user, default='default').value
    e = get_first_event(client)
    assert (e['kind'] == 'feature' and
        e['key'] == 'feature.key' and
        e['user'] == user and
        e['version'] == feature['version'] and
        e['value'] == 'value' and
        e['variation'] == 0 and
        e['reason'] == {'kind': 'OFF'} and
        e['default'] == 'default' and
        e['trackEvents'] == True and
        e['debugEventsUntilDate'] == 1000)


def test_event_for_unknown_feature():
    store = InMemoryFeatureStore()
    store.init({FEATURES: {}})
    client = make_client(store)
    assert 'default' == client.variation('feature.key', user, default='default')
    e = get_first_event(client)
    assert (e['kind'] == 'feature' and
        e['key'] == 'feature.key' and
        e['user'] == user and
        e['value'] == 'default' and
        e['variation'] == None and
        e['default'] == 'default')


def test_event_for_existing_feature_with_no_user():
    feature = make_off_flag_with_value('feature.key', 'value')
    feature['trackEvents'] = True
    feature['debugEventsUntilDate'] = 1000
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    assert 'default' == client.variation('feature.key', None, default='default')
    e = get_first_event(client)
    assert (e['kind'] == 'feature' and
        e['key'] == 'feature.key' and
        e['user'] == None and
        e['version'] == feature['version'] and
        e['value'] == 'default' and
        e['variation'] == None and
        e['default'] == 'default' and
        e['trackEvents'] == True and
        e['debugEventsUntilDate'] == 1000)


def test_event_for_existing_feature_with_no_user_key():
    feature = make_off_flag_with_value('feature.key', 'value')
    feature['trackEvents'] = True
    feature['debugEventsUntilDate'] = 1000
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    bad_user = { u'name': u'Bob' }
    assert 'default' == client.variation('feature.key', bad_user, default='default')
    e = get_first_event(client)
    assert (e['kind'] == 'feature' and
        e['key'] == 'feature.key' and
        e['user'] == bad_user and
        e['version'] == feature['version'] and
        e['value'] == 'default' and
        e['variation'] == None and
        e['default'] == 'default' and
        e['trackEvents'] == True and
        e['debugEventsUntilDate'] == 1000)


def test_secure_mode_hash():
    user = {'key': 'Message'}
    assert offline_client.secure_mode_hash(user) == "aa747c502a898200f9e4fa21bac68136f886a0e27aec70ba06daf2e2a5cb5597"


dependency_ordering_test_data = {
    FEATURES: {
        "a": { "key": "a", "prerequisites": [ { "key": "b" }, { "key": "c" } ] },
        "b": { "key": "b", "prerequisites": [ { "key": "c" }, { "key": "e" } ] },
        "c": { "key": "c" },
        "d": { "key": "d" },
        "e": { "key": "e" },
        "f": { "key": "f" }
    },
    SEGMENTS: {
        "o": { "key": "o" }
    }
}

class DependencyOrderingDataUpdateProcessor(UpdateProcessor):
    def __init__(self, config, store, ready):
        store.init(dependency_ordering_test_data)
        ready.set()

    def start(self):
        pass

    def initialized(self):
        return True


def test_store_data_set_ordering():
    store = CapturingFeatureStore()
    config = Config(sdk_key = 'SDK_KEY', send_events=False, feature_store=store,
                    update_processor_class=DependencyOrderingDataUpdateProcessor)
    LDClient(config=config)

    data = store.received_data
    assert data is not None
    assert len(data) == 2

    assert data.keys()[0] == SEGMENTS
    assert len(data.values()[0]) == len(dependency_ordering_test_data[SEGMENTS])

    assert data.keys()[1] == FEATURES
    flags_map = data.values()[1]
    flags_list = flags_map.values()
    assert len(flags_list) == len(dependency_ordering_test_data[FEATURES])
    for item_index, item in enumerate(flags_list):
        for prereq in item.get("prerequisites", []):
            prereq_item = flags_map[prereq["key"]]
            prereq_index = flags_list.index(prereq_item)
            if prereq_index > item_index:
                all_keys = (f["key"] for f in flags_list)
                raise Exception("%s depends on %s, but %s was listed first; keys in order are [%s]" %
                    (item["key"], prereq["key"], item["key"], ", ".join(all_keys)))
