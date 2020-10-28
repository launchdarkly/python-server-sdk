from ldclient.client import LDClient, Config
from ldclient.event_processor import DefaultEventProcessor
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.impl.stubs import NullEventProcessor, NullUpdateProcessor
from ldclient.interfaces import UpdateProcessor
from ldclient.polling import PollingUpdateProcessor
from ldclient.streaming import StreamingUpdateProcessor
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

import logging
import pytest
from testing.stub_util import CapturingFeatureStore, MockEventProcessor, MockUpdateProcessor
from testing.sync_util import wait_until
import queue


unreachable_uri="http://fake"


user = {
    u'key': u'xyz',
    u'custom': {
        u'bizzle': u'def'
    }
}


def make_client(store = InMemoryFeatureStore()):
    return LDClient(config=Config(sdk_key = 'SDK_KEY',
                                  base_uri=unreachable_uri,
                                  events_uri=unreachable_uri,
                                  stream_uri=unreachable_uri,
                                  event_processor_class=MockEventProcessor,
                                  update_processor_class=MockUpdateProcessor,
                                  feature_store=store))


def make_offline_client():
    return LDClient(config=Config(sdk_key="secret",
                                  offline=True,
                                  base_uri=unreachable_uri,
                                  events_uri=unreachable_uri,
                                  stream_uri=unreachable_uri))


def make_ldd_client():
    return LDClient(config=Config(sdk_key="secret",
                                  use_ldd=True,
                                  base_uri=unreachable_uri,
                                  events_uri=unreachable_uri,
                                  stream_uri=unreachable_uri))


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
    e = c._event_processor._events.pop(0)
    c._event_processor._events = []
    return e


def count_events(c):
    n = len(c._event_processor._events)
    c._event_processor._events = []
    return n


def test_client_has_null_event_processor_if_offline():
    with make_offline_client() as client:
        assert isinstance(client._event_processor, NullEventProcessor)


def test_client_has_null_event_processor_if_send_events_off():
    config = Config(sdk_key="secret", base_uri=unreachable_uri,
                    update_processor_class = MockUpdateProcessor, send_events=False)
    with LDClient(config=config) as client:
        assert isinstance(client._event_processor, NullEventProcessor)


def test_client_has_normal_event_processor_in_ldd_mode():
    with make_ldd_client() as client:
        assert isinstance(client._event_processor, DefaultEventProcessor)


def test_client_has_null_update_processor_in_offline_mode():
    with make_offline_client() as client:
        assert isinstance(client._update_processor, NullUpdateProcessor)


def test_client_has_null_update_processor_in_ldd_mode():
    with make_ldd_client() as client:
        assert isinstance(client._update_processor, NullUpdateProcessor)


@pytest.mark.skip("Can't currently use a live stream processor in tests because its error logging will disrupt other tests.")
def test_client_has_streaming_processor_by_default():
    config = Config(sdk_key="secret", base_uri=unreachable_uri, stream_uri=unreachable_uri, send_events=False)
    with LDClient(config=config, start_wait=0) as client:
        assert isinstance(client._update_processor, StreamingUpdateProcessor)


@pytest.mark.skip("Can't currently use a live polling processor in tests because its error logging will disrupt other tests.")
def test_client_has_polling_processor_if_streaming_is_disabled():
    config = Config(sdk_key="secret", stream=False, base_uri=unreachable_uri, stream_uri=unreachable_uri, send_events=False)
    with LDClient(config=config, start_wait=0) as client:
        assert isinstance(client._update_processor, PollingUpdateProcessor)


def test_toggle_offline():
    with make_offline_client() as client:
        assert client.variation('feature.key', user, default=None) is None


def test_identify():
    with make_client() as client:
        client.identify(user)
        e = get_first_event(client)
        assert e['kind'] == 'identify' and e['key'] == u'xyz' and e['user'] == user


def test_identify_no_user():
    with make_client() as client:
        client.identify(None)
        assert count_events(client) == 0


def test_identify_no_user_key():
    with make_client() as client:
        client.identify({ 'name': 'nokey' })
        assert count_events(client) == 0


def test_track():
    with make_client() as client:
        client.track('my_event', user)
        e = get_first_event(client)
        assert e['kind'] == 'custom' and e['key'] == 'my_event' and e['user'] == user and e.get('data') is None and e.get('metricValue') is None


def test_track_with_data():
    with make_client() as client:
        client.track('my_event', user, 42)
        e = get_first_event(client)
        assert e['kind'] == 'custom' and e['key'] == 'my_event' and e['user'] == user and e['data'] == 42 and e.get('metricValue') is None


def test_track_with_metric_value():
    with make_client() as client:
        client.track('my_event', user, 42, 1.5)
        e = get_first_event(client)
        assert e['kind'] == 'custom' and e['key'] == 'my_event' and e['user'] == user and e['data'] == 42 and e.get('metricValue') == 1.5


def test_track_no_user():
    with make_client() as client:
        client.track('my_event', None)
        assert count_events(client) == 0


def test_track_no_user_key():
    with make_client() as client:
        client.track('my_event', { 'name': 'nokey' })
        assert count_events(client) == 0


def test_defaults():
    config=Config("SDK_KEY", base_uri="http://localhost:3000", defaults={"foo": "bar"}, offline=True)
    with LDClient(config=config) as client:
        assert "bar" == client.variation('foo', user, default=None)


def test_defaults_and_online():
    expected = "bar"
    my_client = LDClient(config=Config("SDK_KEY",
                                       base_uri="http://localhost:3000",
                                       defaults={"foo": expected},
                                       event_processor_class=MockEventProcessor,
                                       update_processor_class=MockUpdateProcessor,
                                       feature_store=InMemoryFeatureStore()))
    actual = my_client.variation('foo', user, default="originalDefault")
    assert actual == expected
    e = get_first_event(my_client)
    assert e['kind'] == 'feature' and e['key'] == u'foo' and e['user'] == user


def test_defaults_and_online_no_default():
    my_client = LDClient(config=Config("SDK_KEY",
                                       base_uri="http://localhost:3000",
                                       defaults={"foo": "bar"},
                                       event_processor_class=MockEventProcessor,
                                       update_processor_class=MockUpdateProcessor))
    assert "jim" == my_client.variation('baz', user, default="jim")
    e = get_first_event(my_client)
    assert e['kind'] == 'feature' and e['key'] == u'baz' and e['user'] == user


def test_no_defaults():
    with make_offline_client() as client:
        assert "bar" == client.variation('foo', user, default="bar")


def test_event_for_existing_feature():
    feature = make_off_flag_with_value('feature.key', 'value')
    feature['trackEvents'] = True
    feature['debugEventsUntilDate'] = 1000
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    with make_client(store) as client:
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
    with make_client(store) as client:
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


def test_event_for_existing_feature_with_tracked_rule():
    feature = {
        'key': 'feature.key',
        'version': 100,
        'salt': u'',
        'on': True,
        'rules': [
            {
                'clauses': [
                    { 'attribute': 'key', 'op': 'in', 'values': [ user['key'] ] }
                ],
                'variation': 0,
                'trackEvents': True,
                'id': 'rule_id'
            }
        ],
        'variations': [ 'value' ]
    }
    store = InMemoryFeatureStore()
    store.init({FEATURES: {feature['key']: feature}})
    client = make_client(store)
    assert 'value' == client.variation(feature['key'], user, default='default')
    e = get_first_event(client)
    assert (e['kind'] == 'feature' and
        e['key'] == feature['key'] and
        e['user'] == user and
        e['version'] == feature['version'] and
        e['value'] == 'value' and
        e['variation'] == 0 and
        e['reason'] == { 'kind': 'RULE_MATCH', 'ruleIndex': 0, 'ruleId': 'rule_id' } and
        e['default'] == 'default' and
        e['trackEvents'] == True and
        e.get('debugEventsUntilDate') is None)


def test_event_for_existing_feature_with_untracked_rule():
    feature = {
        'key': 'feature.key',
        'version': 100,
        'salt': u'',
        'on': True,
        'rules': [
            {
                'clauses': [
                    { 'attribute': 'key', 'op': 'in', 'values': [ user['key'] ] }
                ],
                'variation': 0,
                'trackEvents': False,
                'id': 'rule_id'
            }
        ],
        'variations': [ 'value' ]
    }
    store = InMemoryFeatureStore()
    store.init({FEATURES: {feature['key']: feature}})
    client = make_client(store)
    assert 'value' == client.variation(feature['key'], user, default='default')
    e = get_first_event(client)
    assert (e['kind'] == 'feature' and
        e['key'] == feature['key'] and
        e['user'] == user and
        e['version'] == feature['version'] and
        e['value'] == 'value' and
        e['variation'] == 0 and
        e.get('reason') is None and
        e['default'] == 'default' and
        e.get('trackEvents', False) == False and
        e.get('debugEventsUntilDate') is None)


def test_event_for_existing_feature_with_tracked_fallthrough():
    feature = {
        'key': 'feature.key',
        'version': 100,
        'salt': u'',
        'on': True,
        'rules': [],
        'fallthrough': { 'variation': 0 },
        'variations': [ 'value' ],
        'trackEventsFallthrough': True
    }
    store = InMemoryFeatureStore()
    store.init({FEATURES: {feature['key']: feature}})
    client = make_client(store)
    assert 'value' == client.variation(feature['key'], user, default='default')
    e = get_first_event(client)
    assert (e['kind'] == 'feature' and
        e['key'] == feature['key'] and
        e['user'] == user and
        e['version'] == feature['version'] and
        e['value'] == 'value' and
        e['variation'] == 0 and
        e['reason'] == { 'kind': 'FALLTHROUGH' } and
        e['default'] == 'default' and
        e['trackEvents'] == True and
        e.get('debugEventsUntilDate') is None)


def test_event_for_existing_feature_with_untracked_fallthrough():
    feature = {
        'key': 'feature.key',
        'version': 100,
        'salt': u'',
        'on': True,
        'rules': [],
        'fallthrough': { 'variation': 0 },
        'variations': [ 'value' ],
        'trackEventsFallthrough': False
    }
    store = InMemoryFeatureStore()
    store.init({FEATURES: {feature['key']: feature}})
    client = make_client(store)
    assert 'value' == client.variation(feature['key'], user, default='default')
    e = get_first_event(client)
    assert (e['kind'] == 'feature' and
        e['key'] == feature['key'] and
        e['user'] == user and
        e['version'] == feature['version'] and
        e['value'] == 'value' and
        e['variation'] == 0 and
        e.get('reason') is None and
        e['default'] == 'default' and
        e.get('trackEvents', False) == False and
        e.get('debugEventsUntilDate') is None)


def test_event_for_unknown_feature():
    store = InMemoryFeatureStore()
    store.init({FEATURES: {}})
    with make_client(store) as client:
        assert 'default' == client.variation('feature.key', user, default='default')
        e = get_first_event(client)
        assert (e['kind'] == 'feature' and
            e['key'] == 'feature.key' and
            e['user'] == user and
            e['value'] == 'default' and
            e.get('variation') is None and
            e['default'] == 'default')


def test_event_for_existing_feature_with_no_user():
    feature = make_off_flag_with_value('feature.key', 'value')
    feature['trackEvents'] = True
    feature['debugEventsUntilDate'] = 1000
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    with make_client(store) as client:
        assert 'default' == client.variation('feature.key', None, default='default')
        e = get_first_event(client)
        assert (e['kind'] == 'feature' and
            e['key'] == 'feature.key' and
            e.get('user') is None and
            e['version'] == feature['version'] and
            e['value'] == 'default' and
            e.get('variation') is None and
            e['default'] == 'default' and
            e['trackEvents'] == True and
            e['debugEventsUntilDate'] == 1000)


def test_event_for_existing_feature_with_no_user_key():
    feature = make_off_flag_with_value('feature.key', 'value')
    feature['trackEvents'] = True
    feature['debugEventsUntilDate'] = 1000
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    with make_client(store) as client:
        bad_user = { u'name': u'Bob' }
        assert 'default' == client.variation('feature.key', bad_user, default='default')
        e = get_first_event(client)
        assert (e['kind'] == 'feature' and
            e['key'] == 'feature.key' and
            e['user'] == bad_user and
            e['version'] == feature['version'] and
            e['value'] == 'default' and
            e.get('variation') is None and
            e['default'] == 'default' and
            e['trackEvents'] == True and
            e['debugEventsUntilDate'] == 1000)


def test_secure_mode_hash():
    user = {'key': 'Message'}
    with make_offline_client() as client:
        assert client.secure_mode_hash(user) == "aa747c502a898200f9e4fa21bac68136f886a0e27aec70ba06daf2e2a5cb5597"


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
    keys = list(data.keys())
    values = list(data.values())

    assert keys[0] == SEGMENTS
    assert len(values[0]) == len(dependency_ordering_test_data[SEGMENTS])

    assert keys[1] == FEATURES
    flags_map = values[1]
    flags_list = list(flags_map.values())
    assert len(flags_list) == len(dependency_ordering_test_data[FEATURES])
    for item_index, item in enumerate(flags_list):
        for prereq in item.get("prerequisites", []):
            prereq_item = flags_map[prereq["key"]]
            prereq_index = flags_list.index(prereq_item)
            if prereq_index > item_index:
                all_keys = (f["key"] for f in flags_list)
                raise Exception("%s depends on %s, but %s was listed first; keys in order are [%s]" %
                    (item["key"], prereq["key"], item["key"], ", ".join(all_keys)))
