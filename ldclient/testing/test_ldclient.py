import pytest

from ldclient.client import Config, Context, LDClient
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.impl.datasource.polling import PollingUpdateProcessor
from ldclient.impl.datasource.streaming import StreamingUpdateProcessor
from ldclient.impl.stubs import NullUpdateProcessor
from ldclient.interfaces import UpdateProcessor
from ldclient.testing.builders import *
from ldclient.testing.stub_util import (CapturingFeatureStore,
                                        MockEventProcessor,
                                        MockUpdateProcessor)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

unreachable_uri = "http://fake"


context = Context.builder('xyz').set('bizzle', 'def').build()
user = Context.from_dict({u'key': u'xyz', u'kind': u'user', u'bizzle': u'def'})

anonymous_user = Context.from_dict({u'key': u'abc', u'kind': u'user', u'anonymous': True})


def make_client(store=InMemoryFeatureStore()):
    return LDClient(
        config=Config(
            sdk_key='SDK_KEY',
            base_uri=unreachable_uri,
            events_uri=unreachable_uri,
            stream_uri=unreachable_uri,
            event_processor_class=MockEventProcessor,
            update_processor_class=MockUpdateProcessor,
            feature_store=store,
        )
    )


def make_offline_client():
    return LDClient(config=Config(sdk_key="secret", offline=True, base_uri=unreachable_uri, events_uri=unreachable_uri, stream_uri=unreachable_uri))


def make_ldd_client():
    return LDClient(config=Config(sdk_key="secret", use_ldd=True, base_uri=unreachable_uri, events_uri=unreachable_uri, stream_uri=unreachable_uri))


def get_first_event(c):
    e = c._event_processor._events.pop(0)
    c._event_processor._events = []
    return e


def count_events(c):
    n = len(c._event_processor._events)
    c._event_processor._events = []
    return n


def test_client_has_null_update_processor_in_offline_mode():
    with make_offline_client() as client:
        assert isinstance(client._update_processor, NullUpdateProcessor)


def test_client_has_null_update_processor_in_ldd_mode():
    with make_ldd_client() as client:
        assert isinstance(client._update_processor, NullUpdateProcessor)


def test_client_has_streaming_processor_by_default():
    config = Config(sdk_key="secret", base_uri=unreachable_uri, stream_uri=unreachable_uri, send_events=False)
    with LDClient(config=config, start_wait=0) as client:
        assert isinstance(client._update_processor, StreamingUpdateProcessor)


def test_client_has_polling_processor_if_streaming_is_disabled():
    config = Config(sdk_key="secret", stream=False, base_uri=unreachable_uri, stream_uri=unreachable_uri, send_events=False)
    with LDClient(config=config, start_wait=0) as client:
        assert isinstance(client._update_processor, PollingUpdateProcessor)


def test_toggle_offline():
    with make_offline_client() as client:
        assert client.variation('feature.key', user, default=None) is None


def test_defaults():
    config = Config("SDK_KEY", base_uri="http://localhost:3000", defaults={"foo": "bar"}, offline=True)
    with LDClient(config=config) as client:
        assert "bar" == client.variation('foo', user, default=None)


def test_defaults_and_online():
    expected = "bar"
    my_client = LDClient(
        config=Config(
            "SDK_KEY",
            base_uri="http://localhost:3000",
            defaults={"foo": expected},
            event_processor_class=MockEventProcessor,
            update_processor_class=MockUpdateProcessor,
            feature_store=InMemoryFeatureStore(),
        )
    )
    actual = my_client.variation('foo', user, default="originalDefault")
    assert actual == expected


def test_defaults_and_online_no_default():
    my_client = LDClient(config=Config("SDK_KEY", base_uri="http://localhost:3000", defaults={"foo": "bar"}, event_processor_class=MockEventProcessor, update_processor_class=MockUpdateProcessor))
    assert "jim" == my_client.variation('baz', user, default="jim")


def test_no_defaults():
    with make_offline_client() as client:
        assert "bar" == client.variation('foo', user, default="bar")


def test_secure_mode_hash():
    context_to_hash = Context.create('Message')
    equivalent_user_to_hash = Context.from_dict({'key': 'Message', 'kind': 'user'})
    expected_hash = "aa747c502a898200f9e4fa21bac68136f886a0e27aec70ba06daf2e2a5cb5597"
    with make_offline_client() as client:
        assert client.secure_mode_hash(context_to_hash) == expected_hash
        assert client.secure_mode_hash(equivalent_user_to_hash) == expected_hash


dependency_ordering_test_data = {
    FEATURES: {
        "a": {"key": "a", "prerequisites": [{"key": "b"}, {"key": "c"}]},
        "b": {"key": "b", "prerequisites": [{"key": "c"}, {"key": "e"}]},
        "c": {"key": "c"},
        "d": {"key": "d"},
        "e": {"key": "e"},
        "f": {"key": "f"},
    },
    SEGMENTS: {"o": {"key": "o"}},
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
    config = Config(sdk_key='SDK_KEY', send_events=False, feature_store=store, update_processor_class=DependencyOrderingDataUpdateProcessor)
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
                raise Exception("%s depends on %s, but %s was listed first; keys in order are [%s]" % (item["key"], prereq["key"], item["key"], ", ".join(all_keys)))
