import logging

import pytest

from ldclient import datasystem
from ldclient.client import Config, Context, LDClient
from ldclient.datasystem import DataStoreMode
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.impl.datasourcev2.polling import (
    FallbackToFDv1PollingDataSourceBuilder,
    PollingDataSourceBuilder
)
from ldclient.impl.datasourcev2.streaming import StreamingDataSourceBuilder
from ldclient.impl.util import _Success
from ldclient.integrations.test_datav2 import TestDataV2
from ldclient.interfaces import (
    Basis,
    BasisResult,
    ChangeSetBuilder,
    IntentCode,
    ObjectKind,
    Selector,
    SelectorStore
)
from ldclient.testing.builders import *
from ldclient.testing.stub_util import (
    CapturingFeatureStore,
    MockEventProcessor
)
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
            data_system=datasystem.daemon(store).build(),
        )
    )


def make_offline_client():
    return LDClient(config=Config(sdk_key="secret", offline=True, base_uri=unreachable_uri, events_uri=unreachable_uri, stream_uri=unreachable_uri))


def make_daemon_client(store=None, **kwargs):
    config = Config(
        sdk_key="secret",
        data_system=datasystem.daemon(store or InMemoryFeatureStore()).build(),
        base_uri=unreachable_uri,
        events_uri=unreachable_uri,
        stream_uri=unreachable_uri,
        diagnostic_opt_out=True,
        **kwargs,
    )
    return LDClient(config=config)


class _ClosableStore(InMemoryFeatureStore):
    def __init__(self):
        super().__init__()
        self.closed = False

    def close(self):
        self.closed = True


def get_first_event(c):
    e = c._event_processor._events.pop(0)
    c._event_processor._events = []
    return e


def count_events(c):
    n = len(c._event_processor._events)
    c._event_processor._events = []
    return n


def test_client_uses_the_default_data_system_when_none_is_set():
    config = Config(sdk_key="secret", base_uri=unreachable_uri, stream_uri=unreachable_uri, send_events=False)
    with LDClient(config=config, start_wait=0) as client:
        dsc = client._data_system_config
        assert [type(b) for b in dsc.initializers] == [PollingDataSourceBuilder]
        assert [type(b) for b in dsc.synchronizers] == [StreamingDataSourceBuilder, PollingDataSourceBuilder]
        assert isinstance(dsc.fdv1_fallback_synchronizer, FallbackToFDv1PollingDataSourceBuilder)
        assert dsc.data_store is None


def test_client_uses_the_configured_data_system():
    dsc = datasystem.polling().build()
    config = Config(sdk_key="secret", base_uri=unreachable_uri, send_events=False, data_system=dsc)
    with LDClient(config=config, start_wait=0) as client:
        assert client._data_system_config is dsc


def test_daemon_mode_client_is_initialized_with_an_empty_store():
    with make_daemon_client(send_events=False) as client:
        assert client.is_initialized()


def test_daemon_mode_logs_daemon_mode(caplog):
    caplog.set_level(logging.INFO)
    with make_daemon_client(send_events=False):
        pass
    assert "Started LaunchDarkly Client in daemon mode" in caplog.text


def test_offline_mode_with_a_daemon_config_logs_only_offline_mode(caplog):
    caplog.set_level(logging.INFO)
    with make_daemon_client(send_events=False, offline=True):
        pass
    assert "Started LaunchDarkly Client in offline mode" in caplog.text
    assert "daemon mode" not in caplog.text


def test_data_system_with_no_sources_and_no_store_logs_a_warning(caplog):
    config = Config(sdk_key="secret", send_events=False, data_system=datasystem.custom().build())
    with LDClient(config=config, start_wait=0):
        pass
    assert "daemon mode" not in caplog.text
    assert any(
        r.levelname == "WARNING" and "no data sources and no persistent store" in r.message
        for r in caplog.records
    )


def test_close_closes_the_persistent_store_in_daemon_mode():
    store = _ClosableStore()
    client = make_daemon_client(store, send_events=False)
    client.close()
    assert store.closed


def test_close_closes_the_persistent_store_with_a_synchronizer():
    store = _ClosableStore()
    dsc = datasystem.custom() \
        .synchronizers(TestDataV2.data_source().builder) \
        .data_store(store, DataStoreMode.READ_WRITE) \
        .build()
    client = LDClient(config=Config(sdk_key="secret", send_events=False, data_system=dsc))
    client.close()
    assert store.closed


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
            data_system=datasystem.daemon(InMemoryFeatureStore()).build(),
        )
    )
    actual = my_client.variation('foo', user, default="originalDefault")
    assert actual == expected


def test_defaults_and_online_no_default():
    my_client = LDClient(config=Config("SDK_KEY", base_uri="http://localhost:3000", defaults={"foo": "bar"}, event_processor_class=MockEventProcessor, data_system=datasystem.daemon(InMemoryFeatureStore()).build()))
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


# Items carry the fields the in-memory store needs to decode them.
dependency_ordering_test_data = {
    FEATURES: {
        "a": {"key": "a", "version": 1, "prerequisites": [{"key": "b", "variation": 0}, {"key": "c", "variation": 0}]},
        "b": {"key": "b", "version": 1, "prerequisites": [{"key": "c", "variation": 0}, {"key": "e", "variation": 0}]},
        "c": {"key": "c", "version": 1},
        "d": {"key": "d", "version": 1},
        "e": {"key": "e", "version": 1},
        "f": {"key": "f", "version": 1},
    },
    SEGMENTS: {"o": {"key": "o", "version": 1}},
}


class DependencyOrderingDataInitializer:
    # Returns the unordered test data set as a persistable full transfer.
    @property
    def name(self) -> str:
        return "DependencyOrderingDataInitializer"

    def fetch(self, ss: SelectorStore) -> BasisResult:
        builder = ChangeSetBuilder()
        builder.start(IntentCode.TRANSFER_FULL)
        for kind, object_kind in ((FEATURES, ObjectKind.FLAG), (SEGMENTS, ObjectKind.SEGMENT)):
            for key, item in dependency_ordering_test_data[kind].items():
                builder.add_put(object_kind, key, item['version'], item)
        change_set = builder.finish(Selector.new_selector("state", 1))
        return _Success(Basis(change_set=change_set, persist=True, environment_id=None))


class DependencyOrderingDataInitializerBuilder:
    def build(self, config):
        return DependencyOrderingDataInitializer()


def test_store_data_set_ordering():
    store = CapturingFeatureStore()
    # The data system sorts the data set when it writes to a READ_WRITE store.
    datasystem_config = datasystem.custom() \
        .initializers([DependencyOrderingDataInitializerBuilder()]) \
        .data_store(store, DataStoreMode.READ_WRITE) \
        .build()
    config = Config(sdk_key='SDK_KEY', send_events=False, data_system=datasystem_config)
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
