from ldclient.client import LDClient, Config, Context
from ldclient.impl.events.event_processor import DefaultEventProcessor
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.impl.events.types import EventInputCustom, EventInputEvaluation, EventInputIdentify
from ldclient.impl.stubs import NullEventProcessor
from ldclient.versioned_data_kind import FEATURES

from testing.builders import *
from testing.stub_util import MockUpdateProcessor
from testing.test_ldclient import context, make_client, make_ldd_client, make_offline_client, unreachable_uri, user


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


def test_identify():
    with make_client() as client:
        client.identify(context)
        e = get_first_event(client)
        assert isinstance(e, EventInputIdentify)
        assert e.context == context


def test_identify_with_user_dict():
    with make_client() as client:
        client.identify(user)
        e = get_first_event(client)
        assert isinstance(e, EventInputIdentify)
        assert e.context == context


def test_identify_no_user():
    with make_client() as client:
        client.identify(None)
        assert count_events(client) == 0


def test_identify_no_user_key():
    with make_client() as client:
        client.identify({ 'name': 'nokey' })
        assert count_events(client) == 0


def test_identify_invalid_context():
    with make_client() as client:
        client.identify(Context.create(''))
        assert count_events(client) == 0


def test_track():
    with make_client() as client:
        client.track('my_event', context)
        e = get_first_event(client)
        assert isinstance(e, EventInputCustom)
        assert e.key == 'my_event'
        assert e.context == context
        assert e.data is None
        assert e.metric_value is None


def test_track_with_user_dict():
    with make_client() as client:
        client.track('my_event', user)
        e = get_first_event(client)
        assert isinstance(e, EventInputCustom)
        assert e.key == 'my_event'
        assert e.context == context
        assert e.data is None
        assert e.metric_value is None


def test_track_with_data():
    with make_client() as client:
        client.track('my_event', context, 42)
        e = get_first_event(client)
        assert isinstance(e, EventInputCustom)
        assert e.key == 'my_event'
        assert e.context == context
        assert e.data == 42
        assert e.metric_value is None


def test_track_with_metric_value():
    with make_client() as client:
        client.track('my_event', context, 42, 1.5)
        e = get_first_event(client)
        assert isinstance(e, EventInputCustom)
        assert e.key == 'my_event'
        assert e.context == context
        assert e.data == 42
        assert e.metric_value == 1.5


def test_track_no_context():
    with make_client() as client:
        client.track('my_event', None)
        assert count_events(client) == 0


def test_track_invalid_context():
    with make_client() as client:
        client.track('my_event', Context.create(''))
        assert count_events(client) == 0


def test_event_for_existing_feature():
    feature = build_off_flag_with_value('feature.key', 'value').track_events(True).build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {feature.key: feature.to_json_dict()}})
    with make_client(store) as client:
        assert 'value' == client.variation(feature.key, context, default='default')
        e = get_first_event(client)
        assert isinstance(e, EventInputEvaluation)
        assert (e.key == feature.key and
            e.flag == feature and
            e.context == context and
            e.value == 'value' and
            e.variation == 0 and
            e.reason is None and
            e.default_value == 'default' and
            e.track_events is True)


def test_event_for_existing_feature_with_reason():
    feature = build_off_flag_with_value('feature.key', 'value').track_events(True).build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {feature.key: feature.to_json_dict()}})
    with make_client(store) as client:
        assert 'value' == client.variation_detail(feature.key, context, default='default').value
        e = get_first_event(client)
        assert isinstance(e, EventInputEvaluation)
        assert (e.key == feature.key and
            e.flag == feature and
            e.context == context and
            e.value == 'value' and
            e.variation == 0 and
            e.reason == {'kind': 'OFF'} and
            e.default_value == 'default' and
            e.track_events is True)


def test_event_for_existing_feature_with_tracked_rule():
    feature = FlagBuilder('feature.key').version(100).on(True).variations('value') \
        .rules(
            FlagRuleBuilder().variation(0).id('rule_id').track_events(True) \
                .clauses(make_clause(None, 'key', 'in', user['key'])) \
                .build()
        ) \
        .build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {feature.key: feature.to_json_dict()}})
    client = make_client(store)
    assert 'value' == client.variation(feature.key, context, default='default')
    e = get_first_event(client)
    assert isinstance(e, EventInputEvaluation)
    assert (e.key == feature.key and
        e.flag == feature and
        e.context == context and
        e.value == 'value' and
        e.variation == 0 and
        e.reason == { 'kind': 'RULE_MATCH', 'ruleIndex': 0, 'ruleId': 'rule_id' } and
        e.default_value == 'default' and
        e.track_events is True)


def test_event_for_existing_feature_with_untracked_rule():
    feature = FlagBuilder('feature.key').version(100).on(True).variations('value') \
        .rules(
            FlagRuleBuilder().variation(0).id('rule_id') \
                .clauses(make_clause(None, 'key', 'in', user['key'])) \
                .build()
        ) \
        .build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {feature.key: feature.to_json_dict()}})
    client = make_client(store)
    assert 'value' == client.variation(feature.key, context, default='default')
    e = get_first_event(client)
    assert isinstance(e, EventInputEvaluation)
    assert (e.key == feature.key and
        e.flag == feature and
        e.context == context and
        e.value == 'value' and
        e.variation == 0 and
        e.reason is None and
        e.default_value == 'default' and
        e.track_events is False)


def test_event_for_existing_feature_with_tracked_fallthrough():
    feature = FlagBuilder('feature.key').version(100).on(True).variations('value') \
        .fallthrough_variation(0).track_events_fallthrough(True) \
        .build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {feature.key: feature.to_json_dict()}})
    client = make_client(store)
    assert 'value' == client.variation(feature.key, context, default='default')
    e = get_first_event(client)
    assert isinstance(e, EventInputEvaluation)
    assert (e.key == feature.key and
        e.flag == feature and
        e.context == context and
        e.value == 'value' and
        e.variation == 0 and
        e.reason == { 'kind': 'FALLTHROUGH' } and
        e.default_value == 'default' and
        e.track_events is True)


def test_event_for_existing_feature_with_untracked_fallthrough():
    feature = FlagBuilder('feature.key').version(100).on(True).variations('value') \
        .fallthrough_variation(0) \
        .build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {feature.key: feature.to_json_dict()}})
    client = make_client(store)
    detail = client.variation_detail(feature.key, context, default='default')
    assert 'value' == detail.value
    e = get_first_event(client)
    assert isinstance(e, EventInputEvaluation)
    assert (e.key == feature.key and
        e.flag == feature and
        e.context == context and
        e.value == 'value' and
        e.variation == 0 and
        e.reason == { 'kind': 'FALLTHROUGH' } and
        e.default_value == 'default' and
        e.track_events is False)


def test_event_for_unknown_feature():
    store = InMemoryFeatureStore()
    store.init({FEATURES: {}})
    with make_client(store) as client:
        assert 'default' == client.variation('feature.key', context, default='default')
        e = get_first_event(client)
        assert isinstance(e, EventInputEvaluation)
        assert (e.key == 'feature.key' and
            e.flag is None and
            e.context == context and
            e.value == 'default' and
            e.variation is None and
            e.reason is None and
            e.default_value == 'default' and
            e.track_events is False)


def test_no_event_for_existing_feature_with_no_context():
    feature = build_off_flag_with_value('feature.key', 'value').track_events(True).build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {feature.key: feature.to_json_dict()}})
    with make_client(store) as client:
        assert 'default' == client.variation(feature.key, None, default='default')
        assert count_events(client) == 0


def test_no_event_for_existing_feature_with_invalid_context():
    feature = build_off_flag_with_value('feature.key', 'value').track_events(True).build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {feature.key: feature.to_json_dict()}})
    with make_client(store) as client:
        bad_context = Context.create('')
        assert 'default' == client.variation('feature.key', bad_context, default='default')
        assert count_events(client) == 0
