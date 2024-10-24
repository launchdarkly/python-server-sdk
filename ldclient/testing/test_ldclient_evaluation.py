import time

from ldclient.client import Config, Context, LDClient
from ldclient.config import BigSegmentsConfig
from ldclient.evaluation import BigSegmentsStatus, EvaluationDetail
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.impl.big_segments import _hash_for_user_key
from ldclient.impl.evaluator import _make_big_segment_ref
from ldclient.interfaces import FeatureStore
from ldclient.testing.builders import *
from ldclient.testing.mock_components import MockBigSegmentStore
from ldclient.testing.stub_util import MockEventProcessor, MockUpdateProcessor
from ldclient.testing.test_ldclient import make_client, user
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

flag1 = {'key': 'key1', 'version': 100, 'on': False, 'offVariation': 0, 'variations': ['value1'], 'trackEvents': False}
flag2 = {'key': 'key2', 'version': 200, 'on': False, 'offVariation': 1, 'variations': ['x', 'value2'], 'trackEvents': True, 'debugEventsUntilDate': 1000}


class ErroringFeatureStore(FeatureStore):
    def get(self, kind, key, callback=lambda x: x):
        raise NotImplementedError()

    def all(self, kind, callback=lambda x: x):
        raise NotImplementedError()

    def upsert(self, kind, item):
        pass

    def delete(self, key, version):
        pass

    def init(self, data):
        pass

    @property
    def initialized(self):
        return True


def get_log_lines(caplog, level):
    loglines = caplog.records
    if callable(loglines):
        # records() is a function in older versions of the caplog plugin
        loglines = loglines()
    return [line.message for line in loglines if line.levelname == level]


def test_variation_for_existing_feature():
    feature = build_off_flag_with_value('feature.key', 'value').build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    assert 'value' == client.variation('feature.key', user, default='default')


def test_variation_passes_context_to_evaluator():
    c = Context.create('userkey')
    feature = FlagBuilder('feature.key').on(True).variations('wrong', 'right').target(1, 'userkey').build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    assert 'right' == client.variation('feature.key', c, default='default')


def test_variation_for_unknown_feature():
    store = InMemoryFeatureStore()
    client = make_client(store)
    assert 'default' == client.variation('feature.key', user, default='default')


def test_variation_when_user_has_no_key():
    feature = build_off_flag_with_value('feature.key', 'value').build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    assert 'default' == client.variation('feature.key', Context.from_dict({}), default='default')


def test_variation_for_invalid_context():
    c = Context.create('')
    feature = build_off_flag_with_value('feature.key', 'value').build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    assert 'default' == client.variation('feature.key', c, default='default')


def test_variation_for_flag_that_evaluates_to_none():
    empty_flag = FlagBuilder('feature.key').on(False).build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': empty_flag}})
    client = make_client(store)
    assert 'default' == client.variation('feature.key', user, default='default')


def test_variation_detail_for_existing_feature():
    feature = build_off_flag_with_value('feature.key', 'value').build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    expected = EvaluationDetail('value', 0, {'kind': 'OFF'})
    assert expected == client.variation_detail('feature.key', user, default='default')


def test_variation_detail_for_unknown_feature():
    store = InMemoryFeatureStore()
    client = make_client(store)
    expected = EvaluationDetail('default', None, {'kind': 'ERROR', 'errorKind': 'FLAG_NOT_FOUND'})
    assert expected == client.variation_detail('feature.key', user, default='default')


def test_variation_detail_when_user_has_no_key():
    feature = build_off_flag_with_value('feature.key', 'value').build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    expected = EvaluationDetail('default', None, {'kind': 'ERROR', 'errorKind': 'USER_NOT_SPECIFIED'})
    assert expected == client.variation_detail('feature.key', Context.from_dict({}), default='default')


def test_variation_detail_for_flag_that_evaluates_to_none():
    empty_flag = FlagBuilder('feature.key').on(False).build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': empty_flag}})
    client = make_client(store)
    expected = EvaluationDetail('default', None, {'kind': 'OFF'})
    actual = client.variation_detail('feature.key', user, default='default')
    assert expected == actual
    assert actual.is_default_value() is True


def test_variation_when_feature_store_throws_error(caplog):
    store = ErroringFeatureStore()
    client = make_client(store)
    assert client.variation('feature.key', Context.from_dict({"key": "user", "kind": "user"}), default='default') == 'default'
    errlog = get_log_lines(caplog, 'ERROR')
    assert errlog == ['Unexpected error while retrieving feature flag "feature.key": NotImplementedError()']


def test_variation_detail_when_feature_store_throws_error(caplog):
    store = ErroringFeatureStore()
    client = make_client(store)
    expected = EvaluationDetail('default', None, {'kind': 'ERROR', 'errorKind': 'EXCEPTION'})
    actual = client.variation_detail('feature.key', Context.from_dict({"key": "user", "kind": "user"}), default='default')
    assert expected == actual
    assert actual.is_default_value() is True
    errlog = get_log_lines(caplog, 'ERROR')
    assert errlog == ['Unexpected error while retrieving feature flag "feature.key": NotImplementedError()']


def test_flag_using_big_segment():
    segment = SegmentBuilder('segkey').unbounded(True).generation(1).build()
    flag = make_boolean_flag_matching_segment(segment)
    store = InMemoryFeatureStore()
    store.init({FEATURES: {flag['key']: flag}, SEGMENTS: {segment['key']: segment}})
    segstore = MockBigSegmentStore()
    segstore.setup_metadata_always_up_to_date()
    segstore.setup_membership(_hash_for_user_key(user['key']), {_make_big_segment_ref(segment): True})
    config = Config(sdk_key='SDK_KEY', feature_store=store, big_segments=BigSegmentsConfig(store=segstore), event_processor_class=MockEventProcessor, update_processor_class=MockUpdateProcessor)
    with LDClient(config) as client:
        detail = client.variation_detail(flag['key'], user, False)
        assert detail.value is True
        assert detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.HEALTHY


def test_all_flags_returns_values():
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'key1': flag1, 'key2': flag2}})
    client = make_client(store)
    result = client.all_flags_state(user).to_values_map()
    assert result == {'key1': 'value1', 'key2': 'value2'}


def test_all_flags_returns_none_if_user_has_no_key():
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'key1': flag1, 'key2': flag2}})
    client = make_client(store)
    result = client.all_flags_state(Context.from_dict({}))
    assert not result.valid


def test_all_flags_returns_none_if_feature_store_throws_error(caplog):
    store = ErroringFeatureStore()
    client = make_client(store)
    assert not client.all_flags_state(Context.from_dict({"key": "user", "kind": "user"})).valid
    errlog = get_log_lines(caplog, 'ERROR')
    assert errlog == ['Unable to read flags for all_flag_state: NotImplementedError()']


def test_all_flags_state_returns_state():
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'key1': flag1, 'key2': flag2}})
    client = make_client(store)
    state = client.all_flags_state(user)
    assert state.valid
    result = state.to_json_dict()
    assert result == {
        'key1': 'value1',
        'key2': 'value2',
        '$flagsState': {'key1': {'variation': 0, 'version': 100}, 'key2': {'variation': 1, 'version': 200, 'trackEvents': True, 'debugEventsUntilDate': 1000}},
        '$valid': True,
    }


def test_all_flags_state_only_includes_top_level_prereqs():
    store = InMemoryFeatureStore()
    store.init(
        {
            FEATURES: {
                'top-level-has-prereqs-1': {
                    'key': 'top-level-has-prereqs-1',
                    'version': 100,
                    'on': True,
                    'fallthrough': {'variation': 0},
                    'variations': ['value'],
                    'prerequisites': [{'key': 'prereq1', 'variation': 0}, {'key': 'prereq2', 'variation': 0}],
                },
                'top-level-has-prereqs-2': {
                    'key': 'top-level-has-prereqs-2',
                    'version': 100,
                    'on': True,
                    'fallthrough': {'variation': 0},
                    'variations': ['value'],
                    'prerequisites': [{'key': 'prereq3', 'variation': 0}],
                },
                'prereq1': {
                    'key': 'prereq1',
                    'version': 200,
                    'on': True,
                    'fallthrough': {'variation': 0},
                    'variations': ['value'],
                },
                'prereq2': {
                    'key': 'prereq2',
                    'version': 200,
                    'on': True,
                    'fallthrough': {'variation': 0},
                    'variations': ['value'],
                },
                'prereq3': {
                    'key': 'prereq3',
                    'version': 200,
                    'on': True,
                    'fallthrough': {'variation': 0},
                    'variations': ['value'],
                },
            }
        }
    )
    client = make_client(store)
    state = client.all_flags_state(user)
    assert state.valid
    result = state.to_json_dict()
    assert result == {
        'top-level-has-prereqs-1': 'value',
        'top-level-has-prereqs-2': 'value',
        'prereq1': 'value',
        'prereq2': 'value',
        'prereq3': 'value',
        '$flagsState': {
            'top-level-has-prereqs-1': {'variation': 0, 'version': 100, 'prerequisites': ['prereq1', 'prereq2']},
            'top-level-has-prereqs-2': {'variation': 0, 'version': 100, 'prerequisites': ['prereq3']},
            'prereq1': {'variation': 0, 'version': 200},
            'prereq2': {'variation': 0, 'version': 200},
            'prereq3': {'variation': 0, 'version': 200},
        },
        '$valid': True,
    }


def test_all_flags_state_returns_state_with_reasons():
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'key1': flag1, 'key2': flag2}})
    client = make_client(store)
    state = client.all_flags_state(user, with_reasons=True)
    assert state.valid
    result = state.to_json_dict()
    assert result == {
        'key1': 'value1',
        'key2': 'value2',
        '$flagsState': {
            'key1': {'variation': 0, 'version': 100, 'reason': {'kind': 'OFF'}},
            'key2': {'variation': 1, 'version': 200, 'trackEvents': True, 'debugEventsUntilDate': 1000, 'reason': {'kind': 'OFF'}},
        },
        '$valid': True,
    }


def test_all_flags_state_can_be_filtered_for_client_side_flags():
    flag1 = {'key': 'server-side-1', 'on': False, 'offVariation': 0, 'variations': ['a'], 'clientSide': False, 'version': 100, 'trackEvents': False}
    flag2 = {'key': 'server-side-2', 'on': False, 'offVariation': 0, 'variations': ['b'], 'clientSide': False, 'version': 200, 'trackEvents': False}
    flag3 = {'key': 'client-side-1', 'on': False, 'offVariation': 0, 'variations': ['value1'], 'trackEvents': False, 'clientSide': True, 'version': 300, 'trackEvents': False}
    flag4 = {'key': 'client-side-2', 'on': False, 'offVariation': 0, 'variations': ['value2'], 'clientSide': True, 'version': 400, 'trackEvents': False}

    store = InMemoryFeatureStore()
    store.init({FEATURES: {flag1['key']: flag1, flag2['key']: flag2, flag3['key']: flag3, flag4['key']: flag4}})
    client = make_client(store)

    state = client.all_flags_state(user, client_side_only=True)
    assert state.valid
    values = state.to_values_map()
    assert values == {'client-side-1': 'value1', 'client-side-2': 'value2'}


def test_all_flags_state_can_omit_details_for_untracked_flags():
    future_time = (time.time() * 1000) + 100000
    flag1 = {'key': 'key1', 'version': 100, 'on': False, 'offVariation': 0, 'variations': ['value1'], 'trackEvents': False}
    flag2 = {'key': 'key2', 'version': 200, 'on': False, 'offVariation': 1, 'variations': ['x', 'value2'], 'trackEvents': True}
    flag3 = {'key': 'key3', 'version': 300, 'on': False, 'offVariation': 1, 'variations': ['x', 'value3'], 'trackEvents': False, 'debugEventsUntilDate': future_time}
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'key1': flag1, 'key2': flag2, 'key3': flag3}})
    client = make_client(store)
    state = client.all_flags_state(user, with_reasons=True, details_only_for_tracked_flags=True)
    assert state.valid is True
    result = state.to_json_dict()
    assert result == {
        'key1': 'value1',
        'key2': 'value2',
        'key3': 'value3',
        '$flagsState': {
            'key1': {'variation': 0},
            'key2': {'variation': 1, 'version': 200, 'trackEvents': True, 'reason': {'kind': 'OFF'}},
            'key3': {'variation': 1, 'version': 300, 'debugEventsUntilDate': future_time, 'reason': {'kind': 'OFF'}},
        },
        '$valid': True,
    }


def test_all_flags_state_returns_empty_state_if_user_has_no_key():
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'key1': flag1, 'key2': flag2}})
    client = make_client(store)
    state = client.all_flags_state(Context.from_dict({}))
    assert state.valid is False


def test_all_flags_returns_empty_state_if_feature_store_throws_error(caplog):
    store = ErroringFeatureStore()
    client = make_client(store)
    state = client.all_flags_state(Context.from_dict({"key": "user", "kind": "user"}))
    assert state.valid is False
    errlog = get_log_lines(caplog, 'ERROR')
    assert errlog == ['Unable to read flags for all_flag_state: NotImplementedError()']
