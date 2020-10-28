import pytest
import json
import time
from ldclient.client import LDClient, Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.flag import EvaluationDetail
from ldclient.interfaces import FeatureStore
from ldclient.versioned_data_kind import FEATURES
from testing.stub_util import MockEventProcessor, MockUpdateProcessor
from testing.test_ldclient import make_off_flag_with_value


user = { 'key': 'userkey' }
flag1 = {
    'key': 'key1',
    'version': 100,
    'on': False,
    'offVariation': 0,
    'variations': [ 'value1' ],
    'trackEvents': False
}
flag2 = {
    'key': 'key2',
    'version': 200,
    'on': False,
    'offVariation': 1,
    'variations': [ 'x', 'value2' ],
    'trackEvents': True,
    'debugEventsUntilDate': 1000
}

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

def make_client(store):
    return LDClient(config=Config(sdk_key='SDK_KEY',
                                  base_uri='http://test',
                                  event_processor_class=MockEventProcessor,
                                  update_processor_class=MockUpdateProcessor,
                                  feature_store=store))

def get_log_lines(caplog, level):
    loglines = caplog.records
    if callable(loglines):
        # records() is a function in older versions of the caplog plugin
        loglines = loglines()
    return [line.message for line in loglines if line.levelname == level]


def test_variation_for_existing_feature():
    feature = make_off_flag_with_value('feature.key', 'value')
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    assert 'value' == client.variation('feature.key', user, default='default')

def test_variation_for_unknown_feature():
    store = InMemoryFeatureStore()
    client = make_client(store)
    assert 'default' == client.variation('feature.key', user, default='default')

def test_variation_when_user_is_none():
    feature = make_off_flag_with_value('feature.key', 'value')
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    assert 'default' == client.variation('feature.key', None, default='default')

def test_variation_when_user_has_no_key():
    feature = make_off_flag_with_value('feature.key', 'value')
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    assert 'default' == client.variation('feature.key', { }, default='default')

def test_variation_for_flag_that_evaluates_to_none():
    empty_flag = {
        'key': 'feature.key',
        'on': False,
        'offVariation': None
    }
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': empty_flag}})
    client = make_client(store)
    assert 'default' == client.variation('feature.key', user, default='default')

def test_variation_detail_for_existing_feature():
    feature = make_off_flag_with_value('feature.key', 'value')
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

def test_variation_detail_when_user_is_none():
    feature = make_off_flag_with_value('feature.key', 'value')
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    expected = EvaluationDetail('default', None, {'kind': 'ERROR', 'errorKind': 'USER_NOT_SPECIFIED'})
    assert expected == client.variation_detail('feature.key', None, default='default')

def test_variation_detail_when_user_has_no_key():
    feature = make_off_flag_with_value('feature.key', 'value')
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': feature}})
    client = make_client(store)
    expected = EvaluationDetail('default', None, {'kind': 'ERROR', 'errorKind': 'USER_NOT_SPECIFIED'})
    assert expected == client.variation_detail('feature.key', { }, default='default')

def test_variation_detail_for_flag_that_evaluates_to_none():
    empty_flag = {
        'key': 'feature.key',
        'on': False,
        'offVariation': None
    }
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'feature.key': empty_flag}})
    client = make_client(store)
    expected = EvaluationDetail('default', None, {'kind': 'OFF'})
    actual = client.variation_detail('feature.key', user, default='default')
    assert expected == actual
    assert actual.is_default_value() == True

def test_variation_when_feature_store_throws_error(caplog):
    store = ErroringFeatureStore()
    client = make_client(store)
    assert client.variation('feature.key', { "key": "user" }, default='default') == 'default'
    errlog = get_log_lines(caplog, 'ERROR')
    assert errlog == [ 'Unexpected error while retrieving feature flag "feature.key": NotImplementedError()' ]

def test_variation_detail_when_feature_store_throws_error(caplog):
    store = ErroringFeatureStore()
    client = make_client(store)
    expected = EvaluationDetail('default', None, {'kind': 'ERROR', 'errorKind': 'EXCEPTION'})
    actual = client.variation_detail('feature.key', { "key": "user" }, default='default')
    assert expected == actual
    assert actual.is_default_value() == True
    errlog = get_log_lines(caplog, 'ERROR')
    assert errlog == [ 'Unexpected error while retrieving feature flag "feature.key": NotImplementedError()' ]

def test_all_flags_returns_values():
    store = InMemoryFeatureStore()
    store.init({ FEATURES: { 'key1': flag1, 'key2': flag2 } })
    client = make_client(store)
    result = client.all_flags_state(user).to_values_map()
    assert result == { 'key1': 'value1', 'key2': 'value2' }

def test_all_flags_returns_none_if_user_is_none():
    store = InMemoryFeatureStore()
    store.init({ FEATURES: { 'key1': flag1, 'key2': flag2 } })
    client = make_client(store)
    result = client.all_flags_state(None)
    assert not result.valid

def test_all_flags_returns_none_if_user_has_no_key():
    store = InMemoryFeatureStore()
    store.init({ FEATURES: { 'key1': flag1, 'key2': flag2 } })
    client = make_client(store)
    result = client.all_flags_state({ })
    assert not result.valid

def test_all_flags_returns_none_if_feature_store_throws_error(caplog):
    store = ErroringFeatureStore()
    client = make_client(store)
    assert not client.all_flags_state({ "key": "user" }).valid
    errlog = get_log_lines(caplog, 'ERROR')
    assert errlog == [ 'Unable to read flags for all_flag_state: NotImplementedError()' ]

def test_all_flags_state_returns_state():
    store = InMemoryFeatureStore()
    store.init({ FEATURES: { 'key1': flag1, 'key2': flag2 } })
    client = make_client(store)
    state = client.all_flags_state(user)
    assert state.valid
    result = state.to_json_dict()
    assert result == {
        'key1': 'value1',
        'key2': 'value2',
        '$flagsState': {
            'key1': {
                'variation': 0,
                'version': 100
            },
            'key2': {
                'variation': 1,
                'version': 200,
                'trackEvents': True,
                'debugEventsUntilDate': 1000
            }
        },
        '$valid': True
    }

def test_all_flags_state_returns_state_with_reasons():
    store = InMemoryFeatureStore()
    store.init({ FEATURES: { 'key1': flag1, 'key2': flag2 } })
    client = make_client(store)
    state = client.all_flags_state(user, with_reasons=True)
    assert state.valid
    result = state.to_json_dict()
    assert result == {
        'key1': 'value1',
        'key2': 'value2',
        '$flagsState': {
            'key1': {
                'variation': 0,
                'version': 100,
                'reason': {'kind': 'OFF'}
            },
            'key2': {
                'variation': 1,
                'version': 200,
                'trackEvents': True,
                'debugEventsUntilDate': 1000,
                'reason': {'kind': 'OFF'}
            }
        },
        '$valid': True
    }

def test_all_flags_state_can_be_filtered_for_client_side_flags():
    flag1 = {
        'key': 'server-side-1',
        'on': False,
        'offVariation': 0,
        'variations': [ 'a' ],
        'clientSide': False
    }
    flag2 = {
        'key': 'server-side-2',
        'on': False,
        'offVariation': 0,
        'variations': [ 'b' ],
        'clientSide': False
    }
    flag3 = {
        'key': 'client-side-1',
        'on': False,
        'offVariation': 0,
        'variations': [ 'value1' ],
        'clientSide': True
    }
    flag4 = {
        'key': 'client-side-2',
        'on': False,
        'offVariation': 0,
        'variations': [ 'value2' ],
        'clientSide': True
    }

    store = InMemoryFeatureStore()
    store.init({ FEATURES: { flag1['key']: flag1, flag2['key']: flag2, flag3['key']: flag3, flag4['key']: flag4 } })
    client = make_client(store)

    state = client.all_flags_state(user, client_side_only=True)
    assert state.valid
    values = state.to_values_map()
    assert values == { 'client-side-1': 'value1', 'client-side-2': 'value2' }

def test_all_flags_state_can_omit_details_for_untracked_flags():
    future_time = (time.time() * 1000) + 100000
    flag1 = {
        'key': 'key1',
        'version': 100,
        'on': False,
        'offVariation': 0,
        'variations': [ 'value1' ],
        'trackEvents': False
    }
    flag2 = {
        'key': 'key2',
        'version': 200,
        'on': False,
        'offVariation': 1,
        'variations': [ 'x', 'value2' ],
        'trackEvents': True
    }
    flag3 = {
        'key': 'key3',
        'version': 300,
        'on': False,
        'offVariation': 1,
        'variations': [ 'x', 'value3' ],
        'debugEventsUntilDate': future_time
    }
    store = InMemoryFeatureStore()
    store.init({ FEATURES: { 'key1': flag1, 'key2': flag2, 'key3': flag3 } })
    client = make_client(store)
    state = client.all_flags_state(user, with_reasons=True, details_only_for_tracked_flags=True)
    assert state.valid == True
    result = state.to_json_dict()
    assert result == {
        'key1': 'value1',
        'key2': 'value2',
        'key3': 'value3',
        '$flagsState': {
            'key1': {
                'variation': 0
            },
            'key2': {
                'variation': 1,
                'version': 200,
                'trackEvents': True,
                'reason': {'kind': 'OFF'}
            },
            'key3': {
                'variation': 1,
                'version': 300,
                'debugEventsUntilDate': future_time,
                'reason': {'kind': 'OFF'}
            }
        },
        '$valid': True
    }

def test_all_flags_state_returns_empty_state_if_user_is_none():
    store = InMemoryFeatureStore()
    store.init({ FEATURES: { 'key1': flag1, 'key2': flag2 } })
    client = make_client(store)
    state = client.all_flags_state(None)
    assert state.valid == False

def test_all_flags_state_returns_empty_state_if_user_has_no_key():
    store = InMemoryFeatureStore()
    store.init({ FEATURES: { 'key1': flag1, 'key2': flag2 } })
    client = make_client(store)
    state = client.all_flags_state({ })
    assert state.valid == False

def test_all_flags_returns_empty_state_if_feature_store_throws_error(caplog):
    store = ErroringFeatureStore()
    client = make_client(store)
    state = client.all_flags_state({ "key": "user" })
    assert state.valid == False
    errlog = get_log_lines(caplog, 'ERROR')
    assert errlog == [ 'Unable to read flags for all_flag_state: NotImplementedError()' ]
