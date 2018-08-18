import pytest
import json
from ldclient.client import LDClient, Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.versioned_data_kind import FEATURES
from testing.stub_util import MockEventProcessor, MockUpdateProcessor


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

def make_client(store):
    return LDClient(config=Config(sdk_key='SDK_KEY',
                                  base_uri='http://test',
                                  event_processor_class=MockEventProcessor,
                                  update_processor_class=MockUpdateProcessor,
                                  feature_store=store))

def test_all_flags_returns_values():
    store = InMemoryFeatureStore()
    store.init({ FEATURES: { 'key1': flag1, 'key2': flag2 } })
    client = make_client(store)
    result = client.all_flags(user)
    assert result == { 'key1': 'value1', 'key2': 'value2' }

def test_all_flags_returns_none_if_user_is_none():
    store = InMemoryFeatureStore()
    store.init({ FEATURES: { 'key1': flag1, 'key2': flag2 } })
    client = make_client(store)
    result = client.all_flags(None)
    assert result is None

def test_all_flags_returns_none_if_user_has_no_key():
    store = InMemoryFeatureStore()
    store.init({ FEATURES: { 'key1': flag1, 'key2': flag2 } })
    client = make_client(store)
    result = client.all_flags({ })
    assert result is None

def test_all_flags_state_returns_state():
    store = InMemoryFeatureStore()
    store.init({ FEATURES: { 'key1': flag1, 'key2': flag2 } })
    client = make_client(store)
    state = client.all_flags_state(user)
    assert state.valid == True
    result = json.loads(state.to_json_string())
    assert result == {
        'key1': 'value1',
        'key2': 'value2',
        '$flagsState': {
            'key1': {
                'variation': 0,
                'version': 100,
                'trackEvents': False
            },
            'key2': {
                'variation': 1,
                'version': 200,
                'trackEvents': True,
                'debugEventsUntilDate': 1000
            }
        }
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
