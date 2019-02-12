import pytest
import json
import jsonpickle
from ldclient.flags_state import FeatureFlagsState

def test_can_get_flag_value():
    state = FeatureFlagsState(True)
    flag = { 'key': 'key' }
    state.add_flag(flag, 'value', 1, None, False)
    assert state.get_flag_value('key') == 'value'

def test_returns_none_for_unknown_flag():
    state = FeatureFlagsState(True)
    assert state.get_flag_value('key') is None

def test_can_convert_to_values_map():
    state = FeatureFlagsState(True)
    flag1 = { 'key': 'key1' }
    flag2 = { 'key': 'key2' }
    state.add_flag(flag1, 'value1', 0, None, False)
    state.add_flag(flag2, 'value2', 1, None, False)
    assert state.to_values_map() == { 'key1': 'value1', 'key2': 'value2' }

def test_can_convert_to_json_dict():
    state = FeatureFlagsState(True)
    flag1 = { 'key': 'key1', 'version': 100, 'offVariation': 0, 'variations': [ 'value1' ], 'trackEvents': False }
    flag2 = { 'key': 'key2', 'version': 200, 'offVariation': 1, 'variations': [ 'x', 'value2' ], 'trackEvents': True, 'debugEventsUntilDate': 1000 }
    state.add_flag(flag1, 'value1', 0, None, False)
    state.add_flag(flag2, 'value2', 1, None, False)

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

def test_can_convert_to_json_string():
    state = FeatureFlagsState(True)
    flag1 = { 'key': 'key1', 'version': 100, 'offVariation': 0, 'variations': [ 'value1' ], 'trackEvents': False }
    flag2 = { 'key': 'key2', 'version': 200, 'offVariation': 1, 'variations': [ 'x', 'value2' ], 'trackEvents': True, 'debugEventsUntilDate': 1000 }
    state.add_flag(flag1, 'value1', 0, None, False)
    state.add_flag(flag2, 'value2', 1, None, False)

    obj = state.to_json_dict()
    str = state.to_json_string()
    assert json.loads(str) == obj

# We don't actually use jsonpickle in the SDK, but FeatureFlagsState has a magic method that makes it
# behave correctly in case the application uses jsonpickle to serialize it.
def test_can_serialize_with_jsonpickle():
    state = FeatureFlagsState(True)
    flag1 = { 'key': 'key1', 'version': 100, 'offVariation': 0, 'variations': [ 'value1' ], 'trackEvents': False }
    flag2 = { 'key': 'key2', 'version': 200, 'offVariation': 1, 'variations': [ 'x', 'value2' ], 'trackEvents': True, 'debugEventsUntilDate': 1000 }
    state.add_flag(flag1, 'value1', 0, None, False)
    state.add_flag(flag2, 'value2', 1, None, False)

    obj = state.to_json_dict()
    str = jsonpickle.encode(state, unpicklable=False)
    assert json.loads(str) == obj
