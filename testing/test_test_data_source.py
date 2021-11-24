import pytest

from ldclient.client import LDClient
from ldclient.config import Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

#from ldclient.integrations import TestData
from ldclient.impl.integrations.test_data.test_data_source import TestData



def setup_function():
    print("Setup")

def teardown_function():
    print("Teardown")

def test_makes_flag():
    flag = TestData.flag('test-flag')
    assert flag is not None

    builtFlag = flag.build(0)
    assert builtFlag['key'] is 'test-flag'
    assert builtFlag['on'] is True
    assert builtFlag['variations'] == []

def test_flagbuilder_can_turn_flag_off():
    flag = TestData.flag('test-flag')
    flag.on(False)

    assert flag.build(0)['on'] is False

def test_flagbuilder_can_set_fallthrough_variation():
    flag = TestData.flag('test-flag')
    flag.fallthrough_variation(2)

    assert flag.build(0)['fallthrough_variation'] == 2

def test_flagbuilder_can_set_off_variation():
    flag = TestData.flag('test-flag')
    flag.off_variation(2)

    assert flag.build(0)['off_variation'] == 2

def test_flagbuilder_can_make_boolean_flag():
    flag = TestData.flag('boolean-flag').boolean_flag()

    assert flag.is_boolean_flag() == True

    builtFlag = flag.build(0)
    assert builtFlag['fallthrough_variation'] == 0
    assert builtFlag['off_variation'] == 1

def test_flagbuilder_can_set_variation_for_all_users():
    flag = TestData.flag('test-flag')
    flag.variation_for_all_users(True)
    assert flag.build(0)['fallthrough_variation'] == 0

def test_flagbuilder_can_set_variations():
    flag = TestData.flag('test-flag')
    flag.variations(2,3,4,5)
    assert flag.build(0)['variations'] == [2,3,4,5]

def test_flagbuilder_can_safely_copy():
    flag = TestData.flag('test-flag')
    flag.variations(1,2)
    copy_of_flag = flag.copy()
    flag.variations(3,4)
    assert copy_of_flag.build(0)['variations'] == [1,2]

    copy_of_flag.variations(5,6)
    assert flag.build(0)['variations'] == [3,4]

def test_flagbuilder_can_set_boolean_variation_for_user():
    flag = TestData.flag('user-variation-flag')
    flag.variation_for_user('christian', False)
    expected_targets = [
        {
            'variation': 1,
            'values': ['christian']
        }
    ]
    assert flag.build(0)['targets'] == expected_targets

def test_flagbuilder_can_set_numerical_variation_for_user():
    flag = TestData.flag('user-variation-flag')
    flag.variations('a','b','c')
    flag.variation_for_user('christian', 2)
    expected_targets = [
        {
            'variation': 2,
            'values': ['christian']
        }
    ]
    assert flag.build(1)['targets'] == expected_targets

def test_flagbuilder_can_build():
    flag = TestData.flag('some-flag')
    flag.if_match('country', 'fr').then_return(True)
    expected_result = {
        'fallthrough_variation': 0,
        'key': 'some-flag',
        'off_variation': 1,
        'on': True,
        'variations': [True, False],
        'rules': [
            {
                'clauses': [
                    {'attribute': 'country',
                    'negate': False,
                    'operator': 'in',
                    'values': ['fr']
                    }
                ],
                'id': 'rule0',
                'variation': 0
            }
        ],
        'version': 1,
    }

    assert flag.build(1) == expected_result
