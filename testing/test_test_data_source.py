import pytest
import warnings

from ldclient.client import LDClient
from ldclient.config import Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

#from ldclient.integrations import TestData
from ldclient.impl.integrations.test_data.test_data_source import TestData


# Filter warning arising from Pytest treating classes starting
# with the word 'Test' as part of the test suite
warnings.filterwarnings("ignore", message="cannot collect test class 'TestData'")

def setup_function():
    print("Setup")

def teardown_function():
    print("Teardown")

def test_makes_flag():
    td = TestData.data_source()
    flag = td.flag(key='test-flag')
    assert flag is not None

    builtFlag = flag.build(0)
    assert builtFlag['key'] is 'test-flag'
    assert builtFlag['on'] is True
    assert builtFlag['variations'] == []

def test_initializes_flag_with_client():
    td = TestData.data_source()
    client = LDClient(config=Config('SDK_KEY', update_processor_class = td, send_events = False, offline = True))

    client.close()

def test_flagbuilder_can_turn_flag_off():
    td = TestData.data_source()
    flag = td.flag('test-flag')
    flag.on(False)

    assert flag.build(0)['on'] is False

def test_flagbuilder_can_set_fallthrough_variation():
    td = TestData.data_source()
    flag = td.flag('test-flag')
    flag.fallthrough_variation(2)

    assert flag.build(0)['fallthrough_variation'] == 2

def test_flagbuilder_can_set_off_variation():
    td = TestData.data_source()
    flag = td.flag('test-flag')
    flag.off_variation(2)

    assert flag.build(0)['off_variation'] == 2

def test_flagbuilder_can_make_boolean_flag():
    td = TestData.data_source()
    flag = td.flag('boolean-flag').boolean_flag()

    assert flag.is_boolean_flag() == True

    builtFlag = flag.build(0)
    assert builtFlag['fallthrough_variation'] == 0
    assert builtFlag['off_variation'] == 1

def test_flagbuilder_can_set_variation_for_all_users():
    td = TestData.data_source()
    flag = td.flag('test-flag')
    flag.variation_for_all_users(True)
    assert flag.build(0)['fallthrough_variation'] == 0

def test_flagbuilder_can_set_variations():
    td = TestData.data_source()
    flag = td.flag('test-flag')
    flag.variations(2,3,4,5)
    assert flag.build(0)['variations'] == [2,3,4,5]

def test_flagbuilder_can_safely_copy():
    td = TestData.data_source()
    flag = td.flag('test-flag')
    flag.variations(1,2)
    copy_of_flag = flag.copy()
    flag.variations(3,4)
    assert copy_of_flag.build(0)['variations'] == [1,2]

    copy_of_flag.variations(5,6)
    assert flag.build(0)['variations'] == [3,4]

def test_flagbuilder_can_set_boolean_variation_for_user():
    td = TestData.data_source()
    flag = td.flag('user-variation-flag')
    flag.variation_for_user('christian', False)
    expected_targets = [
        {
            'variation': 1,
            'values': ['christian']
        }
    ]
    assert flag.build(0)['targets'] == expected_targets

def test_flagbuilder_can_set_numerical_variation_for_user():
    td = TestData.data_source()
    flag = td.flag('user-variation-flag')
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
    td = TestData.data_source()
    flag = td.flag('some-flag')
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
