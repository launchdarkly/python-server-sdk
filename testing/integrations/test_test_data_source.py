import pytest
import warnings

from ldclient.client import LDClient
from ldclient.config import Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

from ldclient.integrations.test_data import TestData


## Test Data + Data Source

def test_makes_valid_datasource():
    td = TestData.data_source()
    store = InMemoryFeatureStore()

    client = LDClient(config=Config('SDK_KEY', update_processor_class = td, send_events = False, offline = True, feature_store = store))

    assert store.all(FEATURES, lambda x: x) == {}


def test_makes_valid_datasource_with_flag():
    td = TestData.data_source()
    flag = td.flag(key='test-flag')
    assert flag is not None

    builtFlag = flag._build(0)
    assert builtFlag['key'] is 'test-flag'
    assert builtFlag['on'] is True
    assert builtFlag['variations'] == [True, False]


def test_can_retrieve_flag_from_store():
    td = TestData.data_source()
    td.update(td.flag('some-flag'))

    store = InMemoryFeatureStore()

    client = LDClient(config=Config('SDK_KEY', update_processor_class = td, send_events = False, offline = True, feature_store = store))

    assert store.get(FEATURES, 'some-flag') == td.flag('some-flag')._build(1)

    client.close()

def test_updates_to_flags_are_reflected_in_store():
    td = TestData.data_source()

    store = InMemoryFeatureStore()

    client = LDClient(config=Config('SDK_KEY', update_processor_class = td, send_events = False, offline = True, feature_store = store))

    td.update(td.flag('some-flag'))

    assert store.get(FEATURES, 'some-flag') == td.flag('some-flag')._build(1)

    client.close()

def test_updates_after_client_close_have_no_affect():
    td = TestData.data_source()

    store = InMemoryFeatureStore()

    client = LDClient(config=Config('SDK_KEY', update_processor_class = td, send_events = False, offline = True, feature_store = store))

    client.close()

    td.update(td.flag('some-flag'))

    assert store.get(FEATURES, 'some-flag') == None

def test_can_handle_multiple_clients():
    td = TestData.data_source()
    td.update(td.flag('flag'))

    store = InMemoryFeatureStore()
    store2 = InMemoryFeatureStore()

    config = Config('SDK_KEY', update_processor_class = td, send_events = False, offline = True, feature_store = store)
    client = LDClient(config=config)

    config2 = Config('SDK_KEY', update_processor_class = td, send_events = False, offline = True, feature_store = store2)
    client2 = LDClient(config=config2)

    assert store.get(FEATURES, 'flag') == {
            'fallthrough': {
                'variation': 0,
            },
            'key': 'flag',
            'offVariation': 1,
            'on': True,
            'rules': [],
            'targets': [],
            'variations': [True, False],
            'version': 1
            }

    assert store2.get(FEATURES, 'flag') == {
            'fallthrough': {
                'variation': 0,
            },
            'key': 'flag',
            'offVariation': 1,
            'on': True,
            'rules': [],
            'targets': [],
            'variations': [True, False],
            'version': 1
            }

    td.update(td.flag('flag').variation_for_all_users(False))

    assert store.get(FEATURES, 'flag') == {
            'fallthrough': {
                'variation': 1,
            },
            'key': 'flag',
            'offVariation': 1,
            'on': True,
            'rules': [],
            'targets': [],
            'variations': [True, False],
            'version': 2
            }

    assert store2.get(FEATURES, 'flag') == {
            'fallthrough': {
                'variation': 1,
            },
            'key': 'flag',
            'offVariation': 1,
            'on': True,
            'rules': [],
            'targets': [],
            'variations': [True, False],
            'version': 2
            }

    client.close()
    client2.close()


## FlagBuilder

def test_flagbuilder_defaults_to_boolean_flag():
    td = TestData.data_source()
    flag = td.flag('empty-flag')
    assert flag._build(0)['variations'] == [True, False]
    assert flag._build(0)['fallthrough'] == {'variation': 0}
    assert flag._build(0)['offVariation'] == 1

def test_flagbuilder_can_turn_flag_off():
    td = TestData.data_source()
    flag = td.flag('test-flag')
    flag.on(False)

    assert flag._build(0)['on'] is False

def test_flagbuilder_can_set_fallthrough_variation():
    td = TestData.data_source()
    flag = td.flag('test-flag')
    flag.fallthrough_variation(2)

    assert flag._build(0)['fallthrough'] == {'variation': 2}

    flag.fallthrough_variation(True)

    assert flag._build(0)['fallthrough'] == {'variation': 0}

def test_flagbuilder_can_set_off_variation():
    td = TestData.data_source()
    flag = td.flag('test-flag')
    flag.off_variation(2)

    assert flag._build(0)['offVariation'] == 2

    flag.off_variation(True)

    assert flag._build(0)['offVariation'] == 0

def test_flagbuilder_can_make_boolean_flag():
    td = TestData.data_source()
    flag = td.flag('boolean-flag').boolean_flag()

    builtFlag = flag._build(0)
    assert builtFlag['fallthrough'] == {'variation': 0}
    assert builtFlag['offVariation'] == 1

def test_flagbuilder_can_set_variation_when_targeting_is_off():
    td = TestData.data_source()
    flag = td.flag('test-flag') \
        .on(False)
    assert flag._build(0)['on'] == False
    assert flag._build(0)['variations'] == [True,False]
    flag.variations('dog', 'cat')
    assert flag._build(0)['variations'] == ['dog','cat']

def test_flagbuilder_can_set_variation_for_all_users():
    td = TestData.data_source()
    flag = td.flag('test-flag')
    flag.variation_for_all_users(True)
    assert flag._build(0)['fallthrough'] == {'variation': 0}

def test_flagbuilder_clears_existing_rules_and_targets_when_setting_variation_for_all_users():
    td = TestData.data_source()

    flag = td.flag('test-flag').if_match('name', 'christian').then_return(False).variation_for_user('christian', False).variation_for_all_users(True)._build(0)

    assert flag['rules'] == []
    assert flag['targets'] == []

def test_flagbuilder_can_set_variations():
    td = TestData.data_source()
    flag = td.flag('test-flag')
    flag.variations(2,3,4,5)
    assert flag._build(0)['variations'] == [2,3,4,5]

def test_flagbuilder_can_make_an_immutable_copy():
    td = TestData.data_source()
    flag = td.flag('test-flag')
    flag.variations(1,2)
    copy_of_flag = flag._copy()
    flag.variations(3,4)
    assert copy_of_flag._build(0)['variations'] == [1,2]

    copy_of_flag.variations(5,6)
    assert flag._build(0)['variations'] == [3,4]

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
    assert flag._build(0)['targets'] == expected_targets

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
    assert flag._build(1)['targets'] == expected_targets

def test_flagbuilder_can_set_value_for_all_users():
    td = TestData.data_source()
    flag = td.flag('user-value-flag')
    flag.variation_for_user('john', 1)

    built_flag = flag._build(0)
    assert built_flag['targets'] == [{'values': ['john'], 'variation': 1}]
    assert built_flag['variations'] == [True, False]

    flag.value_for_all_users('yes')

    built_flag2 = flag._build(0)
    assert built_flag2['targets'] == []
    assert built_flag2['variations'] == ['yes']


def test_flagbuilder_can_build():
    td = TestData.data_source()
    flag = td.flag('some-flag')
    flag.if_match('country', 'fr').then_return(True)
    expected_result = {
        'fallthrough': {
            'variation': 0,
        },
        'key': 'some-flag',
        'offVariation': 1,
        'on': True,
        'targets': [],
        'variations': [True, False],
        'rules': [
            {
                'clauses': [
                    {'attribute': 'country',
                    'negate': False,
                    'op': 'in',
                    'values': ['fr']
                    }
                ],
                'id': 'rule0',
                'variation': 0
            }
        ],
        'version': 1,
    }

    assert flag._build(1) == expected_result

def test_flag_can_evaluate_rules():
    td = TestData.data_source()
    store = InMemoryFeatureStore()

    client = LDClient(config=Config('SDK_KEY',
                      update_processor_class = td,
                      send_events = False,
                      feature_store = store))

    td.update(td.flag(key='test-flag')
                .fallthrough_variation(False)
                .if_match('firstName', 'Mike')
                .and_not_match('country', 'gb')
                .then_return(True))

    # user1 should satisfy the rule (matching firstname, not matching country)
    user1 = { 'key': 'user1', 'firstName': 'Mike', 'country': 'us' }
    eval1 = client.variation_detail('test-flag', user1, default='default')

    assert eval1.value == True
    assert eval1.variation_index == 0
    assert eval1.reason['kind'] == 'RULE_MATCH'

    # user2 should NOT satisfy the rule (not matching firstname despite not matching country)
    user2 = { 'key': 'user2', 'firstName': 'Joe', 'country': 'us' }
    eval2 = client.variation_detail('test-flag', user2, default='default')

    assert eval2.value == False
    assert eval2.variation_index == 1
    assert eval2.reason['kind'] == 'FALLTHROUGH'

