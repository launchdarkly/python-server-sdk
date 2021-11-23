import json
import os
import pytest
import threading
import time

from ldclient.client import LDClient
from ldclient.config import Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

#from ldclient.integrations import TestData
from ldclient.impl.integrations.test_data.test_data_source import _FlagBuilder
from ldclient.impl.integrations.test_data.test_data_source import _FlagRuleBuilder



data_source = None
store = None
ready = None


def setup_function():
    print("Setup")

def teardown_function():
    print("Teardown")

def test_makes_flag_builder():
    flagBuilder = _FlagBuilder('test-flag')
    assert flagBuilder is not None
    assert flagBuilder._key is 'test-flag'
    assert flagBuilder._on is True
    assert flagBuilder._variations == []

def test_flagbuilder_can_turn_flag_off():
    flagBuilder = _FlagBuilder('test-flag')
    flagBuilder.on(False)
    assert flagBuilder._on is False

def test_flagbuilder_can_set_fallthrough_variation():
    flagBuilder = _FlagBuilder('test-flag')
    flagBuilder.fallthrough_variation(2)
    assert flagBuilder._fallthrough_variation == 2

def test_flagbuilder_can_set_off_variation():
    flagBuilder = _FlagBuilder('test-flag')
    flagBuilder.off_variation(2)
    assert flagBuilder._off_variation == 2

def test_flagbuilder_can_make_boolean_flag():
    flagBuilder = _FlagBuilder('boolean-flag').boolean_flag()
    assert flagBuilder._is_boolean_flag() == True
    assert flagBuilder._fallthrough_variation == 0
    assert flagBuilder._off_variation == 1

def test_flagbuilder_can_set_variation_for_all_users():
    flagBuilder = _FlagBuilder('test-flag')
    flagBuilder.variation_for_all_users(True)
    assert flagBuilder._fallthrough_variation == 0

def test_flagbuilder_can_set_variations():
    flagBuilder = _FlagBuilder('test-flag')
    flagBuilder.variations(2,3,4,5)
    assert flagBuilder._variations == [2,3,4,5]

def test_flagbuilder_can_copy():
    flagBuilder = _FlagBuilder('test-flag')
    flagBuilder.variations(1,2)
    flagBuilderCopy = flagBuilder.copy()
    flagBuilder.variations(3,4)
    assert flagBuilderCopy._variations == [1,2]

def test_flagbuilder_can_set_boolean_variation_for_user():
    flagBuilder = _FlagBuilder('user-variation-flag')
    flagBuilder.variation_for_user('christian', False)
    assert flagBuilder._targets == {1: ['christian']}

def test_flagbuilder_can_set_numerical_variation_for_user():
    flagBuilder = _FlagBuilder('user-variation-flag')
    flagBuilder.variations('a','b','c')
    flagBuilder.variation_for_user('christian', 2)
    expected_targets = [
        {
            'variation': 2,
            'values': ['christian']
        }
    ]
    assert flagBuilder.build(1)['targets'] == expected_targets

def test_flagbuilder_can_build():
    flagBuilder = _FlagBuilder('some-flag')
    flagRuleBuilder = _FlagRuleBuilder(flagBuilder)
    flagRuleBuilder.and_match('country', 'fr').then_return(True)
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

    assert flagBuilder.build(1) == expected_result
