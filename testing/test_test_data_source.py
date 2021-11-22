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

def test_can_turn_flag_off():
    flagBuilder = _FlagBuilder('test-flag')
    flagBuilder.on(False)
    assert flagBuilder._on is False

def test_can_set_fallthrough_variation():
    flagBuilder = _FlagBuilder('test-flag')
    flagBuilder.fallthrough_variation(2)
    assert flagBuilder._fallthrough_variation == 2

def test_can_set_off_variation():
    flagBuilder = _FlagBuilder('test-flag')
    flagBuilder.off_variation(2)
    assert flagBuilder._off_variation == 2

def test_can_make_boolean_flag():
    flagBuilder = _FlagBuilder('boolean-flag').boolean_flag()
    assert flagBuilder._is_boolean_flag() == True
    assert flagBuilder._fallthrough_variation == 0
    assert flagBuilder._off_variation == 1

def test_can_set_variation_for_all_users():
    flagBuilder = _FlagBuilder('test-flag')
    flagBuilder.variation_for_all_users(True)
    assert flagBuilder._fallthrough_variation == 0
