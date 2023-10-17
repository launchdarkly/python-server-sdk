import json
import os
import pytest
import tempfile
import threading
import time

from ldclient.client import LDClient, Context
from ldclient.config import Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.integrations import Files
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

have_yaml = False
try:
    import yaml
    have_yaml = True
except ImportError:
    pass


all_flag_keys = [ 'flag1', 'flag2' ]
all_properties_json = '''
  {
    "flags": {
      "flag1": {
        "key": "flag1",
        "on": true,
        "fallthrough": {
          "variation": 2
        },
        "variations": [ "fall", "off", "on" ]
      }
    },
    "flagValues": {
      "flag2": "value2"
    },
    "segments": {
      "seg1": {
        "key": "seg1",
        "include": ["user1"]
      }
    }
  }
'''

all_properties_yaml = '''
---
flags:
  flag1:
    key: flag1
    "on": true
flagValues:
  flag2: value2
segments:
  seg1:
    key: seg1
    include: ["user1"]
'''

flag_only_json = '''
  {
    "flags": {
      "flag1": {
        "key": "flag1",
        "on": true,
        "fallthrough": {
          "variation": 2
        },
        "variations": [ "fall", "off", "on" ]
      }
    }
  }
'''

segment_only_json = '''
  {
    "segments": {
      "seg1": {
        "key": "seg1",
        "include": ["user1"]
      }
    }
  }
'''

data_source = None
store = None
ready = None


def setup_function():
    global data_source, store, ready
    store = InMemoryFeatureStore()
    ready = threading.Event()

def teardown_function():
    if data_source is not None:
        data_source.stop()

def make_data_source(**kwargs):
    global data_source
    data_source = Files.new_data_source(**kwargs)(Config("SDK_KEY"), store, ready)
    return data_source

def make_temp_file(content):
    f, path = tempfile.mkstemp()
    os.write(f, content.encode("latin-1"))
    os.close(f)
    return path

def replace_file(path, content):
    with open(path, 'w') as f:
        f.write(content)

def test_does_not_load_data_prior_to_start():
    path = make_temp_file('{"flagValues":{"key":"value"}}')
    try:
        source = make_data_source(paths = path)
        assert ready.is_set() is False
        assert source.initialized() is False
        assert store.initialized is False
    finally:
        os.remove(path)

def test_loads_flags_on_start_from_json():
    path = make_temp_file(all_properties_json)
    try:
        source = make_data_source(paths = path)
        source.start()
        assert store.initialized is True
        assert sorted(list(store.all(FEATURES, lambda x: x).keys())) == all_flag_keys
    finally:
        os.remove(path)

def test_loads_flags_on_start_from_yaml():
    if not have_yaml:
        pytest.skip("skipping file source test with YAML because pyyaml isn't available")
    path = make_temp_file(all_properties_yaml)
    try:
        source = make_data_source(paths = path)
        source.start()
        assert store.initialized is True
        assert sorted(list(store.all(FEATURES, lambda x: x).keys())) == all_flag_keys
    finally:
        os.remove(path)

def test_sets_ready_event_and_initialized_on_successful_load():
    path = make_temp_file(all_properties_json)
    try:
        source = make_data_source(paths = path)
        source.start()
        assert source.initialized() is True
        assert ready.is_set() is True
    finally:
        os.remove(path)

def test_sets_ready_event_and_does_not_set_initialized_on_unsuccessful_load():
    bad_file_path = 'no-such-file'
    source = make_data_source(paths = bad_file_path)
    source.start()
    assert source.initialized() is False
    assert ready.is_set() is True

def test_can_load_multiple_files():
    path1 = make_temp_file(flag_only_json)
    path2 = make_temp_file(segment_only_json)
    try:
        source = make_data_source(paths = [ path1, path2 ])
        source.start()
        assert len(store.all(FEATURES, lambda x: x)) == 1
        assert len(store.all(SEGMENTS, lambda x: x)) == 1
    finally:
        os.remove(path1)
        os.remove(path2)

def test_does_not_allow_duplicate_keys():
    path1 = make_temp_file(flag_only_json)
    path2 = make_temp_file(flag_only_json)
    try:
        source = make_data_source(paths = [ path1, path2 ])
        source.start()
        assert len(store.all(FEATURES, lambda x: x)) == 0
    finally:
        os.remove(path1)
        os.remove(path2)

def test_does_not_reload_modified_file_if_auto_update_is_off():
    path = make_temp_file(flag_only_json)
    try:
        source = make_data_source(paths = path)
        source.start()
        assert len(store.all(SEGMENTS, lambda x: x)) == 0
        time.sleep(0.5)
        replace_file(path, segment_only_json)
        time.sleep(0.5)
        assert len(store.all(SEGMENTS, lambda x: x)) == 0
    finally:
        os.remove(path)

def do_auto_update_test(options):
    path = make_temp_file(flag_only_json)
    options['paths'] = path
    try:
        source = make_data_source(**options)
        source.start()
        assert len(store.all(SEGMENTS, lambda x: x)) == 0
        time.sleep(0.5)
        replace_file(path, segment_only_json)
        deadline = time.time() + 10
        while time.time() < deadline:
            time.sleep(0.1)
            if len(store.all(SEGMENTS, lambda x: x)) == 1:
                return
        assert False, "Flags were not reloaded after 10 seconds"
    finally:
        os.remove(path)

def test_reloads_modified_file_if_auto_update_is_on():
    do_auto_update_test({ 'auto_update': True })

def test_reloads_modified_file_in_polling_mode():
    do_auto_update_test({ 'auto_update': True, 'force_polling': True, 'poll_interval': 0.1 })

def test_evaluates_full_flag_with_client_as_expected():
    path = make_temp_file(all_properties_json)
    try:
        factory = Files.new_data_source(paths = path)
        client = LDClient(config=Config('SDK_KEY', update_processor_class = factory, send_events = False))
        value = client.variation('flag1', Context.from_dict({'key': 'user', 'kind': 'user'}), '')
        assert value == 'on'
    finally:
        os.remove(path)
        if client is not None:
            client.close()

def test_evaluates_simplified_flag_with_client_as_expected():
    path = make_temp_file(all_properties_json)
    try:
        factory = Files.new_data_source(paths = path)
        client = LDClient(config=Config('SDK_KEY', update_processor_class = factory, send_events = False))
        value = client.variation('flag2', Context.from_dict({'key': 'user', 'kind': 'user'}), '')
        assert value == 'value2'
    finally:
        os.remove(path)
        if client is not None:
            client.close()

unsafe_yaml_caused_method_to_be_called = False

def arbitrary_method_called_from_yaml(x):
    global unsafe_yaml_caused_method_to_be_called
    unsafe_yaml_caused_method_to_be_called = True

def test_does_not_allow_unsafe_yaml():
    if not have_yaml:
        pytest.skip("skipping file source test with YAML because pyyaml isn't available")

    # This extended syntax defined by pyyaml allows arbitrary code execution. We should be using
    # yaml.safe_load() which does not support such things.
    unsafe_yaml = '''
!!python/object/apply:testing.test_file_data_source.arbitrary_method_called_from_yaml ["hi"]
'''
    path = make_temp_file(unsafe_yaml)
    try:
        factory = Files.new_data_source(paths = path)
        client = LDClient(config=Config('SDK_KEY', update_processor_class = factory, send_events = False))
    finally:
        os.remove(path)
        if client is not None:
            client.close()
    assert unsafe_yaml_caused_method_to_be_called == False
