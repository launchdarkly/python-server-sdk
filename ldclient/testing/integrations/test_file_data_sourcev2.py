import json
import os
import tempfile
import threading
import time

import pytest

from ldclient.config import Config
from ldclient.impl.util import _Fail, _Success
from ldclient.integrations import Files
from ldclient.interfaces import (
    DataSourceState,
    IntentCode,
    ObjectKind,
    Selector
)
from ldclient.testing.mock_components import MockSelectorStore

# Skip all tests in this module in CI due to flakiness
pytestmark = pytest.mark.skipif(
    os.getenv('LD_SKIP_FLAKY_TESTS', '').lower() in ('true', '1', 'yes'),
    reason="Skipping flaky test"
)

have_yaml = False
try:
    import yaml
    have_yaml = True
except ImportError:
    pass


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

flag_values_only_json = '''
{
  "flagValues": {
    "flag2": "value2"
  }
}
'''


def make_temp_file(content):
    """Create a temporary file with the given content."""
    f, path = tempfile.mkstemp()
    os.write(f, content.encode("utf-8"))
    os.close(f)
    return path


def replace_file(path, content):
    """Replace the contents of a file."""
    with open(path, 'w') as f:
        f.write(content)


def test_creates_valid_initializer():
    """Test that FileDataSourceV2 creates a working initializer."""
    path = make_temp_file(all_properties_json)
    try:
        file_source = Files.new_data_source_v2(paths=[path])
        initializer = file_source(Config(sdk_key="dummy"))

        result = initializer.fetch(MockSelectorStore(Selector.no_selector()))
        assert isinstance(result, _Success)

        basis = result.value
        assert not basis.persist
        assert basis.environment_id is None
        assert basis.change_set.intent_code == IntentCode.TRANSFER_FULL

        # Should have 2 flags and 1 segment
        changes = basis.change_set.changes
        assert len(changes) == 3

        flag_changes = [c for c in changes if c.kind == ObjectKind.FLAG]
        segment_changes = [c for c in changes if c.kind == ObjectKind.SEGMENT]

        assert len(flag_changes) == 2
        assert len(segment_changes) == 1

        # Check selector is no_selector
        assert basis.change_set.selector == Selector.no_selector()
    finally:
        os.remove(path)


def test_initializer_handles_missing_file():
    """Test that initializer returns error for missing file."""
    file_source = Files.new_data_source_v2(paths=['no-such-file.json'])
    initializer = file_source(Config(sdk_key="dummy"))

    result = initializer.fetch(MockSelectorStore(Selector.no_selector()))
    assert isinstance(result, _Fail)
    assert "no-such-file.json" in result.error


def test_initializer_handles_invalid_json():
    """Test that initializer returns error for invalid JSON."""
    path = make_temp_file('{"flagValues":{')
    try:
        file_source = Files.new_data_source_v2(paths=[path])
        initializer = file_source(Config(sdk_key="dummy"))

        result = initializer.fetch(MockSelectorStore(Selector.no_selector()))
        assert isinstance(result, _Fail)
        assert "Unable to load flag data" in result.error
    finally:
        os.remove(path)


def test_initializer_handles_duplicate_keys():
    """Test that initializer returns error when same key appears in multiple files."""
    path1 = make_temp_file(flag_only_json)
    path2 = make_temp_file(flag_only_json)
    try:
        file_source = Files.new_data_source_v2(paths=[path1, path2])
        initializer = file_source(Config(sdk_key="dummy"))

        result = initializer.fetch(MockSelectorStore(Selector.no_selector()))
        assert isinstance(result, _Fail)
        assert "was used more than once" in result.error
    finally:
        os.remove(path1)
        os.remove(path2)


def test_initializer_loads_multiple_files():
    """Test that initializer can load from multiple files."""
    path1 = make_temp_file(flag_only_json)
    path2 = make_temp_file(segment_only_json)
    try:
        file_source = Files.new_data_source_v2(paths=[path1, path2])
        initializer = file_source(Config(sdk_key="dummy"))

        result = initializer.fetch(MockSelectorStore(Selector.no_selector()))
        assert isinstance(result, _Success)

        changes = result.value.change_set.changes
        flag_changes = [c for c in changes if c.kind == ObjectKind.FLAG]
        segment_changes = [c for c in changes if c.kind == ObjectKind.SEGMENT]

        assert len(flag_changes) == 1
        assert len(segment_changes) == 1
    finally:
        os.remove(path1)
        os.remove(path2)


def test_initializer_loads_yaml():
    """Test that initializer can parse YAML files."""
    if not have_yaml:
        pytest.skip("skipping YAML test because pyyaml isn't available")

    path = make_temp_file(all_properties_yaml)
    try:
        file_source = Files.new_data_source_v2(paths=[path])
        initializer = file_source(Config(sdk_key="dummy"))

        result = initializer.fetch(MockSelectorStore(Selector.no_selector()))
        assert isinstance(result, _Success)

        changes = result.value.change_set.changes
        assert len(changes) == 3  # 2 flags + 1 segment
    finally:
        os.remove(path)


def test_initializer_handles_flag_values():
    """Test that initializer properly converts flagValues to flags."""
    path = make_temp_file(flag_values_only_json)
    try:
        file_source = Files.new_data_source_v2(paths=[path])
        initializer = file_source(Config(sdk_key="dummy"))

        result = initializer.fetch(MockSelectorStore(Selector.no_selector()))
        assert isinstance(result, _Success)

        changes = result.value.change_set.changes
        flag_changes = [c for c in changes if c.kind == ObjectKind.FLAG]
        assert len(flag_changes) == 1

        # Check the flag was created with the expected structure
        flag_change = flag_changes[0]
        assert flag_change.key == "flag2"
        assert flag_change.object['key'] == "flag2"
        assert flag_change.object['on'] is True
        assert flag_change.object['variations'] == ["value2"]
    finally:
        os.remove(path)


def test_creates_valid_synchronizer():
    """Test that FileDataSourceV2 creates a working synchronizer."""
    path = make_temp_file(all_properties_json)
    try:
        file_source = Files.new_data_source_v2(paths=[path], force_polling=True, poll_interval=0.1)
        synchronizer = file_source(Config(sdk_key="dummy"))

        updates = []
        update_count = 0

        def collect_updates():
            nonlocal update_count
            for update in synchronizer.sync(MockSelectorStore(Selector.no_selector())):
                updates.append(update)
                update_count += 1

                if update_count == 1:
                    # Should get initial state
                    assert update.state == DataSourceState.VALID
                    assert update.change_set is not None
                    assert update.change_set.intent_code == IntentCode.TRANSFER_FULL
                    assert len(update.change_set.changes) == 3
                    synchronizer.stop()
                    break

        # Start the synchronizer in a thread with timeout to prevent hanging
        sync_thread = threading.Thread(target=collect_updates)
        sync_thread.start()

        # Wait for the thread to complete with timeout
        sync_thread.join(timeout=5)

        # Ensure thread completed successfully
        if sync_thread.is_alive():
            synchronizer.stop()
            sync_thread.join()
            pytest.fail("Synchronizer test timed out after 5 seconds")

        assert len(updates) == 1
    finally:
        synchronizer.stop()
        os.remove(path)


def test_synchronizer_detects_file_changes():
    """Test that synchronizer detects and reports file changes."""
    path = make_temp_file(flag_only_json)
    try:
        file_source = Files.new_data_source_v2(paths=[path], force_polling=True, poll_interval=0.1)
        synchronizer = file_source(Config(sdk_key="dummy"))

        updates = []
        update_event = threading.Event()

        def collect_updates():
            for update in synchronizer.sync(MockSelectorStore(Selector.no_selector())):
                updates.append(update)
                update_event.set()

                if len(updates) >= 2:
                    break

        # Start the synchronizer
        sync_thread = threading.Thread(target=collect_updates)
        sync_thread.start()

        # Wait for initial update
        assert update_event.wait(timeout=2), "Did not receive initial update"
        assert len(updates) == 1
        assert updates[0].state == DataSourceState.VALID
        initial_changes = [c for c in updates[0].change_set.changes if c.kind == ObjectKind.FLAG]
        assert len(initial_changes) == 1

        # Modify the file
        update_event.clear()
        time.sleep(0.2)  # Ensure filesystem timestamp changes
        replace_file(path, segment_only_json)

        # Wait for the change to be detected
        assert update_event.wait(timeout=2), "Did not receive update after file change"
        assert len(updates) == 2
        assert updates[1].state == DataSourceState.VALID
        segment_changes = [c for c in updates[1].change_set.changes if c.kind == ObjectKind.SEGMENT]
        assert len(segment_changes) == 1

        synchronizer.stop()
        sync_thread.join(timeout=2)
    finally:
        synchronizer.stop()
        os.remove(path)


def test_synchronizer_reports_error_on_invalid_file_update():
    """Test that synchronizer reports error when file becomes invalid."""
    path = make_temp_file(flag_only_json)
    try:
        file_source = Files.new_data_source_v2(paths=[path], force_polling=True, poll_interval=0.1)
        synchronizer = file_source(Config(sdk_key="dummy"))

        updates = []
        update_event = threading.Event()

        def collect_updates():
            for update in synchronizer.sync(MockSelectorStore(Selector.no_selector())):
                updates.append(update)
                update_event.set()

                if len(updates) >= 2:
                    break

        # Start the synchronizer
        sync_thread = threading.Thread(target=collect_updates)
        sync_thread.start()

        # Wait for initial update
        assert update_event.wait(timeout=2), "Did not receive initial update"
        assert len(updates) == 1
        assert updates[0].state == DataSourceState.VALID

        # Make the file invalid
        update_event.clear()
        time.sleep(0.2)  # Ensure filesystem timestamp changes
        replace_file(path, '{"invalid json')

        # Wait for the error to be detected
        assert update_event.wait(timeout=2), "Did not receive update after file became invalid"
        assert len(updates) == 2
        assert updates[1].state == DataSourceState.INTERRUPTED
        assert updates[1].error is not None

        synchronizer.stop()
        sync_thread.join(timeout=2)
    finally:
        synchronizer.stop()
        os.remove(path)


def test_synchronizer_can_be_stopped():
    """Test that synchronizer stops cleanly."""
    path = make_temp_file(all_properties_json)
    try:
        file_source = Files.new_data_source_v2(paths=[path])
        synchronizer = file_source(Config(sdk_key="dummy"))

        updates = []

        def collect_updates():
            for update in synchronizer.sync(MockSelectorStore(Selector.no_selector())):
                updates.append(update)

        # Start the synchronizer
        sync_thread = threading.Thread(target=collect_updates)
        sync_thread.start()

        # Give it a moment to process initial data
        time.sleep(0.2)

        # Stop it
        synchronizer.stop()

        # Thread should complete
        sync_thread.join(timeout=2)
        assert not sync_thread.is_alive()

        # Should have received at least the initial update
        assert len(updates) >= 1
        assert updates[0].state == DataSourceState.VALID
    finally:
        os.remove(path)


def test_fetch_after_stop_returns_error():
    """Test that fetch returns error after synchronizer is stopped."""
    path = make_temp_file(all_properties_json)
    try:
        file_source = Files.new_data_source_v2(paths=[path])
        initializer = file_source(Config(sdk_key="dummy"))

        # First fetch should work
        result = initializer.fetch(MockSelectorStore(Selector.no_selector()))
        assert isinstance(result, _Success)

        # Stop the source
        initializer.stop()

        # Second fetch should fail
        result = initializer.fetch(MockSelectorStore(Selector.no_selector()))
        assert isinstance(result, _Fail)
        assert "closed" in result.error
    finally:
        os.remove(path)


def test_source_name_property():
    """Test that the data source has the correct name."""
    path = make_temp_file(all_properties_json)
    try:
        file_source = Files.new_data_source_v2(paths=[path])
        source = file_source(Config(sdk_key="dummy"))

        assert source.name == "FileDataV2"
    finally:
        source.stop()
        os.remove(path)


def test_accepts_single_path_string():
    """Test that paths parameter can be a single string."""
    path = make_temp_file(flag_only_json)
    try:
        # Pass a single string instead of a list
        file_source = Files.new_data_source_v2(paths=path)
        initializer = file_source(Config(sdk_key="dummy"))

        result = initializer.fetch(MockSelectorStore(Selector.no_selector()))
        assert isinstance(result, _Success)
        assert len(result.value.change_set.changes) == 1
    finally:
        os.remove(path)
