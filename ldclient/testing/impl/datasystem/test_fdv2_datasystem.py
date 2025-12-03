# pylint: disable=missing-docstring

import os
import tempfile
from threading import Event
from typing import List

from mock import Mock

from ldclient.config import Config, DataSystemConfig
from ldclient.datasystem import file_ds_builder
from ldclient.impl.datasystem import DataAvailability
from ldclient.impl.datasystem.fdv2 import FDv2
from ldclient.integrations.test_datav2 import TestDataV2
from ldclient.interfaces import (
    DataSourceState,
    DataSourceStatus,
    FlagChange,
    Synchronizer,
    Update
)
from ldclient.versioned_data_kind import FEATURES


def test_two_phase_init():
    td_initializer = TestDataV2.data_source()
    td_initializer.update(td_initializer.flag("feature-flag").on(True))

    td_synchronizer = TestDataV2.data_source()
    # Set this to true, and then to false to ensure the version number exceeded
    # the initializer version number. Otherwise, they start as the same version
    # and the latest value is ignored.
    td_synchronizer.update(td_initializer.flag("feature-flag").on(True))
    td_synchronizer.update(td_synchronizer.flag("feature-flag").on(False))
    data_system_config = DataSystemConfig(
        initializers=[td_initializer.build_initializer],
        primary_synchronizer=td_synchronizer.build_synchronizer,
    )

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    initialized = Event()
    modified = Event()
    changes: List[FlagChange] = []
    count = 0

    def listener(flag_change: FlagChange):
        nonlocal count, changes
        count += 1
        changes.append(flag_change)

        if count == 2:
            initialized.set()
        if count == 3:
            modified.set()

    fdv2.flag_tracker.add_listener(listener)

    fdv2.start(set_on_ready)
    assert set_on_ready.wait(1), "Data system did not become ready in time"
    assert initialized.wait(1), "Flag change listener was not called in time"

    td_synchronizer.update(td_synchronizer.flag("feature-flag").on(True))
    assert modified.wait(1), "Flag change listener was not called in time"
    assert len(changes) == 3
    assert changes[0].key == "feature-flag"
    assert changes[1].key == "feature-flag"
    assert changes[2].key == "feature-flag"


def test_can_stop_fdv2():
    td = TestDataV2.data_source()
    data_system_config = DataSystemConfig(
        initializers=None,
        primary_synchronizer=td.build_synchronizer,
    )

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    changed = Event()
    changes: List[FlagChange] = []

    def listener(flag_change: FlagChange):
        changes.append(flag_change)
        changed.set()

    fdv2.flag_tracker.add_listener(listener)

    fdv2.start(set_on_ready)
    assert set_on_ready.wait(1), "Data system did not become ready in time"

    fdv2.stop()

    td.update(td.flag("feature-flag").on(False))
    assert changed.wait(1) is False, "Flag change listener was erroneously called"
    assert len(changes) == 0


def test_fdv2_data_availability_is_refreshed_with_data():
    td = TestDataV2.data_source()
    data_system_config = DataSystemConfig(
        initializers=None,
        primary_synchronizer=td.build_synchronizer,
    )

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    fdv2.start(set_on_ready)
    assert set_on_ready.wait(1), "Data system did not become ready in time"

    assert fdv2.data_availability.at_least(DataAvailability.REFRESHED)
    assert fdv2.target_availability.at_least(DataAvailability.REFRESHED)


def test_fdv2_fallsback_to_secondary_synchronizer():
    mock: Synchronizer = Mock()
    mock.sync.return_value = iter([])  # Empty iterator to simulate no data
    td = TestDataV2.data_source()
    td.update(td.flag("feature-flag").on(True))
    data_system_config = DataSystemConfig(
        initializers=[td.build_initializer],
        primary_synchronizer=lambda _: mock,  # Primary synchronizer is None to force fallback
        secondary_synchronizer=td.build_synchronizer,
    )

    changed = Event()
    changes: List[FlagChange] = []
    count = 0

    def listener(flag_change: FlagChange):
        nonlocal count, changes
        count += 1
        changes.append(flag_change)

        if count == 2:
            changed.set()

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
    fdv2.flag_tracker.add_listener(listener)
    fdv2.start(set_on_ready)
    assert set_on_ready.wait(1), "Data system did not become ready in time"

    td.update(td.flag("feature-flag").on(False))
    assert changed.wait(1), "Flag change listener was not called in time"
    assert len(changes) == 2
    assert changes[0].key == "feature-flag"
    assert changes[1].key == "feature-flag"


def test_fdv2_shutdown_down_if_both_synchronizers_fail():
    mock: Synchronizer = Mock()
    mock.sync.return_value = iter([])  # Empty iterator to simulate no data
    td = TestDataV2.data_source()
    td.update(td.flag("feature-flag").on(True))
    data_system_config = DataSystemConfig(
        initializers=[td.build_initializer],
        primary_synchronizer=lambda _: mock,  # Primary synchronizer is None to force fallback
        secondary_synchronizer=lambda _: mock,  # Secondary synchronizer also fails
    )

    changed = Event()

    def listener(status: DataSourceStatus):
        if status.state == DataSourceState.OFF:
            changed.set()

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
    fdv2.data_source_status_provider.add_listener(listener)
    fdv2.start(set_on_ready)
    assert set_on_ready.wait(1), "Data system did not become ready in time"

    assert changed.wait(1), "Data system did not shut down in time"
    assert fdv2.data_source_status_provider.status.state == DataSourceState.OFF


def test_fdv2_falls_back_to_fdv1_on_polling_error_with_header():
    """
    Test that FDv2 falls back to FDv1 when polling receives an error response
    with the X-LD-FD-Fallback: true header.
    """
    # Create a mock primary synchronizer that signals FDv1 fallback
    mock_primary: Synchronizer = Mock()
    mock_primary.name = "mock-primary"
    mock_primary.stop = Mock()

    # Simulate a synchronizer that yields an OFF state with revert_to_fdv1=True
    mock_primary.sync.return_value = iter([
        Update(
            state=DataSourceState.OFF,
            revert_to_fdv1=True
        )
    ])

    # Create FDv1 fallback data source with actual data
    td_fdv1 = TestDataV2.data_source()
    td_fdv1.update(td_fdv1.flag("fdv1-flag").on(True))

    data_system_config = DataSystemConfig(
        initializers=None,
        primary_synchronizer=lambda _: mock_primary,
        fdv1_fallback_synchronizer=td_fdv1.build_synchronizer,
    )

    changed = Event()
    changes: List[FlagChange] = []

    def listener(flag_change: FlagChange):
        changes.append(flag_change)
        changed.set()

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
    fdv2.flag_tracker.add_listener(listener)
    fdv2.start(set_on_ready)

    assert set_on_ready.wait(1), "Data system did not become ready in time"

    # Update flag in FDv1 data source to verify it's being used
    td_fdv1.update(td_fdv1.flag("fdv1-flag").on(False))
    assert changed.wait(1), "Flag change listener was not called in time"

    # Verify we got flag changes from FDv1
    assert len(changes) > 0
    assert any(c.key == "fdv1-flag" for c in changes)


def test_fdv2_falls_back_to_fdv1_on_polling_success_with_header():
    """
    Test that FDv2 falls back to FDv1 when polling receives a successful response
    with the X-LD-FD-Fallback: true header.
    """
    # Create a mock primary synchronizer that yields valid data but signals fallback
    mock_primary: Synchronizer = Mock()
    mock_primary.name = "mock-primary"
    mock_primary.stop = Mock()

    mock_primary.sync.return_value = iter([
        Update(
            state=DataSourceState.VALID,
            revert_to_fdv1=True
        )
    ])

    # Create FDv1 fallback data source
    td_fdv1 = TestDataV2.data_source()
    td_fdv1.update(td_fdv1.flag("fdv1-fallback-flag").on(True))

    data_system_config = DataSystemConfig(
        initializers=None,
        primary_synchronizer=lambda _: mock_primary,
        fdv1_fallback_synchronizer=td_fdv1.build_synchronizer,
    )

    changed = Event()
    changes: List[FlagChange] = []
    count = 0

    def listener(flag_change: FlagChange):
        nonlocal count
        count += 1
        changes.append(flag_change)
        if count >= 2:
            changed.set()

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
    fdv2.flag_tracker.add_listener(listener)
    fdv2.start(set_on_ready)

    assert set_on_ready.wait(1), "Data system did not become ready in time"

    # Trigger a flag update in FDv1
    td_fdv1.update(td_fdv1.flag("fdv1-fallback-flag").on(False))
    assert changed.wait(1), "Flag change listener was not called in time"

    # Verify FDv1 is active
    assert len(changes) > 0
    assert any(c.key == "fdv1-fallback-flag" for c in changes)


def test_fdv2_falls_back_to_fdv1_with_initializer():
    """
    Test that FDv2 falls back to FDv1 even when initialized with data,
    and that the FDv1 data replaces the initialized data.
    """
    # Initialize with some data
    td_initializer = TestDataV2.data_source()
    td_initializer.update(td_initializer.flag("initial-flag").on(True))

    # Create mock primary that signals fallback
    mock_primary: Synchronizer = Mock()
    mock_primary.name = "mock-primary"
    mock_primary.stop = Mock()

    mock_primary.sync.return_value = iter([
        Update(
            state=DataSourceState.OFF,
            revert_to_fdv1=True
        )
    ])

    # Create FDv1 fallback with different data
    td_fdv1 = TestDataV2.data_source()
    td_fdv1.update(td_fdv1.flag("fdv1-replacement-flag").on(True))

    data_system_config = DataSystemConfig(
        initializers=[td_initializer.build_initializer],
        primary_synchronizer=lambda _: mock_primary,
        fdv1_fallback_synchronizer=td_fdv1.build_synchronizer,
    )

    changed = Event()
    changes: List[FlagChange] = []

    def listener(flag_change: FlagChange):
        changes.append(flag_change)
        if len(changes) >= 2:
            changed.set()

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
    fdv2.flag_tracker.add_listener(listener)
    fdv2.start(set_on_ready)

    assert set_on_ready.wait(1), "Data system did not become ready in time"
    assert changed.wait(2), "Expected flag changes for both initial and fdv1 flags"

    # Verify we got changes for both flags
    flag_keys = [c.key for c in changes]
    assert "initial-flag" in flag_keys
    assert "fdv1-replacement-flag" in flag_keys


def test_fdv2_no_fallback_without_header():
    """
    Test that FDv2 does NOT fall back to FDv1 when an error occurs
    but the fallback header is not present.
    """
    # Create mock primary that fails but doesn't signal fallback
    mock_primary: Synchronizer = Mock()
    mock_primary.name = "mock-primary"
    mock_primary.stop = Mock()

    mock_primary.sync.return_value = iter([
        Update(
            state=DataSourceState.INTERRUPTED,
            revert_to_fdv1=False  # No fallback
        )
    ])

    # Create mock secondary
    mock_secondary: Synchronizer = Mock()
    mock_secondary.name = "mock-secondary"
    mock_secondary.stop = Mock()
    mock_secondary.sync.return_value = iter([
        Update(
            state=DataSourceState.VALID,
            revert_to_fdv1=False
        )
    ])

    # Create FDv1 fallback (should not be used)
    td_fdv1 = TestDataV2.data_source()
    td_fdv1.update(td_fdv1.flag("fdv1-should-not-appear").on(True))

    data_system_config = DataSystemConfig(
        initializers=None,
        primary_synchronizer=lambda _: mock_primary,
        secondary_synchronizer=lambda _: mock_secondary,
        fdv1_fallback_synchronizer=td_fdv1.build_synchronizer,
    )

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
    fdv2.start(set_on_ready)

    assert set_on_ready.wait(1), "Data system did not become ready in time"

    # Verify secondary was called (fallback to secondary, not FDv1)
    # Give it a moment to process
    import time
    time.sleep(0.2)

    # The primary should have been called, then secondary
    mock_primary.sync.assert_called()
    mock_secondary.sync.assert_called()


def test_fdv2_stays_on_fdv1_after_fallback():
    """
    Test that once FDv2 falls back to FDv1, it stays on FDv1 and doesn't
    attempt to recover to FDv2.
    """
    # Create mock primary that signals fallback
    mock_primary: Synchronizer = Mock()
    mock_primary.name = "mock-primary"
    mock_primary.stop = Mock()

    mock_primary.sync.return_value = iter([
        Update(
            state=DataSourceState.OFF,
            revert_to_fdv1=True
        )
    ])

    # Create FDv1 fallback
    td_fdv1 = TestDataV2.data_source()
    td_fdv1.update(td_fdv1.flag("fdv1-flag").on(True))

    data_system_config = DataSystemConfig(
        initializers=None,
        primary_synchronizer=lambda _: mock_primary,
        fdv1_fallback_synchronizer=td_fdv1.build_synchronizer,
    )

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
    fdv2.start(set_on_ready)

    assert set_on_ready.wait(1), "Data system did not become ready in time"

    # Give it time to settle
    import time
    time.sleep(0.5)

    # Primary should only be called once (not retried after fallback)
    assert mock_primary.sync.call_count == 1

    # Verify FDv1 is serving data
    store = fdv2.store
    flag = store.get(FEATURES, "fdv1-flag", lambda x: x)
    assert flag is not None


def test_fdv2_initializer_should_run_until_success():
    """
    Test that FDv2 initializers will run in order until a successful run. Then
    the datasystem is expected to transition to run synchronizers.
    """
    initial_flag_data = '''
{
  "flags": {
    "feature-flag": {
      "key": "feature-flag",
      "version": 0,
      "on": false,
      "fallthrough": {
        "variation": 0
      },
      "variations": ["off", "on"]
    }
  }
}
'''
    f, path = tempfile.mkstemp(suffix='.json')
    try:
        os.write(f, initial_flag_data.encode("utf-8"))
        os.close(f)

        td_initializer = TestDataV2.data_source()
        td_initializer.update(td_initializer.flag("feature-flag").on(True))

        # We actually do not care what this synchronizer does.
        td_synchronizer = TestDataV2.data_source()

        data_system_config = DataSystemConfig(
            initializers=[file_ds_builder([path]), td_initializer.build_initializer],
            primary_synchronizer=td_synchronizer.build_synchronizer,
        )

        set_on_ready = Event()
        synchronizer_ran = Event()
        fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
        count = 0

        def listener(_: FlagChange):
            nonlocal count
            count += 1
            if count == 3:
                synchronizer_ran.set()

        fdv2.flag_tracker.add_listener(listener)

        fdv2.start(set_on_ready)
        assert set_on_ready.wait(1), "Data system did not become ready in time"
        assert synchronizer_ran.wait(1), "Data system did not transition to synchronizer"
    finally:
        os.remove(path)


def test_fdv2_should_finish_initialization_on_first_successful_initializer():
    """
    Test that when a FDv2 initializer returns a basis and selector that the rest
    of the intializers will be skipped and the client starts synchronizing phase.
    """
    initial_flag_data = '''
{
  "flags": {
    "feature-flag": {
      "key": "feature-flag",
      "version": 0,
      "on": false,
      "fallthrough": {
        "variation": 0
      },
      "variations": ["off", "on"]
    }
  }
}
'''
    f, path = tempfile.mkstemp(suffix='.json')
    try:
        os.write(f, initial_flag_data.encode("utf-8"))
        os.close(f)

        td_initializer = TestDataV2.data_source()
        td_initializer.update(td_initializer.flag("feature-flag").on(True))

        # We actually do not care what this synchronizer does.
        td_synchronizer = TestDataV2.data_source()

        data_system_config = DataSystemConfig(
            initializers=[td_initializer.build_initializer, file_ds_builder([path])],
            primary_synchronizer=None,
        )

        set_on_ready = Event()
        fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
        count = 0

        def listener(_: FlagChange):
            nonlocal count
            count += 1

        fdv2.flag_tracker.add_listener(listener)

        fdv2.start(set_on_ready)
        assert set_on_ready.wait(1), "Data system did not become ready in time"
        assert count == 1, "Invalid initializer process"
        fdv2.stop()
    finally:
        os.remove(path)


def test_fdv2_availability_offline():
    """Test that FDv2 returns DEFAULTS for target availability and data availability when offline."""
    data_system_config = DataSystemConfig(
        initializers=None,
        primary_synchronizer=None,
    )

    fdv2 = FDv2(Config(sdk_key="dummy", offline=True), data_system_config)

    assert fdv2.data_availability == DataAvailability.DEFAULTS
    assert fdv2.target_availability == DataAvailability.DEFAULTS


def test_fdv2_availability_with_data_sources_no_store():
    """Test that FDv2 returns DEFAULTS for data and REFRESHED for target when configured with data sources but no store and uninitialized."""
    td = TestDataV2.data_source()

    data_system_config = DataSystemConfig(
        initializers=None,
        primary_synchronizer=td.build_synchronizer,
    )

    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    # Store is not initialized, and we have data sources configured
    assert not fdv2._store.is_initialized()
    assert fdv2.data_availability == DataAvailability.DEFAULTS
    assert fdv2.target_availability == DataAvailability.REFRESHED


def test_fdv2_availability_no_data_sources_with_readonly_store_uninitialized():
    """Test that FDv2 returns DEFAULTS for both when no data sources and read-only store is uninitialized."""
    from ldclient.interfaces import DataStoreMode
    from ldclient.testing.impl.datasystem.test_fdv2_persistence import (
        StubFeatureStore
    )

    store = StubFeatureStore()
    data_system_config = DataSystemConfig(
        initializers=None,
        primary_synchronizer=None,
        data_store=store,
        data_store_mode=DataStoreMode.READ_ONLY,
    )

    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    # Store is not initialized
    assert not store.initialized
    assert fdv2.data_availability == DataAvailability.DEFAULTS
    assert fdv2.target_availability == DataAvailability.CACHED


def test_fdv2_availability_no_data_sources_with_readonly_store_initialized():
    """Test that FDv2 returns CACHED for both when no data sources and read-only store is initialized."""
    from ldclient.interfaces import DataStoreMode
    from ldclient.testing.impl.datasystem.test_fdv2_persistence import (
        StubFeatureStore
    )

    store = StubFeatureStore()
    store.init({FEATURES: {}})

    data_system_config = DataSystemConfig(
        initializers=None,
        primary_synchronizer=None,
        data_store=store,
        data_store_mode=DataStoreMode.READ_ONLY,
    )

    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    # Store is initialized
    assert store.initialized
    assert fdv2.data_availability == DataAvailability.CACHED
    assert fdv2.target_availability == DataAvailability.CACHED


def test_fdv2_availability_no_data_sources_with_readwrite_store_initialized():
    """Test that FDv2 returns CACHED for both when no data sources and read-write store is initialized."""
    from ldclient.interfaces import DataStoreMode
    from ldclient.testing.impl.datasystem.test_fdv2_persistence import (
        StubFeatureStore
    )

    store = StubFeatureStore()
    store.init({FEATURES: {}})

    data_system_config = DataSystemConfig(
        initializers=None,
        primary_synchronizer=None,
        data_store=store,
        data_store_mode=DataStoreMode.READ_WRITE,
    )

    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    # Store is initialized
    assert store.initialized
    assert fdv2.data_availability == DataAvailability.CACHED
    assert fdv2.target_availability == DataAvailability.CACHED


def test_fdv2_availability_with_data_sources_and_store_uninitialized():
    """Test that FDv2 returns DEFAULTS for data and REFRESHED for target when data sources configured with uninitialized store."""
    from ldclient.interfaces import DataStoreMode
    from ldclient.testing.impl.datasystem.test_fdv2_persistence import (
        StubFeatureStore
    )

    td = TestDataV2.data_source()
    store = StubFeatureStore()

    data_system_config = DataSystemConfig(
        initializers=None,
        primary_synchronizer=td.build_synchronizer,
        data_store=store,
        data_store_mode=DataStoreMode.READ_WRITE,
    )

    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    # Store is not initialized
    assert not store.initialized
    assert fdv2.data_availability == DataAvailability.DEFAULTS
    assert fdv2.target_availability == DataAvailability.REFRESHED


def test_fdv2_availability_with_data_sources_and_store_initialized():
    """Test that FDv2 returns CACHED for data and REFRESHED for target when data sources configured with initialized store."""
    from ldclient.interfaces import DataStoreMode
    from ldclient.testing.impl.datasystem.test_fdv2_persistence import (
        StubFeatureStore
    )

    td = TestDataV2.data_source()
    store = StubFeatureStore()
    store.init({FEATURES: {}})

    data_system_config = DataSystemConfig(
        initializers=None,
        primary_synchronizer=td.build_synchronizer,
        data_store=store,
        data_store_mode=DataStoreMode.READ_WRITE,
    )

    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    # Store is initialized but selector not defined yet (synchronizer not started)
    assert store.initialized
    assert fdv2.data_availability == DataAvailability.CACHED
    assert fdv2.target_availability == DataAvailability.REFRESHED
