# pylint: disable=missing-docstring

import os
import tempfile
from threading import Event
from typing import List

from mock import Mock

from ldclient.config import Config, DataSourceBuilder, DataSystemConfig
from ldclient.datasystem import file_ds_builder
from ldclient.impl.datasystem import DataAvailability
from ldclient.impl.datasystem.fdv2 import FDv2
from ldclient.impl.util import _LD_FD_FALLBACK_HEADER, _Fail, _Success
from ldclient.integrations.test_datav2 import TestDataV2
from ldclient.interfaces import (
    Basis,
    BasisResult,
    ChangeSetBuilder,
    DataSourceState,
    DataSourceStatus,
    FlagChange,
    Initializer,
    IntentCode,
    ObjectKind,
    Selector,
    SelectorStore,
    Synchronizer,
    Update
)
from ldclient.versioned_data_kind import FEATURES


class MockDataSourceBuilder(DataSourceBuilder):  # pylint: disable=too-few-public-methods
    """A simple wrapper to turn a mock Synchronizer into a DataSourceBuilder."""

    def __init__(self, mock_synchronizer: Synchronizer):
        self._mock = mock_synchronizer

    def build(self, config: Config) -> Synchronizer:  # pylint: disable=unused-argument
        return self._mock


class _StaticInitializer(Initializer):
    """A test initializer that returns a fixed BasisResult."""

    def __init__(self, name: str, result: BasisResult):
        self._name = name
        self._result = result

    @property
    def name(self) -> str:
        return self._name

    def fetch(self, ss: SelectorStore) -> BasisResult:  # pylint: disable=unused-argument
        return self._result


class _InitializerBuilder(DataSourceBuilder):  # pylint: disable=too-few-public-methods
    """Wraps a static Initializer as a DataSourceBuilder."""

    def __init__(self, initializer: Initializer):
        self._initializer = initializer

    def build(self, config: Config) -> Initializer:  # pylint: disable=unused-argument
        return self._initializer


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
        initializers=[td_initializer.builder],
        synchronizers=[td_synchronizer.builder],
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

    fdv2.flag_change_listeners.add(listener)

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
        synchronizers=[td.builder],
    )

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    changed = Event()
    changes: List[FlagChange] = []

    def listener(flag_change: FlagChange):
        changes.append(flag_change)
        changed.set()

    fdv2.flag_change_listeners.add(listener)

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
        synchronizers=[td.builder],
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
        initializers=[td.builder],
        synchronizers=[MockDataSourceBuilder(mock), td.builder],  # Primary fails, secondary succeeds
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
    fdv2.flag_change_listeners.add(listener)
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
        initializers=[td.builder],
        synchronizers=[MockDataSourceBuilder(mock), MockDataSourceBuilder(mock)],  # Both synchronizers fail
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

    # Simulate a synchronizer that yields an OFF state with fallback_to_fdv1=True
    mock_primary.sync.return_value = iter([
        Update(
            state=DataSourceState.OFF,
            fallback_to_fdv1=True
        )
    ])

    # Create FDv1 fallback data source with actual data
    td_fdv1 = TestDataV2.data_source()
    td_fdv1.update(td_fdv1.flag("fdv1-flag").on(True))

    data_system_config = DataSystemConfig(
        initializers=None,
        synchronizers=[MockDataSourceBuilder(mock_primary)],
        fdv1_fallback_synchronizer=td_fdv1.builder,
    )

    changed = Event()
    changes: List[FlagChange] = []

    def listener(flag_change: FlagChange):
        changes.append(flag_change)
        changed.set()

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
    fdv2.flag_change_listeners.add(listener)
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
            fallback_to_fdv1=True
        )
    ])

    # Create FDv1 fallback data source
    td_fdv1 = TestDataV2.data_source()
    td_fdv1.update(td_fdv1.flag("fdv1-fallback-flag").on(True))

    data_system_config = DataSystemConfig(
        initializers=None,
        synchronizers=[MockDataSourceBuilder(mock_primary)],
        fdv1_fallback_synchronizer=td_fdv1.builder,
    )

    changed = Event()
    changes: List[FlagChange] = []

    def listener(flag_change: FlagChange):
        changes.append(flag_change)
        if flag_change.key == "fdv1-update-flag":
            changed.set()

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
    fdv2.flag_change_listeners.add(listener)
    fdv2.start(set_on_ready)

    assert set_on_ready.wait(1), "Data system did not become ready in time"

    # Update a different flag than the one in initial data to verify FDv1 is
    # actively processing updates (not just init)
    td_fdv1.update(td_fdv1.flag("fdv1-update-flag").on(True))
    assert changed.wait(2), "Flag change listener was not called in time"

    assert any(c.key == "fdv1-update-flag" for c in changes)


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
            fallback_to_fdv1=True
        )
    ])

    # Create FDv1 fallback with different data
    td_fdv1 = TestDataV2.data_source()
    td_fdv1.update(td_fdv1.flag("fdv1-replacement-flag").on(True))

    data_system_config = DataSystemConfig(
        initializers=[td_initializer.builder],
        synchronizers=[MockDataSourceBuilder(mock_primary)],
        fdv1_fallback_synchronizer=td_fdv1.builder,
    )

    changed = Event()
    changes: List[FlagChange] = []

    def listener(flag_change: FlagChange):
        changes.append(flag_change)
        if len(changes) >= 2:
            changed.set()

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
    fdv2.flag_change_listeners.add(listener)
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
            fallback_to_fdv1=False  # No fallback
        )
    ])

    # Create mock secondary
    mock_secondary: Synchronizer = Mock()
    mock_secondary.name = "mock-secondary"
    mock_secondary.stop = Mock()
    mock_secondary.sync.return_value = iter([
        Update(
            state=DataSourceState.VALID,
            fallback_to_fdv1=False
        )
    ])

    # Create FDv1 fallback (should not be used)
    td_fdv1 = TestDataV2.data_source()
    td_fdv1.update(td_fdv1.flag("fdv1-should-not-appear").on(True))

    data_system_config = DataSystemConfig(
        initializers=None,
        synchronizers=[MockDataSourceBuilder(mock_primary), MockDataSourceBuilder(mock_secondary)],
        fdv1_fallback_synchronizer=td_fdv1.builder,
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
            fallback_to_fdv1=True
        )
    ])

    # Create FDv1 fallback
    td_fdv1 = TestDataV2.data_source()
    td_fdv1.update(td_fdv1.flag("fdv1-flag").on(True))

    data_system_config = DataSystemConfig(
        initializers=None,
        synchronizers=[MockDataSourceBuilder(mock_primary)],
        fdv1_fallback_synchronizer=td_fdv1.builder,
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
            initializers=[file_ds_builder([path]), td_initializer.builder],
            synchronizers=[td_synchronizer.builder],
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

        fdv2.flag_change_listeners.add(listener)

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
            initializers=[td_initializer.builder, file_ds_builder([path])],
            synchronizers=None,
        )

        set_on_ready = Event()
        fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
        count = 0

        def listener(_: FlagChange):
            nonlocal count
            count += 1

        fdv2.flag_change_listeners.add(listener)

        fdv2.start(set_on_ready)
        assert set_on_ready.wait(1), "Data system did not become ready in time"
        assert count == 1, "Invalid initializer process"
        fdv2.stop()
    finally:
        os.remove(path)


def _basis_with_one_flag(flag_key: str, fallback_to_fdv1: bool) -> Basis:
    """Builds a Basis containing a single flag, optionally carrying the FDv1 fallback signal."""
    builder = ChangeSetBuilder()
    builder.start(IntentCode.TRANSFER_FULL)
    builder.add_put(
        ObjectKind.FLAG,
        flag_key,
        1,
        {"key": flag_key, "version": 1, "on": True, "variations": [True, False]},
    )
    change_set = builder.finish(Selector(state="initializer-state", version=1))
    return Basis(
        change_set=change_set,
        persist=True,
        environment_id=None,
        fallback_to_fdv1=fallback_to_fdv1,
    )


def test_fdv2_initializer_fallback_with_payload_engages_fdv1_synchronizer():
    """
    When an initializer returns a successful Basis carrying the FDv1 fallback
    signal, the SDK must apply the payload, skip configured FDv2 synchronizers,
    and run the FDv1 Fallback Synchronizer instead.
    """
    init = _StaticInitializer(
        "fallback-initializer",
        _Success(value=_basis_with_one_flag("init-flag", fallback_to_fdv1=True)),
    )

    # FDv2 streaming synchronizer that should never produce updates because we
    # were directed to fall back during initialization.
    fdv2_sync_mock: Synchronizer = Mock()
    fdv2_sync_mock.name = "fdv2-sync-should-not-run"
    fdv2_sync_mock.stop = Mock()
    fdv2_sync_mock.sync.return_value = iter([])

    td_fdv1 = TestDataV2.data_source()
    td_fdv1.update(td_fdv1.flag("fdv1-flag").on(True))

    data_system_config = DataSystemConfig(
        initializers=[_InitializerBuilder(init)],
        synchronizers=[MockDataSourceBuilder(fdv2_sync_mock)],
        fdv1_fallback_synchronizer=td_fdv1.builder,
    )

    fdv1_flag_seen = Event()
    init_flag_seen = Event()

    def listener(flag_change: FlagChange):
        if flag_change.key == "init-flag":
            init_flag_seen.set()
        elif flag_change.key == "fdv1-flag":
            fdv1_flag_seen.set()

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
    fdv2.flag_change_listeners.add(listener)
    fdv2.start(set_on_ready)

    assert set_on_ready.wait(1), "Data system did not become ready in time"
    # The initializer's payload must be applied before the handoff.
    assert init_flag_seen.wait(1), "Initializer payload was not applied before fallback"
    # The configured FDv2 synchronizer must not run after a directive.
    fdv2_sync_mock.sync.assert_not_called()
    # And the FDv1 Fallback Synchronizer must take over.
    assert fdv1_flag_seen.wait(1), "FDv1 fallback synchronizer did not run after directive"


def test_fdv2_initializer_fallback_without_fdv1_configured_transitions_to_off():
    """
    When an initializer signals FDv1 fallback but no FDv1 Fallback Synchronizer
    is configured, the data source status must transition to OFF rather than
    silently dropping the directive or stalling at INITIALIZING.
    """
    init = _StaticInitializer(
        "fallback-initializer-no-fdv1",
        _Fail(
            error="boom",
            exception=None,
            headers={_LD_FD_FALLBACK_HEADER: 'true'},
        ),
    )

    # An FDv2 synchronizer that would otherwise be tried -- it must not run.
    fdv2_sync_mock: Synchronizer = Mock()
    fdv2_sync_mock.name = "fdv2-sync-should-not-run"
    fdv2_sync_mock.stop = Mock()
    fdv2_sync_mock.sync.return_value = iter([])

    data_system_config = DataSystemConfig(
        initializers=[_InitializerBuilder(init)],
        synchronizers=[MockDataSourceBuilder(fdv2_sync_mock)],
        fdv1_fallback_synchronizer=None,
    )

    off = Event()

    def listener(status: DataSourceStatus):
        if status.state == DataSourceState.OFF:
            off.set()

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
    fdv2.data_source_status_provider.add_listener(listener)
    fdv2.start(set_on_ready)

    assert set_on_ready.wait(1), "Data system did not become ready in time"
    assert off.wait(1), "Data source did not transition to OFF after directive without fallback"
    fdv2_sync_mock.sync.assert_not_called()


def test_fdv2_synchronizer_fallback_on_success_with_payload():
    """
    When a synchronizer emits a Valid update carrying both a ChangeSet and the
    FDv1 fallback signal, the SDK must apply the payload before terminally
    handing off to the FDv1 Fallback Synchronizer.
    """
    builder = ChangeSetBuilder()
    builder.start(IntentCode.TRANSFER_FULL)
    builder.add_put(
        ObjectKind.FLAG,
        "fdv2-payload-flag",
        1,
        {"key": "fdv2-payload-flag", "version": 1, "on": True, "variations": [True, False]},
    )
    change_set = builder.finish(Selector(state="state", version=1))

    mock_primary: Synchronizer = Mock()
    mock_primary.name = "mock-primary"
    mock_primary.stop = Mock()
    mock_primary.sync.return_value = iter([
        Update(
            state=DataSourceState.VALID,
            change_set=change_set,
            fallback_to_fdv1=True,
        )
    ])

    td_fdv1 = TestDataV2.data_source()
    td_fdv1.update(td_fdv1.flag("fdv1-flag").on(True))

    data_system_config = DataSystemConfig(
        initializers=None,
        synchronizers=[MockDataSourceBuilder(mock_primary)],
        fdv1_fallback_synchronizer=td_fdv1.builder,
    )

    fdv1_flag_seen = Event()
    payload_flag_seen = Event()

    def listener(flag_change: FlagChange):
        if flag_change.key == "fdv2-payload-flag":
            payload_flag_seen.set()
        elif flag_change.key == "fdv1-flag":
            fdv1_flag_seen.set()

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
    fdv2.flag_change_listeners.add(listener)
    fdv2.start(set_on_ready)

    assert set_on_ready.wait(1), "Data system did not become ready in time"
    # The Valid update's payload must be applied before the handoff.
    assert payload_flag_seen.wait(1), "FDv2 payload was not applied before fallback"
    assert fdv1_flag_seen.wait(1), "FDv1 fallback synchronizer did not run after directive"
