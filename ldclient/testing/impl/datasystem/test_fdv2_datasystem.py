# pylint: disable=missing-docstring

from threading import Event
from typing import List

from mock import Mock

from ldclient.config import Config, DataSystemConfig
from ldclient.impl.datasystem import DataAvailability, Synchronizer
from ldclient.impl.datasystem.fdv2 import FDv2
from ldclient.integrations.test_datav2 import TestDataV2
from ldclient.interfaces import DataSourceState, DataSourceStatus, FlagChange


def test_two_phase_init():
    td_initializer = TestDataV2.data_source()
    td_initializer.update(td_initializer.flag("feature-flag").on(True))

    td_synchronizer = TestDataV2.data_source()
    td_synchronizer.update(td_synchronizer.flag("feature-flag").on(True))
    data_system_config = DataSystemConfig(
        initializers=[td_initializer.build_initializer],
        primary_synchronizer=td_synchronizer.build_synchronizer,
    )

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    changed = Event()
    changes: List[FlagChange] = []
    count = 0

    def listener(flag_change: FlagChange):
        nonlocal count, changes
        count += 1
        changes.append(flag_change)

        if count == 2:
            changed.set()

    fdv2.flag_tracker.add_listener(listener)

    fdv2.start(set_on_ready)
    assert set_on_ready.wait(1), "Data system did not become ready in time"

    td_synchronizer.update(td_synchronizer.flag("feature-flag").on(False))
    assert changed.wait(1), "Flag change listener was not called in time"
    assert len(changes) == 2
    assert changes[0].key == "feature-flag"
    assert changes[1].key == "feature-flag"


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
