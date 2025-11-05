# pylint: disable=missing-docstring

from threading import Event
from typing import Any, Callable, Dict, List, Mapping, Optional

from ldclient.config import Config, DataSystemConfig
from ldclient.impl.datasystem import DataAvailability
from ldclient.impl.datasystem.fdv2 import FDv2
from ldclient.integrations.test_datav2 import TestDataV2
from ldclient.interfaces import DataStoreMode, FeatureStore, FlagChange
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind


class StubFeatureStore(FeatureStore):
    """
    A simple stub implementation of FeatureStore for testing.
    Records all operations and allows inspection of state.
    """

    def __init__(
        self,
        initial_data: Optional[
            Dict[VersionedDataKind, Dict[str, Dict[Any, Any]]]
        ] = None,
    ):
        self._data: Dict[VersionedDataKind, Dict[str, dict]] = {
            FEATURES: {},
            SEGMENTS: {},
        }
        self._initialized = False
        self._available = True
        self._monitoring_enabled = False

        # Track operations for assertions
        self.init_called_count = 0
        self.upsert_calls: List[tuple] = []
        self.delete_calls: List[tuple] = []
        self.get_calls: List[tuple] = []
        self.all_calls: List[VersionedDataKind] = []

        if initial_data:
            self.init(initial_data)

    def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, Dict[Any, Any]]]):
        self.init_called_count += 1
        self._data = {
            FEATURES: dict(all_data.get(FEATURES, {})),
            SEGMENTS: dict(all_data.get(SEGMENTS, {})),
        }
        self._initialized = True

    def get(
        self,
        kind: VersionedDataKind,
        key: str,
        callback: Callable[[Any], Any] = lambda x: x,
    ):
        self.get_calls.append((kind, key))
        item = self._data.get(kind, {}).get(key)
        return callback(item) if item else None

    def all(
        self, kind: VersionedDataKind, callback: Callable[[Any], Any] = lambda x: x
    ):
        self.all_calls.append(kind)
        items = self._data.get(kind, {})
        return {key: callback(value) for key, value in items.items()}

    def delete(self, kind: VersionedDataKind, key: str, version: int):
        self.delete_calls.append((kind, key, version))
        existing = self._data.get(kind, {}).get(key)
        if existing and existing.get("version", 0) < version:
            self._data[kind][key] = {"key": key, "version": version, "deleted": True}

    def upsert(self, kind: VersionedDataKind, item: dict):
        self.upsert_calls.append((kind, item.get("key"), item.get("version")))
        key = item["key"]
        existing = self._data.get(kind, {}).get(key)
        if not existing or existing.get("version", 0) < item.get("version", 0):
            self._data[kind][key] = item

    @property
    def initialized(self) -> bool:
        return self._initialized

    def is_available(self) -> bool:
        """For monitoring support"""
        return self._available

    def is_monitoring_enabled(self) -> bool:
        """For monitoring support"""
        return self._monitoring_enabled

    def set_available(self, available: bool):
        """Test helper to simulate availability changes"""
        self._available = available

    def enable_monitoring(self):
        """Test helper to enable monitoring"""
        self._monitoring_enabled = True

    def get_data_snapshot(self) -> Mapping[VersionedDataKind, Mapping[str, dict]]:
        """Test helper to get a snapshot of current data"""
        return {
            FEATURES: dict(self._data[FEATURES]),
            SEGMENTS: dict(self._data[SEGMENTS]),
        }

    def reset_operation_tracking(self):
        """Test helper to reset operation tracking"""
        self.init_called_count = 0
        self.upsert_calls = []
        self.delete_calls = []
        self.get_calls = []
        self.all_calls = []


def test_persistent_store_read_only_mode():
    """Test that READ_ONLY mode reads from store but never writes"""
    # Pre-populate persistent store with a flag
    initial_data = {
        FEATURES: {
            "existing-flag": {
                "key": "existing-flag",
                "version": 1,
                "on": True,
                "variations": [True, False],
                "fallthrough": {"variation": 0},
            }
        },
        SEGMENTS: {},
    }

    persistent_store = StubFeatureStore(initial_data)

    # Create synchronizer that will provide new data
    td_synchronizer = TestDataV2.data_source()
    td_synchronizer.update(td_synchronizer.flag("new-flag").on(True))

    data_system_config = DataSystemConfig(
        data_store_mode=DataStoreMode.READ_ONLY,
        data_store=persistent_store,
        initializers=None,
        primary_synchronizer=td_synchronizer.build_synchronizer,
    )

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
    fdv2.start(set_on_ready)

    assert set_on_ready.wait(1), "Data system did not become ready in time"

    # Verify data system is initialized and available
    assert fdv2.data_availability.at_least(DataAvailability.REFRESHED)

    # Verify the store was initialized once (by us) but no additional writes happened
    # The persistent store should have been read from, but not written to
    assert persistent_store.init_called_count == 1  # Only our initial setup
    assert len(persistent_store.upsert_calls) == 0  # No upserts in READ_ONLY mode

    fdv2.stop()


def test_persistent_store_read_write_mode():
    """Test that READ_WRITE mode reads from store and writes updates back"""
    # Pre-populate persistent store with a flag
    initial_data = {
        FEATURES: {
            "existing-flag": {
                "key": "existing-flag",
                "version": 1,
                "on": True,
                "variations": [True, False],
                "fallthrough": {"variation": 0},
            }
        },
        SEGMENTS: {},
    }

    persistent_store = StubFeatureStore(initial_data)
    persistent_store.reset_operation_tracking()  # Reset tracking after initial setup

    # Create synchronizer that will provide new data
    td_synchronizer = TestDataV2.data_source()
    td_synchronizer.update(td_synchronizer.flag("new-flag").on(True))

    data_system_config = DataSystemConfig(
        data_store_mode=DataStoreMode.READ_WRITE,
        data_store=persistent_store,
        initializers=None,
        primary_synchronizer=td_synchronizer.build_synchronizer,
    )

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)
    fdv2.start(set_on_ready)

    assert set_on_ready.wait(1), "Data system did not become ready in time"

    # In READ_WRITE mode, the store should be initialized with new data
    assert (
        persistent_store.init_called_count >= 1
    )  # At least one init call for the new data

    # Verify the new flag was written to persistent store
    snapshot = persistent_store.get_data_snapshot()
    assert "new-flag" in snapshot[FEATURES]

    fdv2.stop()


def test_persistent_store_delta_updates_read_write():
    """Test that delta updates are written to persistent store in READ_WRITE mode"""
    persistent_store = StubFeatureStore()

    # Create synchronizer
    td_synchronizer = TestDataV2.data_source()
    td_synchronizer.update(td_synchronizer.flag("feature-flag").on(True))

    data_system_config = DataSystemConfig(
        data_store_mode=DataStoreMode.READ_WRITE,
        data_store=persistent_store,
        initializers=None,
        primary_synchronizer=td_synchronizer.build_synchronizer,
    )

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    # Set up flag change listener to detect the update
    flag_changed = Event()
    change_count = 0

    def listener(flag_change: FlagChange):
        nonlocal change_count
        change_count += 1
        if (
            change_count == 2
        ):  # First change is from initial sync, second is our update
            flag_changed.set()

    fdv2.flag_tracker.add_listener(listener)
    fdv2.start(set_on_ready)

    assert set_on_ready.wait(1), "Data system did not become ready in time"

    persistent_store.reset_operation_tracking()

    # Make a delta update
    td_synchronizer.update(td_synchronizer.flag("feature-flag").on(False))

    # Wait for the flag change to propagate
    assert flag_changed.wait(1), "Flag change did not propagate in time"

    # Verify the update was written to persistent store
    assert len(persistent_store.upsert_calls) > 0
    assert any(call[1] == "feature-flag" for call in persistent_store.upsert_calls)

    # Verify the updated flag is in the store
    snapshot = persistent_store.get_data_snapshot()
    assert "feature-flag" in snapshot[FEATURES]
    assert snapshot[FEATURES]["feature-flag"]["on"] is False

    fdv2.stop()


def test_persistent_store_delta_updates_read_only():
    """Test that delta updates are NOT written to persistent store in READ_ONLY mode"""
    persistent_store = StubFeatureStore()

    # Create synchronizer
    td_synchronizer = TestDataV2.data_source()
    td_synchronizer.update(td_synchronizer.flag("feature-flag").on(True))

    data_system_config = DataSystemConfig(
        data_store_mode=DataStoreMode.READ_ONLY,
        data_store=persistent_store,
        initializers=None,
        primary_synchronizer=td_synchronizer.build_synchronizer,
    )

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    # Set up flag change listener to detect the update
    flag_changed = Event()
    change_count = [0]  # Use list to allow modification in nested function

    def listener(flag_change: FlagChange):
        change_count[0] += 1
        if (
            change_count[0] == 2
        ):  # First change is from initial sync, second is our update
            flag_changed.set()

    fdv2.flag_tracker.add_listener(listener)
    fdv2.start(set_on_ready)

    assert set_on_ready.wait(1), "Data system did not become ready in time"

    persistent_store.reset_operation_tracking()

    # Make a delta update
    td_synchronizer.update(td_synchronizer.flag("feature-flag").on(False))

    # Wait for the flag change to propagate
    assert flag_changed.wait(1), "Flag change did not propagate in time"

    # Verify NO updates were written to persistent store in READ_ONLY mode
    assert len(persistent_store.upsert_calls) == 0

    fdv2.stop()


def test_persistent_store_with_initializer_and_synchronizer():
    """Test that both initializer and synchronizer data are persisted in READ_WRITE mode"""
    persistent_store = StubFeatureStore()

    # Create initializer with one flag
    td_initializer = TestDataV2.data_source()
    td_initializer.update(td_initializer.flag("init-flag").on(True))

    # Create synchronizer with another flag
    td_synchronizer = TestDataV2.data_source()
    td_synchronizer.update(td_synchronizer.flag("sync-flag").on(False))

    data_system_config = DataSystemConfig(
        data_store_mode=DataStoreMode.READ_WRITE,
        data_store=persistent_store,
        initializers=[td_initializer.build_initializer],
        primary_synchronizer=td_synchronizer.build_synchronizer,
    )

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    # Set up flag change listener to detect when synchronizer data arrives
    sync_flag_arrived = Event()

    def listener(flag_change: FlagChange):
        if flag_change.key == "sync-flag":
            sync_flag_arrived.set()

    fdv2.flag_tracker.add_listener(listener)
    fdv2.start(set_on_ready)

    assert set_on_ready.wait(1), "Data system did not become ready in time"

    # Wait for synchronizer to fully initialize
    # The synchronizer does a full data set transfer, so it replaces the initializer data
    assert sync_flag_arrived.wait(1), "Synchronizer data did not arrive in time"

    # The synchronizer flag should be in the persistent store
    # (it replaces the init-flag since synchronizer does a full data set)
    snapshot = persistent_store.get_data_snapshot()
    assert "init-flag" not in snapshot[FEATURES]
    assert "sync-flag" in snapshot[FEATURES]

    fdv2.stop()


def test_persistent_store_delete_operations():
    """Test that delete operations are written to persistent store in READ_WRITE mode"""
    # We'll need to manually trigger a delete via the store
    # This is more of an integration test with the Store class
    from ldclient.impl.datasystem.protocolv2 import (
        Change,
        ChangeSet,
        ChangeType,
        IntentCode,
        ObjectKind
    )
    from ldclient.impl.datasystem.store import Store
    from ldclient.impl.listeners import Listeners

    # Pre-populate with a flag
    initial_data = {
        FEATURES: {
            "deletable-flag": {
                "key": "deletable-flag",
                "version": 1,
                "on": True,
                "variations": [True, False],
                "fallthrough": {"variation": 0},
            }
        },
        SEGMENTS: {},
    }

    persistent_store = StubFeatureStore(initial_data)

    store = Store(Listeners(), Listeners())
    store.with_persistence(persistent_store, True, None)

    # First, initialize the store with the data so it's in memory
    init_changeset = ChangeSet(
        intent_code=IntentCode.TRANSFER_FULL,
        changes=[
            Change(
                action=ChangeType.PUT,
                kind=ObjectKind.FLAG,
                key="deletable-flag",
                version=1,
                object={
                    "key": "deletable-flag",
                    "version": 1,
                    "on": True,
                    "variations": [True, False],
                    "fallthrough": {"variation": 0},
                },
            )
        ],
        selector=None,
    )
    store.apply(init_changeset, True)

    persistent_store.reset_operation_tracking()

    # Now apply a changeset with a delete
    delete_changeset = ChangeSet(
        intent_code=IntentCode.TRANSFER_CHANGES,
        changes=[
            Change(
                action=ChangeType.DELETE,
                kind=ObjectKind.FLAG,
                key="deletable-flag",
                version=2,
                object=None,
            )
        ],
        selector=None,
    )

    store.apply(delete_changeset, True)

    # Verify delete was called on persistent store
    assert len(persistent_store.upsert_calls) > 0
    assert any(call[1] == "deletable-flag" for call in persistent_store.upsert_calls)


def test_data_store_status_provider():
    """Test that data store status provider is correctly initialized"""
    persistent_store = StubFeatureStore()

    td_synchronizer = TestDataV2.data_source()
    td_synchronizer.update(td_synchronizer.flag("feature-flag").on(True))

    data_system_config = DataSystemConfig(
        data_store_mode=DataStoreMode.READ_WRITE,
        data_store=persistent_store,
        initializers=None,
        primary_synchronizer=td_synchronizer.build_synchronizer,
    )

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    # Verify data store status provider exists
    status_provider = fdv2.data_store_status_provider
    assert status_provider is not None

    # Get initial status
    status = status_provider.status
    assert status is not None
    assert status.available is True

    fdv2.start(set_on_ready)
    assert set_on_ready.wait(1), "Data system did not become ready in time"

    fdv2.stop()


def test_data_store_status_monitoring_not_enabled_by_default():
    """Test that monitoring is not enabled by default"""
    persistent_store = StubFeatureStore()

    td_synchronizer = TestDataV2.data_source()
    td_synchronizer.update(td_synchronizer.flag("feature-flag").on(True))

    data_system_config = DataSystemConfig(
        data_store_mode=DataStoreMode.READ_WRITE,
        data_store=persistent_store,
        initializers=None,
        primary_synchronizer=td_synchronizer.build_synchronizer,
    )

    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    # Monitoring should not be enabled because the store doesn't support it
    status_provider = fdv2.data_store_status_provider
    assert status_provider.is_monitoring_enabled() is False


def test_data_store_status_monitoring_enabled_when_supported():
    """Test that monitoring is enabled when the store supports it"""
    persistent_store = StubFeatureStore()
    persistent_store.enable_monitoring()

    td_synchronizer = TestDataV2.data_source()
    td_synchronizer.update(td_synchronizer.flag("feature-flag").on(True))

    data_system_config = DataSystemConfig(
        data_store_mode=DataStoreMode.READ_WRITE,
        data_store=persistent_store,
        initializers=None,
        primary_synchronizer=td_synchronizer.build_synchronizer,
    )

    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    # Monitoring should be enabled
    status_provider = fdv2.data_store_status_provider
    assert status_provider.is_monitoring_enabled() is True


def test_no_persistent_store_status_provider_without_store():
    """Test that data store status provider exists even without a persistent store"""
    td_synchronizer = TestDataV2.data_source()
    td_synchronizer.update(td_synchronizer.flag("feature-flag").on(True))

    data_system_config = DataSystemConfig(
        data_store_mode=DataStoreMode.READ_WRITE,
        data_store=None,
        initializers=None,
        primary_synchronizer=td_synchronizer.build_synchronizer,
    )

    set_on_ready = Event()
    fdv2 = FDv2(Config(sdk_key="dummy"), data_system_config)

    # Status provider should exist but not be monitoring
    status_provider = fdv2.data_store_status_provider
    assert status_provider is not None
    assert status_provider.is_monitoring_enabled() is False

    fdv2.start(set_on_ready)
    assert set_on_ready.wait(1), "Data system did not become ready in time"

    fdv2.stop()
