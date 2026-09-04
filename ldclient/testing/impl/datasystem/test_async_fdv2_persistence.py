# pylint: disable=missing-docstring

"""
Integration tests for ``AsyncFDv2`` wired to an async persistent store.

These drive the async data system end-to-end: the coordinator creates an
``AsyncStore`` and persists through it.
"""

import asyncio
import logging
from typing import Any, Dict, List, Mapping, Optional

import pytest

from ldclient.async_config import AsyncConfig, AsyncDataSystemConfig
from ldclient.impl.datasystem import DataAvailability
from ldclient.impl.datasystem.async_fdv2 import (
    AsyncFDv2,
    _AsyncReadOnlyStoreView
)
from ldclient.impl.datasystem.async_store import AsyncStore
from ldclient.impl.listeners import Listeners
from ldclient.integrations.test_datav2 import TestDataV2
from ldclient.interfaces import (
    AsyncFeatureStore,
    DataStoreMode,
    DataStoreStatus,
    FlagChange
)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind


class StubAsyncFeatureStore(AsyncFeatureStore):
    """An async feature store stub that records operations and lets tests
    inspect state. Availability can be toggled to exercise recovery paths."""

    def __init__(
        self,
        initial_data: Optional[Dict[VersionedDataKind, Dict[str, dict]]] = None,
    ):
        self._data: Dict[VersionedDataKind, Dict[str, dict]] = {
            FEATURES: {},
            SEGMENTS: {},
        }
        self._initialized = False
        self._available = True

        self.init_called_count = 0
        self.upsert_calls: List[tuple] = []
        self.delete_calls: List[tuple] = []
        self.closed = False

        # Controls for the recovery-path tests.
        self.fail_init = False           # raise from init() when True
        self.init_gate: Optional[asyncio.Event] = None  # if set, init() waits on it
        self.init_after_close = False    # set if init() ever runs after close()

        if initial_data:
            self._data = {
                FEATURES: dict(initial_data.get(FEATURES, {})),
                SEGMENTS: dict(initial_data.get(SEGMENTS, {})),
            }
            self._initialized = True

    async def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        if self.init_gate is not None:
            await self.init_gate.wait()
        if self.closed:
            self.init_after_close = True
        if self.fail_init:
            raise RuntimeError("store down")
        self.init_called_count += 1
        self._data = {
            FEATURES: dict(all_data.get(FEATURES, {})),
            SEGMENTS: dict(all_data.get(SEGMENTS, {})),
        }
        self._initialized = True

    async def get(self, kind: VersionedDataKind, key: str) -> Optional[Any]:
        return self._data.get(kind, {}).get(key)

    async def all(self, kind: VersionedDataKind) -> Dict[str, Any]:
        return dict(self._data.get(kind, {}))

    async def upsert(self, kind: VersionedDataKind, item: dict) -> bool:
        self.upsert_calls.append((kind, item.get("key"), item.get("version")))
        key = item["key"]
        existing = self._data.get(kind, {}).get(key)
        if not existing or existing.get("version", 0) < item.get("version", 0):
            self._data[kind][key] = item
            return True
        return False

    async def delete(self, kind: VersionedDataKind, key: str, version: int) -> bool:
        self.delete_calls.append((kind, key, version))
        return await self.upsert(kind, {"key": key, "version": version, "deleted": True})

    @property
    def initialized(self) -> bool:
        return self._initialized

    async def is_initialized(self) -> bool:
        return self._initialized

    async def is_available(self) -> bool:
        return self._available

    async def close(self) -> None:
        self.closed = True

    def snapshot(self) -> Dict[VersionedDataKind, Dict[str, dict]]:
        return {FEATURES: dict(self._data[FEATURES]), SEGMENTS: dict(self._data[SEGMENTS])}

    def reset_operation_tracking(self):
        self.init_called_count = 0
        self.upsert_calls = []
        self.delete_calls = []


def _flag_dict(key: str, version: int, on: bool = True) -> dict:
    return {
        "key": key,
        "version": version,
        "on": on,
        "variations": [True, False],
        "fallthrough": {"variation": 0},
    }


async def _wait_for(event: asyncio.Event, timeout: float = 2.0):
    await asyncio.wait_for(event.wait(), timeout=timeout)


@pytest.mark.asyncio
async def test_async_persistent_store_read_write_mode():
    persistent_store = StubAsyncFeatureStore()

    td = TestDataV2.data_source()
    td.update(td.flag("new-flag").on(True))

    data_system_config = AsyncDataSystemConfig(
        data_store_mode=DataStoreMode.READ_WRITE,
        data_store=persistent_store,
        initializers=None,
        synchronizers=[td.async_builder],
    )

    ready = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)

    # The coordinator must build an AsyncStore -- not a sync Store.
    assert isinstance(fdv2._store, AsyncStore)

    fdv2.start(ready)
    await _wait_for(ready)
    assert (await fdv2.data_availability()).at_least(DataAvailability.REFRESHED)

    # A full transfer persists through init(), and the new flag lands in the store.
    assert persistent_store.init_called_count >= 1
    assert "new-flag" in persistent_store.snapshot()[FEATURES]

    await fdv2.stop()
    assert persistent_store.closed is True


@pytest.mark.asyncio
async def test_async_persistent_store_read_only_mode():
    initial = {FEATURES: {"existing-flag": _flag_dict("existing-flag", 1)}, SEGMENTS: {}}
    persistent_store = StubAsyncFeatureStore(initial)
    persistent_store.reset_operation_tracking()

    td = TestDataV2.data_source()
    td.update(td.flag("new-flag").on(True))

    data_system_config = AsyncDataSystemConfig(
        data_store_mode=DataStoreMode.READ_ONLY,
        data_store=persistent_store,
        initializers=None,
        synchronizers=[td.async_builder],
    )

    ready = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready)
    await _wait_for(ready)
    assert (await fdv2.data_availability()).at_least(DataAvailability.REFRESHED)

    # READ_ONLY: nothing is written back to the persistent store.
    assert persistent_store.init_called_count == 0
    assert len(persistent_store.upsert_calls) == 0

    # In-memory now serves reads.
    flag = await fdv2.store.get(FEATURES, "new-flag")
    assert flag is not None and flag["key"] == "new-flag"

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_persistent_store_delta_updates_read_write():
    persistent_store = StubAsyncFeatureStore()

    td = TestDataV2.data_source()
    td.update(td.flag("feature-flag").on(True))

    data_system_config = AsyncDataSystemConfig(
        data_store_mode=DataStoreMode.READ_WRITE,
        data_store=persistent_store,
        initializers=None,
        synchronizers=[td.async_builder],
    )

    ready = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)

    flag_changed = asyncio.Event()
    change_count = 0

    def listener(_change: FlagChange):
        nonlocal change_count
        change_count += 1
        if change_count == 2:  # first from initial sync, second from our update
            flag_changed.set()

    fdv2.flag_change_listeners.add(listener)
    fdv2.start(ready)
    await _wait_for(ready)

    persistent_store.reset_operation_tracking()

    # A delta update.
    td.update(td.flag("feature-flag").on(False))
    await _wait_for(flag_changed)

    # The delta persists via upsert.
    assert any(call[1] == "feature-flag" for call in persistent_store.upsert_calls)
    assert persistent_store.snapshot()[FEATURES]["feature-flag"]["on"] is False

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_persistent_store_outage_recovery_flushes_on_recovery():
    persistent_store = StubAsyncFeatureStore()

    td = TestDataV2.data_source()
    td.update(td.flag("feature-flag").on(True))

    data_system_config = AsyncDataSystemConfig(
        data_store_mode=DataStoreMode.READ_WRITE,
        data_store=persistent_store,
        initializers=None,
        synchronizers=[td.async_builder],
    )

    ready = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)

    new_flag_applied = asyncio.Event()

    def listener(change: FlagChange):
        if change.key == "new-flag":
            new_flag_applied.set()

    fdv2.flag_change_listeners.add(listener)
    fdv2.start(ready)
    await _wait_for(ready)

    assert "feature-flag" in persistent_store.snapshot()[FEATURES]
    persistent_store.reset_operation_tracking()

    # A runtime update lands in memory (and the store, since READ_WRITE).
    td.update(td.flag("new-flag").on(False))
    await _wait_for(new_flag_applied)

    persistent_store.reset_operation_tracking()

    # Store comes back online with stale data -> the coordinator schedules a commit.
    fdv2._persistent_store_outage_recovery(DataStoreStatus(available=True, stale=True))

    # The commit is scheduled with asyncio.ensure_future; let it run.
    for _ in range(20):
        await asyncio.sleep(0.01)
        if persistent_store.init_called_count > 0:
            break

    assert persistent_store.init_called_count > 0, "Store should have been reinitialized"
    snapshot = persistent_store.snapshot()
    assert "feature-flag" in snapshot[FEATURES]
    assert "new-flag" in snapshot[FEATURES]

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_persistent_store_outage_recovery_no_flush_when_not_stale():
    persistent_store = StubAsyncFeatureStore()

    td = TestDataV2.data_source()
    td.update(td.flag("feature-flag").on(True))

    data_system_config = AsyncDataSystemConfig(
        data_store_mode=DataStoreMode.READ_WRITE,
        data_store=persistent_store,
        initializers=None,
        synchronizers=[td.async_builder],
    )

    ready = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready)
    await _wait_for(ready)

    persistent_store.reset_operation_tracking()
    fdv2._persistent_store_outage_recovery(DataStoreStatus(available=True, stale=False))

    # Give any (erroneously) scheduled task a chance to run.
    await asyncio.sleep(0.05)
    assert persistent_store.init_called_count == 0

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_persistent_store_outage_recovery_no_flush_when_unavailable():
    persistent_store = StubAsyncFeatureStore()

    td = TestDataV2.data_source()
    td.update(td.flag("feature-flag").on(True))

    data_system_config = AsyncDataSystemConfig(
        data_store_mode=DataStoreMode.READ_WRITE,
        data_store=persistent_store,
        initializers=None,
        synchronizers=[td.async_builder],
    )

    ready = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready)
    await _wait_for(ready)

    persistent_store.reset_operation_tracking()
    fdv2._persistent_store_outage_recovery(DataStoreStatus(available=False, stale=True))

    await asyncio.sleep(0.05)
    assert persistent_store.init_called_count == 0

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_store_view_reads_through_async_persistent_store():
    """Before the memory store has data, the active store is the async
    persistent store, so the view must await its get/all and decode dicts."""
    raw = _flag_dict("flag-a", 1)
    inner = StubAsyncFeatureStore({FEATURES: {"flag-a": raw}, SEGMENTS: {}})

    store = AsyncStore(Listeners(), Listeners())
    store.with_async_persistence(inner, True, None)

    view = _AsyncReadOnlyStoreView(store)

    # Active store is the async persistent store -> get()/all() are awaitables.
    got = await view.get(FEATURES, "flag-a")
    assert got == FEATURES.decode(raw)
    assert not isinstance(got, dict)  # dict decoded into a model

    all_flags = await view.all(FEATURES)
    assert set(all_flags.keys()) == {"flag-a"}
    assert all_flags["flag-a"] == FEATURES.decode(raw)

    assert await view.get(FEATURES, "missing") is None


@pytest.mark.asyncio
async def test_async_store_view_reads_from_in_memory_after_swap():
    """After a full apply the active store is the sync in-memory store, whose
    reads are not awaitable; the view's isawaitable gate must handle that too."""
    from ldclient.interfaces import (
        Change,
        ChangeSet,
        ChangeType,
        IntentCode,
        ObjectKind,
        Selector
    )

    inner = StubAsyncFeatureStore()
    store = AsyncStore(Listeners(), Listeners())
    store.with_async_persistence(inner, True, None)
    view = _AsyncReadOnlyStoreView(store)

    changeset = ChangeSet(
        intent_code=IntentCode.TRANSFER_FULL,
        changes=[Change(action=ChangeType.PUT, kind=ObjectKind.FLAG, key="flag-a", version=1, object=_flag_dict("flag-a", 1))],
        selector=Selector.no_selector(),
    )
    await store.apply(changeset, True)

    # Active store is now the in-memory store (non-awaitable reads).
    got = await view.get(FEATURES, "flag-a")
    assert got == FEATURES.decode(_flag_dict("flag-a", 1))
    assert not isinstance(got, dict)


async def _started_fdv2_with_store(store):
    td = TestDataV2.data_source()
    td.update(td.flag("feature-flag").on(True))
    cfg = AsyncDataSystemConfig(
        data_store_mode=DataStoreMode.READ_WRITE,
        data_store=store,
        initializers=None,
        synchronizers=[td.async_builder],
    )
    ready = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), cfg)
    fdv2.start(ready)
    await _wait_for(ready)
    return fdv2


@pytest.mark.asyncio
async def test_recovery_skipped_and_no_write_after_stop():
    """After stop() has closed the store, a recovery firing is skipped: it
    schedules no task and never writes to the closed store."""
    store = StubAsyncFeatureStore()
    fdv2 = await _started_fdv2_with_store(store)

    await fdv2.stop()
    assert store.closed is True
    store.reset_operation_tracking()

    fdv2._persistent_store_outage_recovery(DataStoreStatus(available=True, stale=True))
    await asyncio.sleep(0.05)

    assert store.init_called_count == 0
    assert store.init_after_close is False
    assert not any(t.get_name() == "AsyncFDv2-store-recovery" for t in fdv2._runner._tasks)


@pytest.mark.asyncio
async def test_recovery_task_spawned_before_stop_is_drained_by_stop():
    """A recovery commit in flight is owned by the coordinator's runner, so
    stop() cancels and awaits it -- it does not leak."""
    store = StubAsyncFeatureStore()
    fdv2 = await _started_fdv2_with_store(store)

    # A long-running commit so the recovery task is reliably in flight.
    started = asyncio.Event()

    async def slow_commit():
        started.set()
        await asyncio.sleep(30)
        return None

    fdv2._store.commit = slow_commit  # type: ignore[assignment]

    fdv2._persistent_store_outage_recovery(DataStoreStatus(available=True, stale=True))
    await asyncio.wait_for(started.wait(), timeout=2)

    assert any(t.get_name() == "AsyncFDv2-store-recovery" for t in fdv2._runner._tasks)

    await fdv2.stop()
    assert not any(t.get_name() == "AsyncFDv2-store-recovery" for t in fdv2._runner._tasks)


@pytest.mark.asyncio
async def test_recovery_error_is_logged_not_swallowed(caplog):
    """A failing recovery commit is logged and does not crash the coordinator
    or leak an unretrieved task exception."""
    store = StubAsyncFeatureStore()
    fdv2 = await _started_fdv2_with_store(store)

    store.reset_operation_tracking()
    store.fail_init = True
    store._available = False  # keep the wrapper's availability poller from flapping

    with caplog.at_level(logging.ERROR):
        fdv2._persistent_store_outage_recovery(DataStoreStatus(available=True, stale=True))
        await asyncio.sleep(0.1)

    assert any("Failed to reinitialize data store" in r.getMessage() for r in caplog.records)

    # The coordinator is still usable and shuts down cleanly.
    flag = await fdv2.store.get(FEATURES, "feature-flag")
    assert flag is not None
    store.fail_init = False
    await fdv2.stop()
