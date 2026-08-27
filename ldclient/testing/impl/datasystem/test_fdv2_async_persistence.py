# pylint: disable=missing-docstring

import sys
from typing import Any, Dict, List, Mapping, Optional
from unittest.mock import patch

import pytest

from ldclient.async_config import AsyncConfig, AsyncDataSystemConfig
from ldclient.async_feature_store_helpers import AsyncCachingStoreWrapper
from ldclient.feature_store import CacheConfig
from ldclient.impl.datasystem import DataAvailability
from ldclient.impl.datasystem.async_fdv2 import AsyncFDv2
from ldclient.impl.datasystem.async_store import AsyncStore
from ldclient.impl.datasystem.store import Store
from ldclient.impl.listeners import Listeners
from ldclient.interfaces import (
    AsyncFeatureStore,
    AsyncFeatureStoreCore,
    Change,
    ChangeSet,
    ChangeType,
    DataStoreMode,
    IntentCode,
    ObjectKind,
    Selector
)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind


class FakeAsyncFeatureStore(AsyncFeatureStore):
    """An in-memory async feature store that records the operations it receives."""

    def __init__(self):
        self._data: Dict[VersionedDataKind, Dict[str, dict]] = {FEATURES: {}, SEGMENTS: {}}
        self._inited = False

        self.init_called_count = 0
        self.upsert_calls: List[tuple] = []
        self.closed = False

    async def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        self.init_called_count += 1
        self._data = {
            FEATURES: dict(all_data.get(FEATURES, {})),
            SEGMENTS: dict(all_data.get(SEGMENTS, {})),
        }
        self._inited = True

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
        return await self.upsert(kind, {"key": key, "version": version, "deleted": True})

    @property
    def initialized(self) -> bool:
        return self._inited

    async def is_initialized(self) -> bool:
        return self._inited

    async def close(self) -> None:
        self.closed = True

    def snapshot(self) -> Dict[VersionedDataKind, Dict[str, dict]]:
        return {FEATURES: dict(self._data[FEATURES]), SEGMENTS: dict(self._data[SEGMENTS])}


def _flag(key: str, version: int, on: bool) -> dict:
    return {
        "key": key,
        "version": version,
        "on": on,
        "variations": [True, False],
        "fallthrough": {"variation": 0},
    }


def _full_changeset(key: str, version: int, on: bool) -> ChangeSet:
    return ChangeSet(
        intent_code=IntentCode.TRANSFER_FULL,
        changes=[Change(action=ChangeType.PUT, kind=ObjectKind.FLAG, key=key, version=version, object=_flag(key, version, on))],
        selector=Selector.no_selector(),
    )


def _delta_changeset(key: str, version: int, on: bool) -> ChangeSet:
    return ChangeSet(
        intent_code=IntentCode.TRANSFER_CHANGES,
        changes=[Change(action=ChangeType.PUT, kind=ObjectKind.FLAG, key=key, version=version, object=_flag(key, version, on))],
        selector=Selector.no_selector(),
    )


@pytest.mark.asyncio
async def test_apply_full_transfer_persists_via_init():
    async_store = FakeAsyncFeatureStore()
    store = AsyncStore(Listeners(), Listeners())
    store.with_async_persistence(async_store, True, None)

    await store.apply(_full_changeset("flag-a", 1, True), True)

    # After a full transfer the memory store is authoritative and serves reads
    assert store.get_active_store() is store._memory_store
    flag = store._memory_store.get(FEATURES, "flag-a")
    assert flag is not None
    assert flag["key"] == "flag-a"

    # The async store received the init
    assert async_store.init_called_count == 1
    assert "flag-a" in async_store.snapshot()[FEATURES]


@pytest.mark.asyncio
async def test_apply_delta_persists_via_upsert():
    async_store = FakeAsyncFeatureStore()
    store = AsyncStore(Listeners(), Listeners())
    store.with_async_persistence(async_store, True, None)

    await store.apply(_full_changeset("flag-a", 1, True), True)
    async_store.init_called_count = 0
    async_store.upsert_calls = []

    await store.apply(_delta_changeset("flag-a", 2, False), True)

    assert any(call[1] == "flag-a" and call[2] == 2 for call in async_store.upsert_calls)
    assert async_store.snapshot()[FEATURES]["flag-a"]["on"] is False


@pytest.mark.asyncio
async def test_apply_read_only_does_not_persist():
    async_store = FakeAsyncFeatureStore()
    store = AsyncStore(Listeners(), Listeners())
    # writable=False -> READ_ONLY: never write to the store
    store.with_async_persistence(async_store, False, None)

    await store.apply(_full_changeset("flag-a", 1, True), True)
    await store.apply(_delta_changeset("flag-a", 2, False), True)

    assert async_store.init_called_count == 0
    assert async_store.upsert_calls == []
    # In-memory store still updated
    assert store._memory_store.get(FEATURES, "flag-a") is not None


@pytest.mark.asyncio
async def test_apply_fires_change_set_listeners():
    async_store = FakeAsyncFeatureStore()
    received: List[ChangeSet] = []
    change_set_listeners = Listeners()
    change_set_listeners.add(lambda cs: received.append(cs))

    store = AsyncStore(Listeners(), change_set_listeners)
    store.with_async_persistence(async_store, True, None)

    cs = _full_changeset("flag-a", 1, True)
    await store.apply(cs, True)

    assert received == [cs]


@pytest.mark.asyncio
async def test_commit_writes_memory_to_store():
    async_store = FakeAsyncFeatureStore()
    store = AsyncStore(Listeners(), Listeners())
    store.with_async_persistence(async_store, True, None)

    # Populate memory without persisting yet (read-only apply through memory)
    await store.apply(_full_changeset("flag-a", 1, True), True)
    async_store.init_called_count = 0

    err = await store.commit()
    assert err is None
    assert async_store.init_called_count == 1
    assert "flag-a" in async_store.snapshot()[FEATURES]


@pytest.mark.asyncio
async def test_commit_returns_error_on_failure():
    class FailingStore(FakeAsyncFeatureStore):
        async def init(self, all_data):
            raise RuntimeError("boom")

    async_store = FailingStore()
    store = AsyncStore(Listeners(), Listeners())
    # Read-only so the deferred persist is skipped and memory is populated first.
    store.with_async_persistence(async_store, False, None)
    await store.apply(_full_changeset("flag-a", 1, True), True)

    # Now make it writable and commit, which triggers the failing init.
    store._persistent_store_writable = True
    err = await store.commit()
    assert isinstance(err, RuntimeError)
    assert str(err) == "boom"


@pytest.mark.asyncio
async def test_commit_returns_error_when_snapshot_encode_raises():
    """If encoding the memory snapshot raises, commit() returns the exception
    rather than raising it."""
    async_store = FakeAsyncFeatureStore()
    store = AsyncStore(Listeners(), Listeners())
    store.with_async_persistence(async_store, True, None)

    # Populate memory so the snapshot iterates a flag and calls FEATURES.encode.
    await store.apply(_full_changeset("flag-a", 1, True), True)
    async_store.init_called_count = 0

    with patch.object(FEATURES, "encode", side_effect=RuntimeError("encode boom")):
        err = await store.commit()

    assert isinstance(err, RuntimeError)
    assert str(err) == "encode boom"
    # The failure happened during the snapshot, so the store was never written.
    assert async_store.init_called_count == 0


@pytest.mark.asyncio
async def test_close_closes_async_store():
    async_store = FakeAsyncFeatureStore()
    store = AsyncStore(Listeners(), Listeners())
    store.with_async_persistence(async_store, True, None)

    await store.close()
    assert async_store.closed is True


@pytest.mark.asyncio
async def test_close_logs_and_swallows_store_error(caplog):
    async_store = FakeAsyncFeatureStore()
    store = AsyncStore(Listeners(), Listeners())
    store.with_async_persistence(async_store, True, None)

    with patch.object(async_store, "close", side_effect=RuntimeError("close boom")):
        # A close error is logged and swallowed, never raised.
        await store.close()

    assert any(
        "Error closing the persistent store" in record.message
        for record in caplog.records
        if record.levelname == "WARNING"
    )


class FakeAsyncCore(AsyncFeatureStoreCore):
    """An async core whose data and initialized flag can be set out of band.

    Setting ``inited`` and ``data`` directly, without going through ``init``,
    models another process populating the store. ``query_count`` records how
    often ``initialized_internal`` runs so caching behavior can be asserted.
    """

    def __init__(self):
        self.data: Dict[VersionedDataKind, Dict[str, dict]] = {FEATURES: {}, SEGMENTS: {}}
        self.inited = False
        self.query_count = 0

    async def init_internal(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        self.data = {FEATURES: dict(all_data.get(FEATURES, {})), SEGMENTS: dict(all_data.get(SEGMENTS, {}))}
        self.inited = True

    async def get_internal(self, kind: VersionedDataKind, key: str) -> Optional[dict]:
        return self.data.get(kind, {}).get(key)

    async def get_all_internal(self, kind: VersionedDataKind) -> Mapping[str, dict]:
        return dict(self.data.get(kind, {}))

    async def upsert_internal(self, kind: VersionedDataKind, item: dict) -> dict:
        self.data[kind][item["key"]] = item
        return item

    async def initialized_internal(self) -> bool:
        self.query_count += 1
        return self.inited


@pytest.mark.asyncio
async def test_wrapper_is_initialized_reflects_external_init_and_latches():
    core = FakeAsyncCore()
    wrapper = AsyncCachingStoreWrapper(core, CacheConfig.disabled())

    assert await wrapper.is_initialized() is False
    assert wrapper.initialized is False

    # Another process initializes the store.
    core.inited = True
    assert await wrapper.is_initialized() is True
    assert wrapper.initialized is True

    # The state has latched, so a later loss of the init key does not flip it back.
    core.inited = False
    assert await wrapper.is_initialized() is True


@pytest.mark.asyncio
async def test_wrapper_is_initialized_queries_every_call_when_cache_off():
    core = FakeAsyncCore()
    wrapper = AsyncCachingStoreWrapper(core, CacheConfig.disabled())

    assert await wrapper.is_initialized() is False
    assert await wrapper.is_initialized() is False
    assert core.query_count == 2

    core.inited = True
    assert await wrapper.is_initialized() is True
    assert core.query_count == 3

    # Latched: no more queries.
    assert await wrapper.is_initialized() is True
    assert core.query_count == 3


@pytest.mark.asyncio
async def test_wrapper_is_initialized_infinite_cache_never_reflects_later_init():
    core = FakeAsyncCore()
    wrapper = AsyncCachingStoreWrapper(core, CacheConfig(expiration=sys.maxsize))

    assert await wrapper.is_initialized() is False
    assert core.query_count == 1

    # The False result is cached forever, so a later init is not observed.
    core.inited = True
    assert await wrapper.is_initialized() is False
    assert core.query_count == 1


@pytest.mark.asyncio
async def test_store_is_ready_gates_reads_and_stops_once_memory_active():
    core = FakeAsyncCore()
    wrapper = AsyncCachingStoreWrapper(core, CacheConfig.disabled())
    store = AsyncStore(Listeners(), Listeners())
    store.with_async_persistence(wrapper, False, None)

    assert await store.is_ready() is False

    # Another process initializes the store; the gate then reports ready.
    core.inited = True
    assert await store.is_ready() is True

    # Once a basis arrives the memory store is active; the persistent store is no
    # longer queried.
    await store.apply(_full_changeset("flag-a", 1, True), True)
    assert store.get_active_store() is store._memory_store
    queries_before = core.query_count
    assert await store.is_ready() is True
    assert core.query_count == queries_before


@pytest.mark.asyncio
async def test_fdv2_gate_serves_store_with_data_source_after_external_init():
    core = FakeAsyncCore()
    core.data[FEATURES]["flag-a"] = _flag("flag-a", 1, True)
    wrapper = AsyncCachingStoreWrapper(core, CacheConfig.disabled())

    class _NeverBuiltSyncBuilder:
        pass

    ds_config = AsyncDataSystemConfig(
        synchronizers=[_NeverBuiltSyncBuilder()],  # type: ignore[list-item]
        data_store=wrapper,
        data_store_mode=DataStoreMode.READ_ONLY,
    )
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="fake", send_events=False), ds_config)

    # A synchronizer is configured but no basis has arrived, so before the store
    # reports initialized the gate withholds the store's data.
    assert await fdv2.data_availability() == DataAvailability.DEFAULTS

    # Another process initializes the store; the gate now serves its data, and the
    # store read agrees (no is_initialized-vs-evaluation divergence).
    core.inited = True
    assert await fdv2.data_availability() == DataAvailability.CACHED
    flag = await fdv2.store.get(FEATURES, "flag-a")
    assert flag is not None
    assert flag.key == "flag-a"


@pytest.mark.asyncio
async def test_user_store_missing_readiness_check_is_a_typed_error():
    """A user-supplied AsyncFeatureStore that omits the readiness check cannot be
    instantiated, so the gap surfaces as a typed error rather than silent DEFAULTS."""
    class StoreWithoutReadiness(AsyncFeatureStore):
        async def get(self, kind, key):
            return None

        async def all(self, kind):
            return {}

        async def init(self, all_data):
            pass

        async def upsert(self, kind, item):
            return True

        async def delete(self, kind, key, version):
            return True

        @property
        def initialized(self) -> bool:
            return True

    with pytest.raises(TypeError):
        StoreWithoutReadiness()  # type: ignore[abstract]


def test_sync_apply_still_persists_synchronously():
    """The default sync path is unchanged: it persists inline to a sync store."""
    from ldclient.testing.impl.datasystem.test_fdv2_persistence import (
        StubFeatureStore
    )

    sync_store = StubFeatureStore()
    store = Store(Listeners(), Listeners())
    store.with_persistence(sync_store, True, None)

    store.apply(_full_changeset("flag-a", 1, True), True)
    assert sync_store.init_called_count >= 1
    assert "flag-a" in sync_store.get_data_snapshot()[FEATURES]

    store.apply(_delta_changeset("flag-a", 2, False), True)
    assert any(call[1] == "flag-a" for call in sync_store.upsert_calls)
