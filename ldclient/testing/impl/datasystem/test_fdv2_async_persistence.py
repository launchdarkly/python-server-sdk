# pylint: disable=missing-docstring

from typing import Any, Dict, List, Mapping, Optional

import pytest

from ldclient.impl.datasystem.async_store import AsyncStore
from ldclient.impl.datasystem.store import Store
from ldclient.impl.listeners import Listeners
from ldclient.interfaces import (
    AsyncFeatureStore,
    Change,
    ChangeSet,
    ChangeType,
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
async def test_close_closes_async_store():
    async_store = FakeAsyncFeatureStore()
    store = AsyncStore(Listeners(), Listeners())
    store.with_async_persistence(async_store, True, None)

    err = await store.close()
    assert err is None
    assert async_store.closed is True


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
