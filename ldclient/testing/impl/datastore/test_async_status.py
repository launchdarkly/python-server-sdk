# pylint: disable=missing-docstring

import asyncio
from typing import Any, Dict, List, Mapping, Optional

import pytest

from ldclient.impl.datastore.async_status import AsyncFeatureStoreClientWrapper
from ldclient.interfaces import AsyncFeatureStore, DataStoreStatus
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind


class FakeAsyncStore(AsyncFeatureStore):
    """An async store whose operations can be made to fail on demand."""

    def __init__(self):
        self._data: Dict[VersionedDataKind, Dict[str, dict]] = {FEATURES: {}, SEGMENTS: {}}
        self._inited = False
        self._available = True
        self.fail = False
        self.init_calls: List[Mapping] = []
        self.closed = False

    async def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        if self.fail:
            raise RuntimeError("store down")
        self.init_calls.append(all_data)
        self._data = {FEATURES: dict(all_data.get(FEATURES, {})), SEGMENTS: dict(all_data.get(SEGMENTS, {}))}
        self._inited = True

    async def get(self, kind: VersionedDataKind, key: str) -> Optional[Any]:
        if self.fail:
            raise RuntimeError("store down")
        return self._data.get(kind, {}).get(key)

    async def all(self, kind: VersionedDataKind) -> Dict[str, Any]:
        if self.fail:
            raise RuntimeError("store down")
        return dict(self._data.get(kind, {}))

    async def upsert(self, kind: VersionedDataKind, item: dict) -> bool:
        if self.fail:
            raise RuntimeError("store down")
        self._data[kind][item["key"]] = item
        return True

    async def delete(self, kind: VersionedDataKind, key: str, version: int) -> bool:
        return await self.upsert(kind, {"key": key, "version": version, "deleted": True})

    @property
    def initialized(self) -> bool:
        return self._inited

    async def is_available(self) -> bool:
        return self._available

    async def close(self) -> None:
        self.closed = True


class StoreWithoutAvailability(AsyncFeatureStore):
    async def init(self, all_data):
        pass

    async def get(self, kind, key):
        return None

    async def all(self, kind):
        return {}

    async def upsert(self, kind, item):
        return True

    async def delete(self, kind, key, version):
        return True

    @property
    def initialized(self) -> bool:
        return True


@pytest.mark.asyncio
async def test_is_monitoring_enabled_true_when_store_has_is_available():
    wrapper = AsyncFeatureStoreClientWrapper(FakeAsyncStore(), lambda _s: None)
    assert wrapper.is_monitoring_enabled() is True


@pytest.mark.asyncio
async def test_is_monitoring_enabled_false_without_is_available():
    wrapper = AsyncFeatureStoreClientWrapper(StoreWithoutAvailability(), lambda _s: None)
    assert wrapper.is_monitoring_enabled() is False


@pytest.mark.asyncio
async def test_init_sorts_and_delegates():
    store = FakeAsyncStore()
    wrapper = AsyncFeatureStoreClientWrapper(store, lambda _s: None)
    await wrapper.init({FEATURES: {}, SEGMENTS: {}})
    assert len(store.init_calls) == 1
    assert wrapper.initialized is True


@pytest.mark.asyncio
async def test_failure_marks_unavailable_polls_and_recovers():
    store = FakeAsyncStore()
    statuses: List[DataStoreStatus] = []
    wrapper = AsyncFeatureStoreClientWrapper(store, lambda s: statuses.append(s))

    # Make the next operation fail.
    store.fail = True
    store._available = False

    with pytest.raises(RuntimeError):
        await wrapper.get(FEATURES, "flag-a")

    # The wrapper reported unavailability and started a poller.
    assert len(statuses) == 1
    assert statuses[0].available is False

    # Bring the store back; the poller (0.5s interval) should notice and recover.
    store.fail = False
    store._available = True

    for _ in range(40):
        await asyncio.sleep(0.05)
        if len(statuses) >= 2:
            break

    assert len(statuses) == 2
    assert statuses[1].available is True

    await wrapper.close()


@pytest.mark.asyncio
async def test_close_stops_poller_and_closes_inner():
    store = FakeAsyncStore()
    statuses: List[DataStoreStatus] = []
    wrapper = AsyncFeatureStoreClientWrapper(store, lambda s: statuses.append(s))

    # Trigger an outage so a poller is running.
    store.fail = True
    store._available = False
    with pytest.raises(RuntimeError):
        await wrapper.all(FEATURES)

    assert wrapper._poller is not None

    await wrapper.close()

    assert wrapper._poller is None
    assert store.closed is True


@pytest.mark.asyncio
async def test_successful_ops_pass_through():
    store = FakeAsyncStore()
    wrapper = AsyncFeatureStoreClientWrapper(store, lambda _s: None)

    await wrapper.upsert(FEATURES, {"key": "flag-a", "version": 1})
    got = await wrapper.get(FEATURES, "flag-a")
    assert got is not None and got["key"] == "flag-a"
    allf = await wrapper.all(FEATURES)
    assert "flag-a" in allf
