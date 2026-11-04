# pylint: disable=missing-docstring

from threading import Event

import pytest

from ldclient.async_config import AsyncConfig
from ldclient.async_feature_store import AsyncInMemoryFeatureStore
from ldclient.config import Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.impl.aio.concurrency import AsyncEvent
from ldclient.impl.datasystem.async_fdv1 import AsyncFDv1
from ldclient.impl.datasystem.fdv1 import FDv1


class _ClosingStore(InMemoryFeatureStore):
    def __init__(self):
        super().__init__()
        self.close_count = 0

    def close(self) -> None:
        self.close_count += 1


class _AsyncClosingStore(AsyncInMemoryFeatureStore):
    def __init__(self):
        super().__init__()
        self.close_count = 0

    async def close(self) -> None:
        self.close_count += 1


def test_stop_closes_the_feature_store():
    store = _ClosingStore()
    data_system = FDv1(Config(sdk_key="key", offline=True, feature_store=store))
    data_system.start(Event())

    data_system.stop()

    assert store.close_count == 1


def test_stop_closes_the_feature_store_when_not_started():
    store = _ClosingStore()
    data_system = FDv1(Config(sdk_key="key", offline=True, feature_store=store))

    data_system.stop()

    assert store.close_count == 1


@pytest.mark.asyncio
async def test_async_stop_closes_the_feature_store():
    store = _AsyncClosingStore()
    data_system = AsyncFDv1(AsyncConfig(sdk_key="key", offline=True), store, lambda: None)
    data_system.start(AsyncEvent())

    await data_system.stop()

    assert store.close_count == 1


@pytest.mark.asyncio
async def test_async_stop_closes_the_feature_store_when_not_started():
    store = _AsyncClosingStore()
    data_system = AsyncFDv1(AsyncConfig(sdk_key="key", offline=True), store, lambda: None)

    await data_system.stop()

    assert store.close_count == 1
