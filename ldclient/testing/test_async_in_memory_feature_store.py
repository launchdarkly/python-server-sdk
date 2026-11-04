import pytest

from ldclient.async_feature_store import AsyncInMemoryFeatureStore
from ldclient.interfaces import AsyncFeatureStore
from ldclient.testing.async_feature_store_test_base import (
    AsyncFeatureStoreTestBase,
    AsyncFeatureStoreTester
)


@pytest.mark.asyncio
async def test_async_in_memory_status_checks():
    store = AsyncInMemoryFeatureStore()

    assert not store.is_monitoring_enabled()
    assert await store.is_available()


class AsyncInMemoryFeatureStoreTester(AsyncFeatureStoreTester):
    async def create_feature_store(self) -> AsyncFeatureStore:
        return AsyncInMemoryFeatureStore()


class TestAsyncInMemoryFeatureStore(AsyncFeatureStoreTestBase):
    @pytest.fixture
    def tester(self):
        return AsyncInMemoryFeatureStoreTester()
