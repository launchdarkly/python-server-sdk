"""
Integration tests for the async Consul feature store (_AsyncConsulFeatureStoreCore wrapped by
AsyncCachingStoreWrapper).

These tests require a real Consul instance running on localhost:8500. They are skipped when the
py-consul package is not installed or when the LD_SKIP_DATABASE_TESTS environment variable is set
to '1'. The caching-wrapper logic itself is covered without Consul in
ldclient.testing.test_async_feature_store_helpers.
"""

import pytest

from ldclient.async_feature_store_helpers import AsyncCachingStoreWrapper
from ldclient.feature_store import CacheConfig
from ldclient.integrations import Consul
from ldclient.interfaces import AsyncFeatureStore
from ldclient.testing.async_feature_store_test_base import (
    AsyncFeatureStoreTestBase,
    AsyncFeatureStoreTester
)
from ldclient.testing.test_util import skip_database_tests
from ldclient.versioned_data_kind import FEATURES

have_async_consul = False
try:
    import consul.aio

    have_async_consul = True
except ImportError:
    pass

pytestmark = pytest.mark.skipif(
    not have_async_consul,
    reason="skipping async Consul tests because py-consul package is not installed"
)

DEFAULT_PREFIX = 'launchdarkly'


def clear_data(prefix):
    # A synchronous client is enough for test setup and teardown.
    client = consul.Consul()
    index, keys = client.kv.get((prefix or DEFAULT_PREFIX) + "/", recurse=True, keys=True)
    for key in keys or []:
        client.kv.delete(key)


class AsyncConsulFeatureStoreTester(AsyncFeatureStoreTester):
    def __init__(self, prefix=None, caching=None):
        self.prefix = prefix
        self.caching = caching if caching is not None else CacheConfig.disabled()

    async def create_feature_store(self) -> AsyncFeatureStore:
        return Consul.async_feature_store(prefix=self.prefix, caching=self.caching)


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
class TestAsyncConsulFeatureStore(AsyncFeatureStoreTestBase):
    @pytest.fixture(params=[(False, False), (True, False), (False, True), (True, True)])
    def tester(self, request):
        specify_prefix, use_caching = request.param
        prefix = "testprefix" if specify_prefix else None
        caching = CacheConfig.default() if use_caching else CacheConfig.disabled()
        return AsyncConsulFeatureStoreTester(prefix, caching)

    @pytest.fixture(autouse=True)
    def clear_data_before_each(self, tester):
        clear_data(tester.prefix)


@pytest.mark.asyncio
@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
async def test_stores_with_different_prefixes_are_independent():
    clear_data("a")
    clear_data("b")

    flag_a1 = {'key': 'flagA1', 'version': 1}
    flag_a2 = {'key': 'flagA2', 'version': 1}
    flag_b1 = {'key': 'flagB1', 'version': 1}
    flag_b2 = {'key': 'flagB2', 'version': 1}

    store_a = Consul.async_feature_store(prefix="a")
    store_b = Consul.async_feature_store(prefix="b")
    try:
        await store_a.init({FEATURES: {'flagA1': flag_a1}})
        await store_a.upsert(FEATURES, flag_a2)

        await store_b.init({FEATURES: {'flagB1': flag_b1}})
        await store_b.upsert(FEATURES, flag_b2)

        assert await store_a.get(FEATURES, 'flagA1') == FEATURES.decode(flag_a1)
        assert await store_a.get(FEATURES, 'flagB1') is None
        assert await store_a.all(FEATURES) == {'flagA1': FEATURES.decode(flag_a1), 'flagA2': FEATURES.decode(flag_a2)}

        assert await store_b.get(FEATURES, 'flagB1') == FEATURES.decode(flag_b1)
        assert await store_b.get(FEATURES, 'flagA1') is None
        assert await store_b.all(FEATURES) == {'flagB1': FEATURES.decode(flag_b1), 'flagB2': FEATURES.decode(flag_b2)}
    finally:
        await store_a.close()
        await store_b.close()


@pytest.mark.asyncio
@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
async def test_available_and_monitoring():
    store = Consul.async_feature_store()
    try:
        assert store.is_monitoring_enabled() is True
        assert await store.is_available() is True
    finally:
        await store.close()


def test_async_feature_store_is_caching_wrapper():
    store = Consul.async_feature_store()
    assert isinstance(store, AsyncCachingStoreWrapper)


# Consul does not support Big Segments.
