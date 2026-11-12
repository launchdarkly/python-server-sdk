"""
Integration tests for the async Redis feature store core.

These tests require a real Redis instance running on localhost:6379. They are skipped when the
redis package is not installed or when the LD_SKIP_DATABASE_TESTS environment variable is set to
'1'. The caching-wrapper logic itself is covered without Redis in
ldclient.testing.test_async_feature_store_helpers.
"""

import pytest

from ldclient.async_feature_store_helpers import AsyncCachingStoreWrapper
from ldclient.integrations import Redis
from ldclient.testhelpers.storetest import AsyncFeatureStoreCoreTestSuite
from ldclient.testing.test_util import skip_database_tests

have_async_redis = False
try:
    import redis
    import redis.asyncio

    from ldclient.impl.integrations.redis.async_redis_feature_store import (
        _AsyncRedisFeatureStoreCore
    )

    have_async_redis = True
except ImportError:
    pass

pytestmark = pytest.mark.skipif(
    not have_async_redis,
    reason="skipping async Redis tests because redis package is not installed"
)

# Nothing listens on this port, so each operation fails.
_UNREACHABLE_URL = 'redis://localhost:1/0'


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
class TestAsyncRedisFeatureStoreCore(AsyncFeatureStoreCoreTestSuite):
    def create_core(self, prefix):
        return _AsyncRedisFeatureStoreCore(Redis.DEFAULT_URL, prefix, {})

    async def clear_data(self, prefix):
        r = redis.StrictRedis(host="localhost", port=6379, db=0)
        for key in r.keys("%s:*" % (prefix or Redis.DEFAULT_PREFIX)):
            r.delete(key)

    def create_error_core(self):
        return _AsyncRedisFeatureStoreCore(_UNREACHABLE_URL, None, {})

    def set_concurrent_modification_hook(self, core, hook):
        async def call_hook(base_key, key):
            await hook()

        core.test_update_hook = call_hook


@pytest.mark.asyncio
@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
async def test_available_and_monitoring():
    store = Redis.async_feature_store()
    try:
        assert store.is_monitoring_enabled() is True
        assert await store.is_available() is True
    finally:
        await store.close()


def test_async_feature_store_is_caching_wrapper():
    store = Redis.async_feature_store()
    assert isinstance(store, AsyncCachingStoreWrapper)
