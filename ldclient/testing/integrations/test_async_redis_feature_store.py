"""
Integration tests for the async Redis feature store (_AsyncRedisFeatureStoreCore wrapped by
AsyncCachingStoreWrapper).

These tests require a real Redis instance running on localhost:6379. They are skipped when the
redis package is not installed or when the LD_SKIP_DATABASE_TESTS environment variable is set to
'1'. The caching-wrapper logic itself is covered without Redis in
ldclient.testing.test_async_feature_store_helpers.
"""

import json

import pytest

from ldclient.async_feature_store_helpers import AsyncCachingStoreWrapper
from ldclient.feature_store import CacheConfig
from ldclient.integrations import Redis
from ldclient.interfaces import AsyncFeatureStore
from ldclient.testing.async_feature_store_test_base import (
    AsyncFeatureStoreTestBase,
    AsyncFeatureStoreTester
)
from ldclient.testing.test_util import skip_database_tests
from ldclient.versioned_data_kind import FEATURES

have_async_redis = False
try:
    import redis.asyncio as aioredis

    have_async_redis = True
except ImportError:
    pass

try:
    import redis as _sync_redis

    have_sync_redis = True
except ImportError:
    have_sync_redis = False

pytestmark = pytest.mark.skipif(
    not have_async_redis,
    reason="skipping async Redis tests because redis package is not installed"
)

DEFAULT_PREFIX = 'launchdarkly'


def sync_redis_client():
    """Return a synchronous Redis client for test setup and teardown."""
    import redis
    return redis.StrictRedis(host="localhost", port=6379, db=0)


def clear_data(prefix):
    r = sync_redis_client()
    for key in r.keys("%s:*" % (prefix or DEFAULT_PREFIX)):
        r.delete(key)


class AsyncRedisFeatureStoreTester(AsyncFeatureStoreTester):
    def __init__(self, prefix=None, caching=None):
        self.prefix = prefix
        self.caching = caching if caching is not None else CacheConfig.disabled()

    async def create_feature_store(self) -> AsyncFeatureStore:
        return Redis.async_feature_store(prefix=self.prefix, caching=self.caching)


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
class TestAsyncRedisFeatureStore(AsyncFeatureStoreTestBase):
    @pytest.fixture(params=[(False, False), (True, False), (False, True), (True, True)])
    def tester(self, request):
        specify_prefix, use_caching = request.param
        prefix = "testprefix" if specify_prefix else None
        caching = CacheConfig.default() if use_caching else CacheConfig.disabled()
        return AsyncRedisFeatureStoreTester(prefix, caching)

    @pytest.fixture(autouse=True)
    def clear_data_before_each(self, tester):
        clear_data(tester.prefix)


@pytest.mark.asyncio
@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
@pytest.mark.skipif(not have_sync_redis, reason="skipping: sync redis not available for test setup")
async def test_stores_with_different_prefixes_are_independent():
    clear_data("a")
    clear_data("b")

    flag_a1 = {'key': 'flagA1', 'version': 1}
    flag_a2 = {'key': 'flagA2', 'version': 1}
    flag_b1 = {'key': 'flagB1', 'version': 1}
    flag_b2 = {'key': 'flagB2', 'version': 1}

    store_a = Redis.async_feature_store(prefix="a")
    store_b = Redis.async_feature_store(prefix="b")
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
@pytest.mark.skipif(not have_sync_redis, reason="skipping: sync redis not available for test setup")
async def test_upsert_race_condition_against_external_client_with_higher_version():
    other_client = sync_redis_client()
    store = Redis.async_feature_store()
    try:
        await store.init({FEATURES: {}})

        other_version = {'key': 'flagkey', 'version': 2}

        def hook(base_key, key):
            if other_version['version'] <= 4:
                other_client.hset(base_key, key, json.dumps(other_version))
                other_version['version'] = other_version['version'] + 1

        store._core.test_update_hook = hook

        await store.upsert(FEATURES, {'key': 'flagkey', 'version': 1})
        result = await store.get(FEATURES, 'flagkey')
        assert result['version'] == 2
    finally:
        await store.close()


@pytest.mark.asyncio
@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
@pytest.mark.skipif(not have_sync_redis, reason="skipping: sync redis not available for test setup")
async def test_upsert_race_condition_against_external_client_with_lower_version():
    other_client = sync_redis_client()
    store = Redis.async_feature_store()
    try:
        await store.init({FEATURES: {}})

        other_version = {'key': 'flagkey', 'version': 2}

        def hook(base_key, key):
            if other_version['version'] <= 4:
                other_client.hset(base_key, key, json.dumps(other_version))
                other_version['version'] = other_version['version'] + 1

        store._core.test_update_hook = hook

        await store.upsert(FEATURES, {'key': 'flagkey', 'version': 5})
        result = await store.get(FEATURES, 'flagkey')
        assert result['version'] == 5
    finally:
        await store.close()


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
