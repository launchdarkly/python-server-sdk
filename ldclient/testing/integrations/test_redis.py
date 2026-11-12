from typing import List

import pytest

from ldclient.impl.integrations.redis.redis_big_segment_store import (
    _RedisBigSegmentStore
)
from ldclient.impl.integrations.redis.redis_feature_store import (
    _RedisFeatureStoreCore
)
from ldclient.integrations import Redis
from ldclient.interfaces import BigSegmentStoreMetadata
from ldclient.testhelpers.storetest import (
    BigSegmentStoreTestSuite,
    FeatureStoreCoreTestSuite
)
from ldclient.testing.test_util import skip_database_tests

have_redis = False
try:
    import redis

    have_redis = True
except ImportError:
    pass

pytestmark = pytest.mark.skipif(not have_redis, reason="skipping Redis tests because redis module is not installed")

# Nothing listens on this port, so each operation fails.
_UNREACHABLE_URL = 'redis://localhost:1/0'


class RedisTestHelper:
    @staticmethod
    def make_client() -> redis.StrictRedis:
        return redis.StrictRedis(host="localhost", port=6379, db=0)

    @staticmethod
    def clear_data_for_prefix(prefix):
        r = RedisTestHelper.make_client()
        for key in r.keys("%s:*" % (prefix or Redis.DEFAULT_PREFIX)):
            r.delete(key)


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
class TestRedisFeatureStoreCore(FeatureStoreCoreTestSuite):
    def create_core(self, prefix):
        return _RedisFeatureStoreCore(Redis.DEFAULT_URL, prefix, {})

    def clear_data(self, prefix):
        RedisTestHelper.clear_data_for_prefix(prefix)

    def create_error_core(self):
        return _RedisFeatureStoreCore(_UNREACHABLE_URL, None, {})

    def set_concurrent_modification_hook(self, core, hook):
        core.test_update_hook = lambda base_key, key: hook()


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
class TestRedisBigSegmentStore(BigSegmentStoreTestSuite):
    def create_store(self, prefix):
        return Redis.new_big_segment_store(prefix=prefix)

    def clear_data(self, prefix):
        RedisTestHelper.clear_data_for_prefix(prefix)

    def set_metadata(self, prefix, metadata: BigSegmentStoreMetadata):
        r = RedisTestHelper.make_client()
        r.set((prefix or Redis.DEFAULT_PREFIX) + _RedisBigSegmentStore.KEY_LAST_UP_TO_DATE, "" if metadata.last_up_to_date is None else str(metadata.last_up_to_date))

    def set_segments(self, prefix, context_hash: str, includes: List[str], excludes: List[str]):
        r = RedisTestHelper.make_client()
        prefix = prefix or Redis.DEFAULT_PREFIX
        for ref in includes:
            r.sadd(prefix + _RedisBigSegmentStore.KEY_USER_INCLUDE + context_hash, ref)
        for ref in excludes:
            r.sadd(prefix + _RedisBigSegmentStore.KEY_USER_EXCLUDE + context_hash, ref)


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
def test_feature_store_max_connections_is_not_used():
    """Test that the max_connections parameter is NOT passed to the Redis connection pool."""
    custom_max_connections = 42
    store = Redis.new_feature_store(max_connections=custom_max_connections)

    # Access the connection pool through the wrapper's core
    actual_max_connections = store._core._pool.max_connections

    # Should NOT be our custom value since the parameter is unused
    assert actual_max_connections != custom_max_connections, \
        f"Expected max_connections to NOT be {custom_max_connections}, but it was set"


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
def test_big_segment_store_max_connections_is_not_used():
    """Test that the max_connections parameter is NOT passed to the Redis connection pool."""
    custom_max_connections = 42
    store = Redis.new_big_segment_store(max_connections=custom_max_connections)

    # Access the connection pool directly from the store
    actual_max_connections = store._pool.max_connections

    # Should NOT be our custom value since the parameter is unused
    assert actual_max_connections != custom_max_connections, \
        f"Expected max_connections to NOT be {custom_max_connections}, but it was set"


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
def test_feature_store_max_connections_warns_when_non_default(caplog):
    """Test that a warning is logged when max_connections differs from the default."""
    import logging
    caplog.set_level(logging.WARNING)

    custom_max_connections = 42
    Redis.new_feature_store(max_connections=custom_max_connections)

    assert any("max_connections parameter is not used" in record.message for record in caplog.records), \
        "Expected warning that parameter is not used"
    assert any("redis_opts" in record.message for record in caplog.records), \
        "Expected warning to mention redis_opts"


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
def test_big_segment_store_max_connections_warns_when_non_default(caplog):
    """Test that a warning is logged when max_connections differs from the default."""
    import logging
    caplog.set_level(logging.WARNING)

    custom_max_connections = 42
    Redis.new_big_segment_store(max_connections=custom_max_connections)

    assert any("max_connections parameter is not used" in record.message for record in caplog.records), \
        "Expected warning that parameter is not used"
    assert any("redis_opts" in record.message for record in caplog.records), \
        "Expected warning to mention redis_opts"


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
def test_feature_store_max_connections_no_warn_when_default(caplog):
    """Test that no warning is logged when max_connections is the default value."""
    import logging
    caplog.set_level(logging.WARNING)

    Redis.new_feature_store(max_connections=Redis.DEFAULT_MAX_CONNECTIONS)

    assert not any("max_connections parameter is not used" in record.message for record in caplog.records), \
        "Expected no warning when using default value"


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
def test_big_segment_store_max_connections_no_warn_when_default(caplog):
    """Test that no warning is logged when max_connections is the default value."""
    import logging
    caplog.set_level(logging.WARNING)

    Redis.new_big_segment_store(max_connections=Redis.DEFAULT_MAX_CONNECTIONS)

    assert not any("max_connections parameter is not used" in record.message for record in caplog.records), \
        "Expected no warning when using default value"
