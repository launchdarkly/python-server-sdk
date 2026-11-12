"""
Integration tests for _AsyncRedisBigSegmentStore.

These tests require a real Redis instance running on localhost:6379.
They are skipped when the redis package is not installed or when the
LD_SKIP_DATABASE_TESTS environment variable is set to '1'.
"""

from typing import List

import pytest

from ldclient.impl.integrations.redis.async_redis_big_segment_store import (
    _AsyncRedisBigSegmentStore
)
from ldclient.integrations import Redis
from ldclient.interfaces import BigSegmentStoreMetadata
from ldclient.testhelpers.storetest import AsyncBigSegmentStoreTestSuite
from ldclient.testing.test_util import skip_database_tests

have_async_redis = False
try:
    import redis
    import redis.asyncio

    have_async_redis = True
except ImportError:
    pass

pytestmark = pytest.mark.skipif(
    not have_async_redis,
    reason="skipping async Redis tests because redis package (>=4.6) is not installed"
)


def sync_redis_client():
    """Return a synchronous Redis client for test setup."""
    return redis.StrictRedis(host="localhost", port=6379, db=0)


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
class TestAsyncRedisBigSegmentStore(AsyncBigSegmentStoreTestSuite):
    def create_store(self, prefix):
        return Redis.async_big_segment_store(prefix=prefix)

    async def clear_data(self, prefix):
        r = sync_redis_client()
        for key in r.keys("%s:*" % (prefix or Redis.DEFAULT_PREFIX)):
            r.delete(key)

    async def set_metadata(self, prefix, metadata: BigSegmentStoreMetadata):
        r = sync_redis_client()
        key = (prefix or Redis.DEFAULT_PREFIX) + _AsyncRedisBigSegmentStore.KEY_LAST_UP_TO_DATE
        r.set(key, "" if metadata.last_up_to_date is None else str(metadata.last_up_to_date))

    async def set_segments(self, prefix, context_hash: str, includes: List[str], excludes: List[str]):
        r = sync_redis_client()
        pfx = prefix or Redis.DEFAULT_PREFIX
        for ref in includes:
            r.sadd(pfx + _AsyncRedisBigSegmentStore.KEY_USER_INCLUDE + context_hash, ref)
        for ref in excludes:
            r.sadd(pfx + _AsyncRedisBigSegmentStore.KEY_USER_EXCLUDE + context_hash, ref)
