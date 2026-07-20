"""
Integration tests for _AsyncRedisBigSegmentStore.

These tests require a real Redis instance running on localhost:6379.
They are skipped when the redis package is not installed or when the
LD_SKIP_DATABASE_TESTS environment variable is set to '1'.
"""

from os import environ
from typing import List

import pytest

from ldclient.interfaces import BigSegmentStoreMetadata
from ldclient.testing.test_util import skip_database_tests

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

# Skip marker applied to the whole module
pytestmark = pytest.mark.skipif(
    not have_async_redis,
    reason="skipping async Redis tests because redis package (>=4.6) is not installed"
)

DEFAULT_PREFIX = 'launchdarkly'
FAKE_USER_HASH = 'userhash'

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def sync_redis_client():
    """Return a synchronous Redis client for test setup/teardown."""
    import redis
    return redis.StrictRedis(host="localhost", port=6379, db=0)


def clear_data(prefix: str):
    r = sync_redis_client()
    pattern = "%s:*" % (prefix or DEFAULT_PREFIX)
    for key in r.keys(pattern):
        r.delete(key)


def set_metadata(prefix: str, metadata: BigSegmentStoreMetadata):
    from ldclient.impl.integrations.redis.async_redis_big_segment_store import (
        _AsyncRedisBigSegmentStore
    )
    r = sync_redis_client()
    key = (prefix or DEFAULT_PREFIX) + _AsyncRedisBigSegmentStore.KEY_LAST_UP_TO_DATE
    if metadata.last_up_to_date is None:
        r.set(key, "")
    else:
        r.set(key, str(metadata.last_up_to_date))


def set_segments(prefix: str, user_hash: str, includes: List[str], excludes: List[str]):
    from ldclient.impl.integrations.redis.async_redis_big_segment_store import (
        _AsyncRedisBigSegmentStore
    )
    r = sync_redis_client()
    pfx = prefix or DEFAULT_PREFIX
    for ref in includes:
        r.sadd(pfx + _AsyncRedisBigSegmentStore.KEY_USER_INCLUDE + user_hash, ref)
    for ref in excludes:
        r.sadd(pfx + _AsyncRedisBigSegmentStore.KEY_USER_EXCLUDE + user_hash, ref)


def make_store(prefix=None):
    from ldclient.integrations import Redis
    return Redis.async_big_segment_store(prefix=prefix)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(params=[None, 'testprefix'])
def prefix(request):
    return request.param


@pytest.fixture(autouse=True)
def clear_before_each(prefix):
    if skip_database_tests or not have_sync_redis:
        yield
        return
    try:
        clear_data(prefix)
    except Exception:
        pass
    yield
    try:
        clear_data(prefix)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
@pytest.mark.skipif(not have_sync_redis, reason="skipping: sync redis not available for test setup")
async def test_get_metadata_valid_value(prefix):
    expected_timestamp = 1234567890
    set_metadata(prefix, BigSegmentStoreMetadata(expected_timestamp))
    store = make_store(prefix)
    try:
        actual = await store.get_metadata()
        assert actual is not None
        assert actual.last_up_to_date == expected_timestamp
    finally:
        await store.stop()


@pytest.mark.asyncio
@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
@pytest.mark.skipif(not have_sync_redis, reason="skipping: sync redis not available for test setup")
async def test_get_metadata_no_value(prefix):
    store = make_store(prefix)
    try:
        actual = await store.get_metadata()
        assert actual is not None
        assert actual.last_up_to_date is None
    finally:
        await store.stop()


@pytest.mark.asyncio
@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
@pytest.mark.skipif(not have_sync_redis, reason="skipping: sync redis not available for test setup")
async def test_get_membership_not_found(prefix):
    store = make_store(prefix)
    try:
        membership = await store.get_membership(FAKE_USER_HASH)
        assert membership is None
    finally:
        await store.stop()


@pytest.mark.asyncio
@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
@pytest.mark.skipif(not have_sync_redis, reason="skipping: sync redis not available for test setup")
async def test_get_membership_includes_only(prefix):
    set_segments(prefix, FAKE_USER_HASH, ['key1', 'key2'], [])
    store = make_store(prefix)
    try:
        membership = await store.get_membership(FAKE_USER_HASH)
        assert membership == {'key1': True, 'key2': True}
    finally:
        await store.stop()


@pytest.mark.asyncio
@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
@pytest.mark.skipif(not have_sync_redis, reason="skipping: sync redis not available for test setup")
async def test_get_membership_excludes_only(prefix):
    set_segments(prefix, FAKE_USER_HASH, [], ['key1', 'key2'])
    store = make_store(prefix)
    try:
        membership = await store.get_membership(FAKE_USER_HASH)
        assert membership == {'key1': False, 'key2': False}
    finally:
        await store.stop()


@pytest.mark.asyncio
@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
@pytest.mark.skipif(not have_sync_redis, reason="skipping: sync redis not available for test setup")
async def test_get_membership_includes_and_excludes(prefix):
    set_segments(prefix, FAKE_USER_HASH, ['key1', 'key2'], ['key2', 'key3'])
    store = make_store(prefix)
    try:
        membership = await store.get_membership(FAKE_USER_HASH)
        assert membership == {'key1': True, 'key2': True, 'key3': False}
    finally:
        await store.stop()


@pytest.mark.asyncio
@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
@pytest.mark.skipif(not have_sync_redis, reason="skipping: sync redis not available for test setup")
async def test_stop_closes_client(prefix):
    store = make_store(prefix)
    # stop() should not raise
    await store.stop()
