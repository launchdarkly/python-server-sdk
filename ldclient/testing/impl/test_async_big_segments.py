"""
Unit tests for AsyncBigSegmentStoreManager.

These tests use a mock AsyncBigSegmentStore and require no real Redis connection.
"""

import asyncio
import time

import pytest

from ldclient.async_config import AsyncBigSegmentsConfig
from ldclient.evaluation import BigSegmentsStatus
from ldclient.impl.async_big_segments import (
    AsyncBigSegmentStoreManager,
    _hash_for_user_key
)
from ldclient.interfaces import AsyncBigSegmentStore, BigSegmentStoreMetadata

# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------

user_key = 'user-key'
user_hash = _hash_for_user_key(user_key)


class MockAsyncBigSegmentStore(AsyncBigSegmentStore):
    """In-process async mock that records calls and allows controlled responses."""

    def __init__(self):
        self._membership_queries = []
        self._memberships = {}
        self._metadata_fn = lambda: BigSegmentStoreMetadata(int(time.time() * 1000))
        self._stopped = False

    def setup_membership(self, user_hash: str, membership):
        self._memberships[user_hash] = membership

    def setup_metadata_always_up_to_date(self):
        self._metadata_fn = lambda: BigSegmentStoreMetadata(int(time.time() * 1000))

    def setup_metadata_always_stale(self):
        self._metadata_fn = lambda: BigSegmentStoreMetadata(0)

    def setup_metadata_none(self):
        self._metadata_fn = lambda: None

    def setup_metadata_error(self):
        def _raise():
            raise Exception("deliberate metadata error")
        self._metadata_fn = _raise

    async def get_metadata(self) -> BigSegmentStoreMetadata:
        return self._metadata_fn()

    async def get_membership(self, context_hash: str):
        self._membership_queries.append(context_hash)
        return self._memberships.get(context_hash, None)

    async def stop(self):
        self._stopped = True

    @property
    def membership_queries(self):
        return list(self._membership_queries)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def make_started_manager(store, **kwargs):
    config = AsyncBigSegmentsConfig(store=store, **kwargs)
    # The constructor starts the polling task (it requires a running event loop).
    return AsyncBigSegmentStoreManager(config)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_membership_query_uncached_result_healthy_status():
    store = MockAsyncBigSegmentStore()
    store.setup_metadata_always_up_to_date()
    expected_membership = {"key1": True, "key2": False}
    store.setup_membership(user_hash, expected_membership)

    manager = await make_started_manager(store)
    try:
        membership, status = await manager.get_user_membership(user_key)
        assert membership == expected_membership
        assert status == BigSegmentsStatus.HEALTHY
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_membership_query_cached_result_healthy_status():
    store = MockAsyncBigSegmentStore()
    store.setup_metadata_always_up_to_date()
    expected_membership = {"key1": True, "key2": False}
    store.setup_membership(user_hash, expected_membership)

    manager = await make_started_manager(store)
    try:
        result1 = await manager.get_user_membership(user_key)
        result2 = await manager.get_user_membership(user_key)
        assert result1 == result2
    finally:
        await manager.stop()

    # Only one query to the store despite two calls — cache hit on second call
    assert store.membership_queries == [user_hash]


@pytest.mark.asyncio
async def test_membership_query_can_cache_result_of_none():
    store = MockAsyncBigSegmentStore()
    store.setup_metadata_always_up_to_date()
    store.setup_membership(user_hash, None)  # store returns None → EMPTY_MEMBERSHIP cached

    manager = await make_started_manager(store)
    try:
        membership1, status1 = await manager.get_user_membership(user_key)
        membership2, status2 = await manager.get_user_membership(user_key)
        # None from store becomes EMPTY_MEMBERSHIP ({})
        assert membership1 == {}
        assert membership2 == {}
        assert status1 == BigSegmentsStatus.HEALTHY
        assert status2 == BigSegmentsStatus.HEALTHY
    finally:
        await manager.stop()

    assert store.membership_queries == [user_hash]  # only 1 query, cache hit on second


@pytest.mark.asyncio
async def test_membership_query_stale_status():
    store = MockAsyncBigSegmentStore()
    store.setup_metadata_always_stale()
    expected_membership = {"key1": True, "key2": False}
    store.setup_membership(user_hash, expected_membership)

    manager = await make_started_manager(store)
    try:
        membership, status = await manager.get_user_membership(user_key)
        assert membership == expected_membership
        assert status == BigSegmentsStatus.STALE
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_membership_query_stale_status_no_store_metadata():
    store = MockAsyncBigSegmentStore()
    store.setup_metadata_none()
    expected_membership = {"key1": True, "key2": False}
    store.setup_membership(user_hash, expected_membership)

    manager = await make_started_manager(store)
    try:
        membership, status = await manager.get_user_membership(user_key)
        assert membership == expected_membership
        assert status == BigSegmentsStatus.STALE
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_membership_query_store_error_returns_store_error_status():
    store = MockAsyncBigSegmentStore()
    store.setup_metadata_always_up_to_date()
    # No membership set up — store will raise on get_membership via a broken store
    broken_store = MockAsyncBigSegmentStore()
    broken_store.setup_metadata_always_up_to_date()

    async def _raise_on_membership(context_hash):
        raise Exception("deliberate membership error")

    broken_store.get_membership = _raise_on_membership

    manager = await make_started_manager(broken_store)
    try:
        membership, status = await manager.get_user_membership(user_key)
        assert membership is None
        assert status == BigSegmentsStatus.STORE_ERROR
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_no_store_returns_not_configured():
    config = AsyncBigSegmentsConfig(store=None)
    manager = AsyncBigSegmentStoreManager(config)
    try:
        membership, status = await manager.get_user_membership(user_key)
        assert membership is None
        assert status == BigSegmentsStatus.NOT_CONFIGURED
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_first_call_before_poll_task_triggers_inline_poll():
    """
    When get_user_membership is called before the background polling task has run,
    poll_store_and_update_status() must be called inline so that a valid status is returned
    rather than deterministically returning STORE_ERROR.
    """
    store = MockAsyncBigSegmentStore()
    store.setup_metadata_always_up_to_date()
    store.setup_membership(user_hash, {"seg1": True})

    # Query immediately after construction, racing the just-started poll task;
    # whichever runs first, a valid status must be returned.
    config = AsyncBigSegmentsConfig(store=store)
    manager = AsyncBigSegmentStoreManager(config)
    try:
        membership, status = await manager.get_user_membership(user_key)
        # Should have done an inline poll and returned HEALTHY, not STORE_ERROR
        assert status == BigSegmentsStatus.HEALTHY
        assert membership == {"seg1": True}
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_concurrent_calls_for_same_key_all_return_correct_result():
    """
    Multiple concurrent coroutines missing the cache for the same key may each fetch from
    the store independently, but all must return the same correct result. Duplicate fetches
    are harmless because they return identical data and both populate the cache.
    """
    original_store = MockAsyncBigSegmentStore()
    original_store.setup_metadata_always_up_to_date()
    original_store.setup_membership(user_hash, {"seg1": True})

    class SlowStore(MockAsyncBigSegmentStore):
        async def get_membership(self, context_hash):
            await asyncio.sleep(0.05)  # introduce latency to make concurrency visible
            return await super().get_membership(context_hash)

    slow_store = SlowStore()
    slow_store.setup_metadata_always_up_to_date()
    slow_store.setup_membership(user_hash, {"seg1": True})

    manager = await make_started_manager(slow_store)
    try:
        # Fire 5 concurrent requests for the same key before any has populated the cache
        results = await asyncio.gather(*[manager.get_user_membership(user_key) for _ in range(5)])
    finally:
        await manager.stop()

    # All results should be identical and correct
    assert all(r == results[0] for r in results)
    assert results[0] == ({"seg1": True}, BigSegmentsStatus.HEALTHY)


@pytest.mark.asyncio
async def test_status_reflects_store_unavailable():
    store = MockAsyncBigSegmentStore()
    store.setup_metadata_error()

    manager = await make_started_manager(store)
    try:
        # Force a poll
        status = await manager.poll_store_and_update_status()
        assert status.available is False
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_status_reflects_stale():
    store = MockAsyncBigSegmentStore()
    store.setup_metadata_always_stale()

    manager = await make_started_manager(store)
    try:
        status = await manager.poll_store_and_update_status()
        assert status.available is True
        assert status.stale is True
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_status_reflects_healthy():
    store = MockAsyncBigSegmentStore()
    store.setup_metadata_always_up_to_date()

    manager = await make_started_manager(store)
    try:
        status = await manager.poll_store_and_update_status()
        assert status.available is True
        assert status.stale is False
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_stop_stops_store():
    store = MockAsyncBigSegmentStore()
    store.setup_metadata_always_up_to_date()

    manager = await make_started_manager(store)
    await manager.stop()

    assert store._stopped is True


@pytest.mark.asyncio
async def test_status_provider_status_is_synchronous():
    """BigSegmentStoreStatusProvider.status must be readable synchronously without an await."""
    store = MockAsyncBigSegmentStore()
    store.setup_metadata_always_up_to_date()

    manager = await make_started_manager(store)
    try:
        # Poll once to populate __last_status
        await manager.poll_store_and_update_status()
        # status property is sync — calling it should not raise
        status = manager.status_provider.status
        assert status.available is True
    finally:
        await manager.stop()
