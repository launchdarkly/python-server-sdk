import time
from queue import Queue

from ldclient.config import BigSegmentsConfig
from ldclient.evaluation import BigSegmentsStatus
from ldclient.impl.big_segments import (BigSegmentStoreManager,
                                        _hash_for_user_key)
from ldclient.interfaces import BigSegmentStoreMetadata
from ldclient.testing.mock_components import MockBigSegmentStore

user_key = 'user-key'
user_hash = _hash_for_user_key(user_key)


def test_membership_query_uncached_result_healthy_status():
    expected_membership = {"key1": True, "key2": False}
    store = MockBigSegmentStore()
    store.setup_metadata_always_up_to_date()
    store.setup_membership(user_hash, expected_membership)
    manager = BigSegmentStoreManager(BigSegmentsConfig(store=store))
    try:
        expected_result = (expected_membership, BigSegmentsStatus.HEALTHY)
        assert manager.get_user_membership(user_key) == expected_result
    finally:
        manager.stop()


def test_membership_query_cached_result_healthy_status():
    expected_membership = {"key1": True, "key2": False}
    store = MockBigSegmentStore()
    store.setup_metadata_always_up_to_date()
    store.setup_membership(user_hash, expected_membership)
    manager = BigSegmentStoreManager(BigSegmentsConfig(store=store))
    try:
        expected_result = (expected_membership, BigSegmentsStatus.HEALTHY)
        assert manager.get_user_membership(user_key) == expected_result
        assert manager.get_user_membership(user_key) == expected_result
    finally:
        manager.stop()
    assert store.membership_queries == [user_hash]  # only 1 query done rather than 2, due to caching


def test_membership_query_can_cache_result_of_none():
    store = MockBigSegmentStore()
    store.setup_metadata_always_up_to_date()
    store.setup_membership(user_hash, None)
    manager = BigSegmentStoreManager(BigSegmentsConfig(store=store))
    try:
        expected_result = ({}, BigSegmentsStatus.HEALTHY)
        assert manager.get_user_membership(user_key) == expected_result
        assert manager.get_user_membership(user_key) == expected_result
    finally:
        manager.stop()
    assert store.membership_queries == [user_hash]  # only 1 query done rather than 2, due to caching


def test_membership_query_cache_can_expire():
    expected_membership = {"key1": True, "key2": False}
    store = MockBigSegmentStore()
    store.setup_metadata_always_up_to_date()
    store.setup_membership(user_hash, expected_membership)
    manager = BigSegmentStoreManager(BigSegmentsConfig(store=store, context_cache_time=0.005))
    try:
        expected_result = (expected_membership, BigSegmentsStatus.HEALTHY)
        assert manager.get_user_membership(user_key) == expected_result
        time.sleep(0.1)
        assert manager.get_user_membership(user_key) == expected_result
    finally:
        manager.stop()
    assert store.membership_queries == [user_hash, user_hash]  # cache expired after 1st query


def test_membership_query_stale_status():
    expected_membership = {"key1": True, "key2": False}
    store = MockBigSegmentStore()
    store.setup_metadata_always_stale()
    store.setup_membership(user_hash, expected_membership)
    manager = BigSegmentStoreManager(BigSegmentsConfig(store=store))
    try:
        expected_result = (expected_membership, BigSegmentsStatus.STALE)
        assert manager.get_user_membership(user_key) == expected_result
    finally:
        manager.stop()


def test_membership_query_stale_status_no_store_metadata():
    expected_membership = {"key1": True, "key2": False}
    store = MockBigSegmentStore()
    store.setup_metadata_none()
    store.setup_membership(user_hash, expected_membership)
    manager = BigSegmentStoreManager(BigSegmentsConfig(store=store))
    try:
        expected_result = (expected_membership, BigSegmentsStatus.STALE)
        assert manager.get_user_membership(user_key) == expected_result
    finally:
        manager.stop()


def test_membership_query_least_recent_context_evicted_from_cache():
    user_key_1, user_key_2, user_key_3 = 'userkey1', 'userkey2', 'userkey3'
    user_hash_1, user_hash_2, user_hash_3 = _hash_for_user_key(user_key_1), _hash_for_user_key(user_key_2), _hash_for_user_key(user_key_3)
    membership_1, membership_2, membership_3 = {'seg1': True}, {'seg2': True}, {'seg3': True}
    store = MockBigSegmentStore()
    store.setup_metadata_always_up_to_date()
    store.setup_membership(user_hash_1, membership_1)
    store.setup_membership(user_hash_2, membership_2)
    store.setup_membership(user_hash_3, membership_3)

    manager = BigSegmentStoreManager(BigSegmentsConfig(store=store, context_cache_size=2))

    try:
        result1 = manager.get_user_membership(user_key_1)
        result2 = manager.get_user_membership(user_key_2)
        result3 = manager.get_user_membership(user_key_3)

        assert store.membership_queries == [user_hash_1, user_hash_2, user_hash_3]

        # Since the capacity is only 2 and user_key_1 was the least recently used, that key should be
        # evicted by the user_key_3 query. Now only user_key_2 and user_key_3 are in the cache, and
        # querying them again should not cause a new query to the store.
        result2a = manager.get_user_membership(user_key_2)
        result3a = manager.get_user_membership(user_key_3)
        assert result2a == result2
        assert result3a == result3

        assert store.membership_queries == [user_hash_1, user_hash_2, user_hash_3]

        result1a = manager.get_user_membership(user_key_1)
        assert result1a == result1

        assert store.membership_queries == [user_hash_1, user_hash_2, user_hash_3, user_hash_1]
    finally:
        manager.stop()


def test_status_polling_detects_store_unavailability():
    store = MockBigSegmentStore()
    store.setup_metadata_always_up_to_date()
    statuses = Queue()

    manager = BigSegmentStoreManager(BigSegmentsConfig(store=store, status_poll_interval=0.01))

    try:
        manager.status_provider.add_listener(lambda status: statuses.put(status))

        status1 = manager.status_provider.status
        assert status1.available is True

        store.setup_metadata_error()

        status2 = statuses.get(True, 1.0)
        assert status2.available is False

        store.setup_metadata_always_up_to_date()

        status3 = statuses.get(True, 1.0)
        assert status3.available is True
    finally:
        manager.stop()


def test_status_polling_detects_stale_status():
    store = MockBigSegmentStore()
    store.setup_metadata_always_up_to_date()
    statuses = Queue()

    manager = BigSegmentStoreManager(BigSegmentsConfig(store=store, status_poll_interval=0.01))

    try:
        manager.status_provider.add_listener(lambda status: statuses.put(status))

        status1 = manager.status_provider.status
        assert status1.stale is False

        store.setup_metadata_always_stale()

        status2 = statuses.get(True, 1.0)
        assert status2.stale is True

        store.setup_metadata_always_up_to_date()

        status3 = statuses.get(True, 1.0)
        assert status3.stale is False
    finally:
        manager.stop()
