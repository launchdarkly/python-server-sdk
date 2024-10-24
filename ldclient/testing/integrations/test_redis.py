import json

import pytest

from ldclient.impl.integrations.redis.redis_big_segment_store import \
    _RedisBigSegmentStore
from ldclient.integrations import Redis
from ldclient.testing.integrations.big_segment_store_test_base import *
from ldclient.testing.integrations.persistent_feature_store_test_base import *
from ldclient.testing.test_util import skip_database_tests
from ldclient.versioned_data_kind import FEATURES

have_redis = False
try:
    import redis

    have_redis = True
except ImportError:
    pass

pytestmark = pytest.mark.skipif(not have_redis, reason="skipping Redis tests because redis module is not installed")


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
def redis_defaults_to_available():
    redis = Redis.new_feature_store()
    assert redis.is_monitoring_enabled() is True
    assert redis.is_available() is True


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
def redis_detects_nonexistent_store():
    redis = Redis.new_feature_store(url='http://i-mean-what-are-the-odds')
    assert redis.is_monitoring_enabled() is True
    assert redis.is_available() is False


class RedisTestHelper:
    @staticmethod
    def make_client() -> redis.StrictRedis:
        return redis.StrictRedis(host="localhost", port=6379, db=0)

    def clear_data_for_prefix(prefix):
        r = RedisTestHelper.make_client()
        for key in r.keys("%s:*" % prefix):
            r.delete(key)


class RedisFeatureStoreTester(PersistentFeatureStoreTester):
    def create_persistent_feature_store(self, prefix, caching) -> FeatureStore:
        return Redis.new_feature_store(prefix=prefix, caching=caching)

    def clear_data(self, prefix):
        RedisTestHelper.clear_data_for_prefix(prefix or Redis.DEFAULT_PREFIX)


class RedisBigSegmentStoreTester(BigSegmentStoreTester):
    def create_big_segment_store(self, prefix) -> BigSegmentStore:
        return Redis.new_big_segment_store(prefix=prefix)

    def clear_data(self, prefix):
        RedisTestHelper.clear_data_for_prefix(prefix or Redis.DEFAULT_PREFIX)

    def set_metadata(self, prefix: str, metadata: BigSegmentStoreMetadata):
        r = RedisTestHelper.make_client()
        r.set((prefix or Redis.DEFAULT_PREFIX) + _RedisBigSegmentStore.KEY_LAST_UP_TO_DATE, "" if metadata.last_up_to_date is None else str(metadata.last_up_to_date))

    def set_segments(self, prefix: str, user_hash: str, includes: List[str], excludes: List[str]):
        r = RedisTestHelper.make_client()
        prefix = prefix or Redis.DEFAULT_PREFIX
        for ref in includes:
            r.sadd(prefix + _RedisBigSegmentStore.KEY_USER_INCLUDE + user_hash, ref)
        for ref in excludes:
            r.sadd(prefix + _RedisBigSegmentStore.KEY_USER_EXCLUDE + user_hash, ref)


class TestRedisFeatureStore(PersistentFeatureStoreTestBase):
    @property
    def tester_class(self):
        return RedisFeatureStoreTester

    def test_upsert_race_condition_against_external_client_with_higher_version(self):
        other_client = RedisTestHelper.make_client()
        store = Redis.new_feature_store()
        store.init({FEATURES: {}})

        other_version = {u'key': u'flagkey', u'version': 2}

        def hook(base_key, key):
            if other_version['version'] <= 4:
                other_client.hset(base_key, key, json.dumps(other_version))
                other_version['version'] = other_version['version'] + 1

        store._core.test_update_hook = hook

        feature = {u'key': 'flagkey', u'version': 1}

        store.upsert(FEATURES, feature)
        result = store.get(FEATURES, 'flagkey', lambda x: x)
        assert result['version'] == 2

    def test_upsert_race_condition_against_external_client_with_lower_version(self):
        other_client = RedisTestHelper.make_client()
        store = Redis.new_feature_store()
        store.init({FEATURES: {}})

        other_version = {u'key': u'flagkey', u'version': 2}

        def hook(base_key, key):
            if other_version['version'] <= 4:
                other_client.hset(base_key, key, json.dumps(other_version))
                other_version['version'] = other_version['version'] + 1

        store._core.test_update_hook = hook

        feature = {u'key': 'flagkey', u'version': 5}

        store.upsert(FEATURES, feature)
        result = store.get(FEATURES, 'flagkey', lambda x: x)
        assert result['version'] == 5


class TestRedisBigSegmentStore(BigSegmentStoreTestBase):
    @property
    def tester_class(self):
        return RedisBigSegmentStoreTester
