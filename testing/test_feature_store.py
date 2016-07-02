import pytest
import redis

from ldclient.feature_store import InMemoryFeatureStore
from ldclient.redis_feature_store import RedisFeatureStore


class TestFeatureStore:
    redis_host = 'localhost'
    redis_port = 6379

    def in_memory(self):
        return InMemoryFeatureStore()

    def redis_with_local_cache(self):
        r = redis.StrictRedis(host=self.redis_host, port=self.redis_port, db=0)
        r.delete("launchdarkly:features")
        return RedisFeatureStore(host=self.redis_host, port=self.redis_port)

    def redis_no_local_cache(self):
        r = redis.StrictRedis(host=self.redis_host, port=self.redis_port, db=0)
        r.delete("launchdarkly:features")
        return RedisFeatureStore(host=self.redis_host, port=self.redis_port, expiration=0)

    params = [in_memory, redis_with_local_cache, redis_no_local_cache]

    @pytest.fixture(params=params)
    def store(self, request):
        return request.param(self)

    @staticmethod
    def make_feature(key, ver):
        return {
            u'key': key,
            u'version': ver,
            u'salt': u'abc',
            u'on': True,
            u'variations': [
                {
                    u'value': True,
                    u'weight': 100,
                    u'targets': []
                },
                {
                    u'value': False,
                    u'weight': 0,
                    u'targets': []
                }
            ]
        }

    def base_initialized_store(self, store):
        store.init({
            'foo': self.make_feature('foo', 10),
            'bar': self.make_feature('bar', 10),
        })
        return store

    def test_not_initially_initialized(self, store):
        assert store.initialized is False

    def test_initialized(self, store):
        store = self.base_initialized_store(store)
        assert store.initialized is True

    def test_get_existing_feature(self, store):
        store = self.base_initialized_store(store)
        expected = self.make_feature('foo', 10)
        assert store.get('foo') == expected

    def test_get_nonexisting_feature(self, store):
        store = self.base_initialized_store(store)
        assert store.get('biz') is None

    def test_upsert_with_newer_version(self, store):
        store = self.base_initialized_store(store)
        new_ver = self.make_feature('foo', 11)
        store.upsert('foo', new_ver)
        assert store.get('foo') == new_ver

    def test_upsert_with_older_version(self, store):
        store = self.base_initialized_store(store)
        new_ver = self.make_feature('foo', 9)
        expected = self.make_feature('foo', 10)
        store.upsert('foo', new_ver)
        assert store.get('foo') == expected

    def test_upsert_with_new_feature(self, store):
        store = self.base_initialized_store(store)
        new_ver = self.make_feature('biz', 1)
        store.upsert('biz', new_ver)
        assert store.get('biz') == new_ver

    def test_delete_with_newer_version(self, store):
        store = self.base_initialized_store(store)
        store.delete('foo', 11)
        assert store.get('foo') is None

    def test_delete_unknown_feature(self, store):
        store = self.base_initialized_store(store)
        store.delete('biz', 11)
        assert store.get('biz') is None

    def test_delete_with_older_version(self, store):
        store = self.base_initialized_store(store)
        store.delete('foo', 9)
        expected = self.make_feature('foo', 10)
        assert store.get('foo') == expected
