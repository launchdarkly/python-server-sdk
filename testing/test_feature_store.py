import boto3
import json
import os
import pytest
import redis
import time

from typing import List

# Consul is only supported in some Python versions
have_consul = False
try:
    import consul
    have_consul = True
except ImportError:
    pass

from ldclient.feature_store import CacheConfig, InMemoryFeatureStore
from ldclient.impl.integrations.dynamodb.dynamodb_feature_store import _DynamoDBFeatureStoreCore, _DynamoDBHelpers
from ldclient.integrations import Consul, DynamoDB, Redis
from ldclient.versioned_data_kind import FEATURES

skip_db_tests = os.environ.get('LD_SKIP_DATABASE_TESTS') == '1'

class Tester:
    pass

class InMemoryTester(Tester):
    def init_store(self):
        return InMemoryFeatureStore()

    @property
    def supports_prefix(self):
        return False


class RedisTester(Tester):
    redis_host = 'localhost'
    redis_port = 6379

    def __init__(self, cache_config):
        self._cache_config = cache_config

    def init_store(self, prefix=None):
        self._clear_data()
        return Redis.new_feature_store(caching=self._cache_config, prefix=prefix)

    @property
    def supports_prefix(self):
        return True

    def _clear_data(self):
        r = redis.StrictRedis(host=self.redis_host, port=self.redis_port, db=0)
        r.flushdb()

class ConsulTester(Tester):
    def __init__(self, cache_config):
        self._cache_config = cache_config

    def init_store(self, prefix=None):
        self._clear_data(prefix or "launchdarkly")
        return Consul.new_feature_store(prefix=prefix, caching=self._cache_config)

    @property
    def supports_prefix(self):
        return True

    def _clear_data(self, prefix):
        client = consul.Consul()
        index, keys = client.kv.get(prefix + "/", recurse=True, keys=True)
        for key in (keys or []):
            client.kv.delete(key)


class DynamoDBTester(Tester):
    table_name = 'LD_DYNAMODB_TEST_TABLE'
    table_created = False
    options = {
        'aws_access_key_id': 'key', # not used by local DynamoDB, but still required
        'aws_secret_access_key': 'secret',
        'endpoint_url': 'http://localhost:8000',
        'region_name': 'us-east-1'
    }

    def __init__(self, cache_config):
        self._cache_config = cache_config

    def init_store(self, prefix=None):
        self._create_table()
        self._clear_data()
        return DynamoDB.new_feature_store(self.table_name, prefix=prefix, dynamodb_opts=self.options,
            caching=self._cache_config)

    @property
    def supports_prefix(self):
        return True

    def _create_table(self):
        if self.table_created:
            return
        client = boto3.client('dynamodb', **self.options)
        try:
            client.describe_table(TableName=self.table_name)
            self.table_created = True
            return
        except client.exceptions.ResourceNotFoundException:
            pass
        req = {
            'TableName': self.table_name,
            'KeySchema': [
                {
                    'AttributeName': _DynamoDBFeatureStoreCore.PARTITION_KEY,
                    'KeyType': 'HASH',
                },
                {
                    'AttributeName': _DynamoDBFeatureStoreCore.SORT_KEY,
                    'KeyType': 'RANGE'
                }
            ],
            'AttributeDefinitions': [
                {
                    'AttributeName': _DynamoDBFeatureStoreCore.PARTITION_KEY,
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': _DynamoDBFeatureStoreCore.SORT_KEY,
                    'AttributeType': 'S'
                }
            ],
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        }
        client.create_table(**req)
        while True:
            try:
                client.describe_table(TableName=self.table_name)
                self.table_created = True
                return
            except client.exceptions.ResourceNotFoundException:
                time.sleep(0.5)

    def _clear_data(self):
        client = boto3.client('dynamodb', **self.options)
        delete_requests = []
        req = {
            'TableName': self.table_name,
            'ConsistentRead': True,
            'ProjectionExpression': '#namespace, #key',
            'ExpressionAttributeNames': {
                '#namespace': _DynamoDBFeatureStoreCore.PARTITION_KEY,
                '#key': _DynamoDBFeatureStoreCore.SORT_KEY
            }
        }
        for resp in client.get_paginator('scan').paginate(**req):
            for item in resp['Items']:
                delete_requests.append({ 'DeleteRequest': { 'Key': item } })
        _DynamoDBHelpers.batch_write_requests(client, self.table_name, delete_requests)


class TestFeatureStore:
    params = [] # type: List[Tester]
    if skip_db_tests:
        params += [
            InMemoryTester()
        ]
    else:
        params += [
            InMemoryTester(),
            RedisTester(CacheConfig.default()),
            RedisTester(CacheConfig.disabled()),
            DynamoDBTester(CacheConfig.default()),
            DynamoDBTester(CacheConfig.disabled())
        ]
        if have_consul:
            params.append(ConsulTester(CacheConfig.default()))
            params.append(ConsulTester(CacheConfig.disabled()))

    @pytest.fixture(params=params)
    def tester(self, request):
        return request.param

    @pytest.fixture(params=params)
    def store(self, request):
        return request.param.init_store()

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
            FEATURES: {
                'foo': self.make_feature('foo', 10),
                'bar': self.make_feature('bar', 10),
            }
        })
        return store

    def test_not_initialized_before_init(self, store):
        assert store.initialized is False

    def test_initialized(self, store):
        store = self.base_initialized_store(store)
        assert store.initialized is True

    def test_get_existing_feature(self, store):
        store = self.base_initialized_store(store)
        expected = self.make_feature('foo', 10)
        assert store.get(FEATURES, 'foo', lambda x: x) == expected

    def test_get_nonexisting_feature(self, store):
        store = self.base_initialized_store(store)
        assert store.get(FEATURES, 'biz', lambda x: x) is None

    def test_get_all_versions(self, store):
        store = self.base_initialized_store(store)
        result = store.all(FEATURES, lambda x: x)
        assert len(result) == 2
        assert result.get('foo') == self.make_feature('foo', 10)
        assert result.get('bar') == self.make_feature('bar', 10)

    def test_upsert_with_newer_version(self, store):
        store = self.base_initialized_store(store)
        new_ver = self.make_feature('foo', 11)
        store.upsert(FEATURES, new_ver)
        assert store.get(FEATURES, 'foo', lambda x: x) == new_ver

    def test_upsert_with_older_version(self, store):
        store = self.base_initialized_store(store)
        new_ver = self.make_feature('foo', 9)
        expected = self.make_feature('foo', 10)
        store.upsert(FEATURES, new_ver)
        assert store.get(FEATURES, 'foo', lambda x: x) == expected

    def test_upsert_with_new_feature(self, store):
        store = self.base_initialized_store(store)
        new_ver = self.make_feature('biz', 1)
        store.upsert(FEATURES, new_ver)
        assert store.get(FEATURES, 'biz', lambda x: x) == new_ver

    def test_delete_with_newer_version(self, store):
        store = self.base_initialized_store(store)
        store.delete(FEATURES, 'foo', 11)
        assert store.get(FEATURES, 'foo', lambda x: x) is None

    def test_delete_unknown_feature(self, store):
        store = self.base_initialized_store(store)
        store.delete(FEATURES, 'biz', 11)
        assert store.get(FEATURES, 'biz', lambda x: x) is None

    def test_delete_with_older_version(self, store):
        store = self.base_initialized_store(store)
        store.delete(FEATURES, 'foo', 9)
        expected = self.make_feature('foo', 10)
        assert store.get(FEATURES, 'foo', lambda x: x) == expected

    def test_upsert_older_version_after_delete(self, store):
        store = self.base_initialized_store(store)
        store.delete(FEATURES, 'foo', 11)
        old_ver = self.make_feature('foo', 9)
        store.upsert(FEATURES, old_ver)
        assert store.get(FEATURES, 'foo', lambda x: x) is None

    def test_stores_with_different_prefixes_are_independent(self, tester):
        # This verifies that init(), get(), all(), and upsert() are all correctly using the specified key prefix.
        # The delete() method isn't tested separately because it's implemented as a variant of upsert().
        if not tester.supports_prefix:
            return

        flag_a1 = { 'key': 'flagA1', 'version': 1 }
        flag_a2 = { 'key': 'flagA2', 'version': 1 }
        flag_b1 = { 'key': 'flagB1', 'version': 1 }
        flag_b2 = { 'key': 'flagB2', 'version': 1 }
        store_a = tester.init_store('a')
        store_b = tester.init_store('b')

        store_a.init({ FEATURES: { 'flagA1': flag_a1 } })
        store_a.upsert(FEATURES, flag_a2)

        store_b.init({ FEATURES: { 'flagB1': flag_b1 } })
        store_b.upsert(FEATURES, flag_b2)

        item = store_a.get(FEATURES, 'flagA1', lambda x: x)
        assert item == flag_a1
        item = store_a.get(FEATURES, 'flagB1', lambda x: x)
        assert item is None
        items = store_a.all(FEATURES, lambda x: x)
        assert items == { 'flagA1': flag_a1, 'flagA2': flag_a2 }

        item = store_b.get(FEATURES, 'flagB1', lambda x: x)
        assert item == flag_b1
        item = store_b.get(FEATURES, 'flagA1', lambda x: x)
        assert item is None
        items = store_b.all(FEATURES, lambda x: x)
        assert items == { 'flagB1': flag_b1, 'flagB2': flag_b2 }


@pytest.mark.skipif(skip_db_tests, reason="skipping database tests")
class TestRedisFeatureStoreExtraTests:
    def test_upsert_race_condition_against_external_client_with_higher_version(self):
        other_client = redis.StrictRedis(host='localhost', port=6379, db=0)
        store = Redis.new_feature_store()
        store.init({ FEATURES: {} })

        other_version = {u'key': u'flagkey', u'version': 2}
        def hook(base_key, key):
            if other_version['version'] <= 4:
                other_client.hset(base_key, key, json.dumps(other_version))
                other_version['version'] = other_version['version'] + 1
        store._core.test_update_hook = hook

        feature = { u'key': 'flagkey', u'version': 1 }

        store.upsert(FEATURES, feature)
        result = store.get(FEATURES, 'flagkey', lambda x: x)
        assert result['version'] == 2

    def test_upsert_race_condition_against_external_client_with_lower_version(self):
        other_client = redis.StrictRedis(host='localhost', port=6379, db=0)
        store = Redis.new_feature_store()
        store.init({ FEATURES: {} })

        other_version = {u'key': u'flagkey', u'version': 2}
        def hook(base_key, key):
            if other_version['version'] <= 4:
                other_client.hset(base_key, key, json.dumps(other_version))
                other_version['version'] = other_version['version'] + 1
        store._core.test_update_hook = hook

        feature = { u'key': 'flagkey', u'version': 5 }

        store.upsert(FEATURES, feature)
        result = store.get(FEATURES, 'flagkey', lambda x: x)
        assert result['version'] == 5
