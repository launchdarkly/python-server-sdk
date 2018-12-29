import boto3
import json
import pytest
import redis
import time

from ldclient.dynamodb_feature_store import _DynamoDBFeatureStoreCore, _DynamoDBHelpers
from ldclient.feature_store import CacheConfig, InMemoryFeatureStore
from ldclient.integrations import DynamoDB, Redis
from ldclient.redis_feature_store import RedisFeatureStore
from ldclient.versioned_data_kind import FEATURES


def get_log_lines(caplog):
    loglines = caplog.records
    if callable(loglines):
        # records() is a function in older versions of the caplog plugin
        loglines = loglines()
    return loglines


class InMemoryTester(object):
    def init_store(self):
        return InMemoryFeatureStore()


class RedisTester(object):
    redis_host = 'localhost'
    redis_port = 6379

    def __init__(self, cache_config):
        self._cache_config = cache_config
    
    def init_store(self):
        self._clear_data()
        return Redis.new_feature_store(caching=self._cache_config)

    def _clear_data(self):
        r = redis.StrictRedis(host=self.redis_host, port=self.redis_port, db=0)
        r.delete("launchdarkly:features")


class RedisWithDeprecatedConstructorTester(RedisTester):
    def init_store(self):
        self._clear_data()
        return RedisFeatureStore(expiration=(30 if self._cache_config.enabled else 0))


class DynamoDBTester(object):
    table_name = 'LD_DYNAMODB_TEST_TABLE'
    table_created = False
    options = { 'endpoint_url': 'http://localhost:8000', 'region_name': 'us-east-1' }

    def __init__(self, cache_config):
        self._cache_config = cache_config
    
    def init_store(self):
        self._create_table()
        self._clear_data()
        return DynamoDB.new_feature_store(self.table_name, dynamodb_opts=self.options)

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
    params = [
        InMemoryTester(),
        RedisTester(CacheConfig.default()),
        RedisTester(CacheConfig.disabled()),
        RedisWithDeprecatedConstructorTester(CacheConfig.default()),
        RedisWithDeprecatedConstructorTester(CacheConfig.disabled()),
        DynamoDBTester(CacheConfig.default()),
        DynamoDBTester(CacheConfig.disabled())
    ]

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
        assert len(result) is 2
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


class TestRedisFeatureStoreExtraTests:
    def test_upsert_race_condition_against_external_client_with_higher_version(self):
        other_client = redis.StrictRedis(host='localhost', port=6379, db=0)
        store = RedisFeatureStore()
        store.init({ FEATURES: {} })

        other_version = {u'key': u'flagkey', u'version': 2}
        def hook(base_key, key):
            if other_version['version'] <= 4:
                other_client.hset(base_key, key, json.dumps(other_version))
                other_version['version'] = other_version['version'] + 1
        store.core.test_update_hook = hook

        feature = { u'key': 'flagkey', u'version': 1 }

        store.upsert(FEATURES, feature)
        result = store.get(FEATURES, 'flagkey', lambda x: x)
        assert result['version'] == 2

    def test_upsert_race_condition_against_external_client_with_lower_version(self):
        other_client = redis.StrictRedis(host='localhost', port=6379, db=0)
        store = RedisFeatureStore()
        store.init({ FEATURES: {} })

        other_version = {u'key': u'flagkey', u'version': 2}
        def hook(base_key, key):
            if other_version['version'] <= 4:
                other_client.hset(base_key, key, json.dumps(other_version))
                other_version['version'] = other_version['version'] + 1
        store.core.test_update_hook = hook

        feature = { u'key': 'flagkey', u'version': 5 }

        store.upsert(FEATURES, feature)
        result = store.get(FEATURES, 'flagkey', lambda x: x)
        assert result['version'] == 5

    def test_exception_is_handled_in_get(self, caplog):
        # This just verifies the fix for a bug that caused an error during exception handling in Python 3
        store = RedisFeatureStore(url='redis://bad')
        feature = store.get(FEATURES, 'flagkey')
        assert feature is None
        loglines = get_log_lines(caplog)
        assert len(loglines) == 2
        message = loglines[1].message
        assert message.startswith("RedisFeatureStore: Could not retrieve key flagkey from 'features' with error:")
        assert "connecting to bad:6379" in message

    def test_exception_is_handled_in_all(self, caplog):
        # This just verifies the fix for a bug that caused an error during exception handling in Python 3
        store = RedisFeatureStore(url='redis://bad')
        all = store.all(FEATURES, lambda x: x)
        assert all == {}
        loglines = get_log_lines(caplog)
        assert len(loglines) == 2
        message = loglines[1].message
        assert message.startswith("RedisFeatureStore: Could not retrieve 'features' from Redis")
        assert "connecting to bad:6379" in message
