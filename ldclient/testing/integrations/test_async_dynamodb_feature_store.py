"""
Integration tests for the async DynamoDB feature store (_AsyncDynamoDBFeatureStoreCore wrapped by
AsyncCachingStoreWrapper).

These tests require a local DynamoDB instance running on localhost:8000. They are skipped when the
aioboto3 package is not installed or when the LD_SKIP_DATABASE_TESTS environment variable is set to
'1'. The caching-wrapper logic itself is covered without DynamoDB in
ldclient.testing.test_async_feature_store_helpers. Table setup and teardown use the synchronous
boto3 client, so the database-backed tests also need boto3.
"""

import time

import pytest

from ldclient.async_feature_store_helpers import AsyncCachingStoreWrapper
from ldclient.feature_store import CacheConfig
from ldclient.impl.integrations.dynamodb.async_dynamodb_feature_store import (
    _AsyncDynamoDBFeatureStoreCore,
    have_aioboto3
)
from ldclient.integrations import DynamoDB
from ldclient.interfaces import AsyncFeatureStore
from ldclient.testing.async_feature_store_test_base import (
    AsyncFeatureStoreTestBase,
    AsyncFeatureStoreTester
)
from ldclient.testing.test_util import skip_database_tests

have_aioboto3 = False
try:
    import aioboto3

    have_aioboto3 = True
except ImportError:
    pass

try:
    import boto3

    have_boto3 = True
except ImportError:
    have_boto3 = False

pytestmark = pytest.mark.skipif(
    not have_aioboto3,
    reason="skipping async DynamoDB tests because aioboto3 package is not installed"
)


class DynamoDBTestHelper:
    table_name = 'LD_DYNAMODB_TEST_TABLE'
    table_created = False
    options = {'aws_access_key_id': 'key', 'aws_secret_access_key': 'secret', 'endpoint_url': 'http://localhost:8000', 'region_name': 'us-east-1'}  # not used by local DynamoDB, but still required

    @staticmethod
    def make_client():
        return boto3.client('dynamodb', **DynamoDBTestHelper.options)

    @staticmethod
    def clear_data_for_prefix(prefix):
        client = DynamoDBTestHelper.make_client()
        delete_requests = []
        req = {
            'TableName': DynamoDBTestHelper.table_name,
            'ConsistentRead': True,
            'ProjectionExpression': '#namespace, #key',
            'ExpressionAttributeNames': {'#namespace': _AsyncDynamoDBFeatureStoreCore.PARTITION_KEY, '#key': _AsyncDynamoDBFeatureStoreCore.SORT_KEY},
        }
        for resp in client.get_paginator('scan').paginate(**req):
            for item in resp['Items']:
                delete_requests.append({'DeleteRequest': {'Key': item}})
        _sync_batch_write_requests(client, DynamoDBTestHelper.table_name, delete_requests)

    @staticmethod
    def ensure_table_created():
        if DynamoDBTestHelper.table_created:
            return
        DynamoDBTestHelper.table_created = True
        client = DynamoDBTestHelper.make_client()
        try:
            client.describe_table(TableName=DynamoDBTestHelper.table_name)
            return
        except client.exceptions.ResourceNotFoundException:
            pass
        req = {
            'TableName': DynamoDBTestHelper.table_name,
            'KeySchema': [
                {'AttributeName': _AsyncDynamoDBFeatureStoreCore.PARTITION_KEY, 'KeyType': 'HASH'},
                {'AttributeName': _AsyncDynamoDBFeatureStoreCore.SORT_KEY, 'KeyType': 'RANGE'},
            ],
            'AttributeDefinitions': [
                {'AttributeName': _AsyncDynamoDBFeatureStoreCore.PARTITION_KEY, 'AttributeType': 'S'},
                {'AttributeName': _AsyncDynamoDBFeatureStoreCore.SORT_KEY, 'AttributeType': 'S'},
            ],
            'ProvisionedThroughput': {'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1},
        }
        client.create_table(**req)
        while True:
            try:
                client.describe_table(TableName=DynamoDBTestHelper.table_name)
                return
            except client.exceptions.ResourceNotFoundException:
                time.sleep(0.5)


def _sync_batch_write_requests(client, table_name, requests):
    batch_size = 25
    for batch in (requests[i: i + batch_size] for i in range(0, len(requests), batch_size)):
        client.batch_write_item(RequestItems={table_name: batch})


class AsyncDynamoDBFeatureStoreTester(AsyncFeatureStoreTester):
    def __init__(self, prefix=None, caching=None):
        self.prefix = prefix
        self.caching = caching if caching is not None else CacheConfig.disabled()
        DynamoDBTestHelper.ensure_table_created()

    async def create_feature_store(self) -> AsyncFeatureStore:
        return DynamoDB.async_feature_store(DynamoDBTestHelper.table_name, prefix=self.prefix, caching=self.caching, dynamodb_opts=DynamoDBTestHelper.options)


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
@pytest.mark.skipif(not have_boto3, reason="skipping: boto3 not available for test setup")
class TestAsyncDynamoDBFeatureStore(AsyncFeatureStoreTestBase):
    @pytest.fixture(params=[(False, False), (True, False), (False, True), (True, True)])
    def tester(self, request):
        specify_prefix, use_caching = request.param
        prefix = "testprefix" if specify_prefix else None
        caching = CacheConfig.default() if use_caching else CacheConfig.disabled()
        return AsyncDynamoDBFeatureStoreTester(prefix, caching)

    @pytest.fixture(autouse=True)
    def clear_data_before_each(self, tester):
        DynamoDBTestHelper.clear_data_for_prefix(tester.prefix)


@pytest.mark.asyncio
@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
@pytest.mark.skipif(not have_boto3, reason="skipping: boto3 not available for test setup")
async def test_available_and_monitoring():
    DynamoDBTestHelper.ensure_table_created()
    store = DynamoDB.async_feature_store(DynamoDBTestHelper.table_name, dynamodb_opts=DynamoDBTestHelper.options)
    try:
        assert store.is_monitoring_enabled() is True
        assert await store.is_available() is True
    finally:
        await store.close()


@pytest.mark.asyncio
@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
async def test_detects_nonexistent_store():
    options = dict(DynamoDBTestHelper.options)
    options['endpoint_url'] = 'http://i-mean-what-are-the-odds'
    store = DynamoDB.async_feature_store(DynamoDBTestHelper.table_name, dynamodb_opts=options)
    try:
        assert store.is_monitoring_enabled() is True
        assert await store.is_available() is False
    finally:
        await store.close()


@pytest.mark.asyncio
@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
@pytest.mark.skipif(not have_boto3, reason="skipping: boto3 not available for test setup")
async def test_stores_with_different_prefixes_are_independent():
    from ldclient.versioned_data_kind import FEATURES

    DynamoDBTestHelper.ensure_table_created()
    DynamoDBTestHelper.clear_data_for_prefix("a")
    DynamoDBTestHelper.clear_data_for_prefix("b")

    flag_a1 = {'key': 'flagA1', 'version': 1}
    flag_a2 = {'key': 'flagA2', 'version': 1}
    flag_b1 = {'key': 'flagB1', 'version': 1}
    flag_b2 = {'key': 'flagB2', 'version': 1}

    store_a = DynamoDB.async_feature_store(DynamoDBTestHelper.table_name, prefix="a", dynamodb_opts=DynamoDBTestHelper.options)
    store_b = DynamoDB.async_feature_store(DynamoDBTestHelper.table_name, prefix="b", dynamodb_opts=DynamoDBTestHelper.options)
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


def test_async_feature_store_is_caching_wrapper():
    store = DynamoDB.async_feature_store(DynamoDBTestHelper.table_name)
    assert isinstance(store, AsyncCachingStoreWrapper)


def test_constructing_without_aioboto3_raises(monkeypatch):
    import ldclient.impl.integrations.dynamodb.async_dynamodb_feature_store as mod

    monkeypatch.setattr(mod, "have_aioboto3", False)
    with pytest.raises(NotImplementedError):
        mod._AsyncDynamoDBFeatureStoreCore(DynamoDBTestHelper.table_name, None, {})


@pytest.mark.skipif(not have_aioboto3, reason="aioboto3 is not installed")
@pytest.mark.asyncio
async def test_get_client_after_close_raises():
    # After close(), _get_client() must not build a new client on the
    # already-closed exit stack; it raises instead of leaking one.
    core = _AsyncDynamoDBFeatureStoreCore(DynamoDBTestHelper.table_name, None, {})
    await core.close()
    with pytest.raises(RuntimeError):
        await core._get_client()
