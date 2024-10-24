import time

from ldclient.impl.integrations.dynamodb.dynamodb_big_segment_store import \
    _DynamoDBBigSegmentStore
from ldclient.impl.integrations.dynamodb.dynamodb_feature_store import (
    _DynamoDBFeatureStoreCore, _DynamoDBHelpers)
from ldclient.integrations import DynamoDB
from ldclient.interfaces import UpdateProcessor
from ldclient.testing.integrations.big_segment_store_test_base import *
from ldclient.testing.integrations.persistent_feature_store_test_base import *
from ldclient.testing.test_util import skip_database_tests

have_dynamodb = False
try:
    import boto3

    have_dynamodb = True
except ImportError:
    pass

pytestmark = pytest.mark.skipif(not have_dynamodb, reason="skipping DynamoDB tests because boto3 module is not installed")


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
def dynamodb_defaults_to_available():
    dynamodb = DynamoDB.new_feature_store(DynamoDBTestHelper.table_name, prefix=prefix, caching=caching, dynamodb_opts=DynamoDBTestHelper.options)
    assert dynamodb.is_monitoring_enabled() is True
    assert dynamodb.is_available() is True


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
def dynamodb_detects_nonexistent_store():
    options = DynamoDBTestHelper.options
    options['endpoint_url'] = 'http://i-mean-what-are-the-odds'
    dynamodb = DynamoDB.new_feature_store(DynamoDBTestHelper.table_name, prefix=prefix, caching=caching, dynamodb_opts=options)
    assert dynamodb.is_monitoring_enabled() is True
    assert dynamodb.is_available() is False


class DynamoDBTestHelper:
    table_name = 'LD_DYNAMODB_TEST_TABLE'
    table_created = False
    options = {'aws_access_key_id': 'key', 'aws_secret_access_key': 'secret', 'endpoint_url': 'http://localhost:8000', 'region_name': 'us-east-1'}  # not used by local DynamoDB, but still required

    @staticmethod
    def make_client():
        return boto3.client('dynamodb', **DynamoDBTestHelper.options)

    def clear_data_for_prefix(prefix):
        client = DynamoDBTestHelper.make_client()
        delete_requests = []
        req = {
            'TableName': DynamoDBTestHelper.table_name,
            'ConsistentRead': True,
            'ProjectionExpression': '#namespace, #key',
            'ExpressionAttributeNames': {'#namespace': _DynamoDBFeatureStoreCore.PARTITION_KEY, '#key': _DynamoDBFeatureStoreCore.SORT_KEY},
        }
        for resp in client.get_paginator('scan').paginate(**req):
            for item in resp['Items']:
                delete_requests.append({'DeleteRequest': {'Key': item}})
        _DynamoDBHelpers.batch_write_requests(client, DynamoDBTestHelper.table_name, delete_requests)

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
                {
                    'AttributeName': _DynamoDBFeatureStoreCore.PARTITION_KEY,
                    'KeyType': 'HASH',
                },
                {'AttributeName': _DynamoDBFeatureStoreCore.SORT_KEY, 'KeyType': 'RANGE'},
            ],
            'AttributeDefinitions': [{'AttributeName': _DynamoDBFeatureStoreCore.PARTITION_KEY, 'AttributeType': 'S'}, {'AttributeName': _DynamoDBFeatureStoreCore.SORT_KEY, 'AttributeType': 'S'}],
            'ProvisionedThroughput': {'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1},
        }
        client.create_table(**req)
        while True:
            try:
                client.describe_table(TableName=DynamoDBTestHelper.table_name)
                return
            except client.exceptions.ResourceNotFoundException:
                time.sleep(0.5)


class DynamoDBFeatureStoreTester(PersistentFeatureStoreTester):
    def __init__(self):
        super().__init__()
        DynamoDBTestHelper.ensure_table_created()

    def create_persistent_feature_store(self, prefix, caching) -> FeatureStore:
        return DynamoDB.new_feature_store(DynamoDBTestHelper.table_name, prefix=prefix, caching=caching, dynamodb_opts=DynamoDBTestHelper.options)

    def clear_data(self, prefix):
        DynamoDBTestHelper.clear_data_for_prefix(prefix)


class DynamoDBBigSegmentTester(BigSegmentStoreTester):
    def __init__(self):
        super().__init__()
        DynamoDBTestHelper.ensure_table_created()

    def create_big_segment_store(self, prefix) -> BigSegmentStore:
        return DynamoDB.new_big_segment_store(DynamoDBTestHelper.table_name, prefix=prefix, dynamodb_opts=DynamoDBTestHelper.options)

    def clear_data(self, prefix):
        DynamoDBTestHelper.clear_data_for_prefix(prefix)

    def set_metadata(self, prefix: str, metadata: BigSegmentStoreMetadata):
        client = DynamoDBTestHelper.make_client()
        actual_prefix = prefix + ":" if prefix else ""
        key = actual_prefix + _DynamoDBBigSegmentStore.KEY_METADATA
        client.put_item(
            TableName=DynamoDBTestHelper.table_name,
            Item={
                _DynamoDBBigSegmentStore.PARTITION_KEY: {"S": key},
                _DynamoDBBigSegmentStore.SORT_KEY: {"S": key},
                _DynamoDBBigSegmentStore.ATTR_SYNC_TIME: {"N": "" if metadata.last_up_to_date is None else str(metadata.last_up_to_date)},
            },
        )

    def set_segments(self, prefix: str, user_hash: str, includes: List[str], excludes: List[str]):
        client = DynamoDBTestHelper.make_client()
        actual_prefix = prefix + ":" if prefix else ""
        sets = {_DynamoDBBigSegmentStore.ATTR_INCLUDED: includes, _DynamoDBBigSegmentStore.ATTR_EXCLUDED: excludes}
        for attr_name, values in sets.items():
            if len(values) > 0:
                client.update_item(
                    TableName=DynamoDBTestHelper.table_name,
                    Key={_DynamoDBBigSegmentStore.PARTITION_KEY: {"S": actual_prefix + _DynamoDBBigSegmentStore.KEY_USER_DATA}, _DynamoDBBigSegmentStore.SORT_KEY: {"S": user_hash}},
                    UpdateExpression="ADD %s :value" % attr_name,
                    ExpressionAttributeValues={":value": {"SS": values}},
                )


class TestDynamoDBFeatureStore(PersistentFeatureStoreTestBase):
    @property
    def tester_class(self):
        return DynamoDBFeatureStoreTester


class TestDynamoDBBigSegmentStore(BigSegmentStoreTestBase):
    @property
    def tester_class(self):
        return DynamoDBBigSegmentTester
