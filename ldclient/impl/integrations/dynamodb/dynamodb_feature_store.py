import json

have_dynamodb = False
try:
    import boto3
    have_dynamodb = True
except ImportError:
    pass

from ldclient import log
from ldclient.feature_store import CacheConfig
from ldclient.feature_store_helpers import CachingStoreWrapper
from ldclient.interfaces import DiagnosticDescription, FeatureStore, FeatureStoreCore

#
# Internal implementation of the DynamoDB feature store.
#
# Implementation notes:
#
# * Feature flags, segments, and any other kind of entity the LaunchDarkly client may wish
# to store, are all put in the same table. The only two required attributes are "key" (which
# is present in all storeable entities) and "namespace" (a parameter from the client that is
# used to disambiguate between flags and segments).
#
# * Because of DynamoDB's restrictions on attribute values (e.g. empty strings are not
# allowed), the standard DynamoDB marshaling mechanism with one attribute per object property
# is not used. Instead, the entire object is serialized to JSON and stored in a single
# attribute, "item". The "version" property is also stored as a separate attribute since it
# is used for updates.
#
# * Since DynamoDB doesn't have transactions, the init() method - which replaces the entire data
# store - is not atomic, so there can be a race condition if another process is adding new data
# via upsert(). To minimize this, we don't delete all the data at the start; instead, we update
# the items we've received, and then delete all other items. That could potentially result in
# deleting new data from another process, but that would be the case anyway if the init()
# happened to execute later than the upsert(); we are relying on the fact that normally the
# process that did the init() will also receive the new data shortly and do its own upsert().
#
# * DynamoDB has a maximum item size of 400KB. Since each feature flag or user segment is
# stored as a single item, this mechanism will not work for extremely large flags or segments.
#

class _DynamoDBFeatureStoreCore(FeatureStoreCore):
    PARTITION_KEY = 'namespace'
    SORT_KEY = 'key'
    VERSION_ATTRIBUTE = 'version'
    ITEM_JSON_ATTRIBUTE = 'item'

    def __init__(self, table_name, prefix, dynamodb_opts):
        if not have_dynamodb:
            raise NotImplementedError("Cannot use DynamoDB feature store because AWS SDK (boto3 package) is not installed")
        self._table_name = table_name
        self._prefix = None if prefix == "" else  prefix
        self._client = boto3.client('dynamodb', **dynamodb_opts)

    def init_internal(self, all_data):
        # Start by reading the existing keys; we will later delete any of these that weren't in all_data.
        unused_old_keys = self._read_existing_keys(all_data.keys())
        requests = []
        num_items = 0
        inited_key = self._inited_key()

        # Insert or update every provided item
        for kind, items in all_data.items():
            for key, item in items.items():
                encoded_item = self._marshal_item(kind, item)
                requests.append({ 'PutRequest': { 'Item': encoded_item } })
                combined_key = (self._namespace_for_kind(kind), key)
                unused_old_keys.discard(combined_key)
                num_items = num_items + 1

        # Now delete any previously existing items whose keys were not in the current data
        for combined_key in unused_old_keys:
            if combined_key[0] != inited_key:
                requests.append({ 'DeleteRequest': { 'Key': self._make_keys(combined_key[0], combined_key[1]) } })

        # Now set the special key that we check in initialized_internal()
        requests.append({ 'PutRequest': { 'Item': self._make_keys(inited_key, inited_key) } })

        _DynamoDBHelpers.batch_write_requests(self._client, self._table_name, requests)
        log.info('Initialized table %s with %d items', self._table_name, num_items)

    def get_internal(self, kind, key):
        resp = self._get_item_by_keys(self._namespace_for_kind(kind), key)
        return self._unmarshal_item(resp.get('Item'))

    def get_all_internal(self, kind):
        items_out = {}
        paginator = self._client.get_paginator('query')
        for resp in paginator.paginate(**self._make_query_for_kind(kind)):
            for item in resp['Items']:
                item_out = self._unmarshal_item(item)
                items_out[item_out['key']] = item_out
        return items_out

    def upsert_internal(self, kind, item):
        encoded_item = self._marshal_item(kind, item)
        try:
            req = {
                'TableName': self._table_name,
                'Item': encoded_item,
                'ConditionExpression': 'attribute_not_exists(#namespace) or attribute_not_exists(#key) or :version > #version',
                'ExpressionAttributeNames': {
                    '#namespace': self.PARTITION_KEY,
                    '#key': self.SORT_KEY,
                    '#version': self.VERSION_ATTRIBUTE
                },
                'ExpressionAttributeValues': {
                    ':version': { 'N': str(item['version']) }
                }
            }
            self._client.put_item(**req)
        except self._client.exceptions.ConditionalCheckFailedException:
            # The item was not updated because there's a newer item in the database. We must now
            # read the item that's in the database and return it, so CachingStoreWrapper can cache it.
            return self.get_internal(kind, item['key'])
        return item

    def initialized_internal(self):
        resp = self._get_item_by_keys(self._inited_key(), self._inited_key())
        return resp.get('Item') is not None and len(resp['Item']) > 0

    def describe_configuration(self, config):
        return 'DynamoDB'

    def _prefixed_namespace(self, base):
        return base if self._prefix is None else (self._prefix + ':' + base)

    def _namespace_for_kind(self, kind):
        return self._prefixed_namespace(kind.namespace)

    def _inited_key(self):
        return self._prefixed_namespace('$inited')

    def _make_keys(self, namespace, key):
        return {
            self.PARTITION_KEY: { 'S': namespace },
            self.SORT_KEY: { 'S': key }
        }

    def _make_query_for_kind(self, kind):
        return {
            'TableName': self._table_name,
            'ConsistentRead': True,
            'KeyConditions': {
                self.PARTITION_KEY: {
                    'AttributeValueList': [
                        {  'S': self._namespace_for_kind(kind) }
                    ],
                    'ComparisonOperator': 'EQ'
                }
            }
        }

    def _get_item_by_keys(self, namespace, key):
        return self._client.get_item(TableName=self._table_name, Key=self._make_keys(namespace,  key))

    def _read_existing_keys(self, kinds):
        keys = set()
        for kind in kinds:
            req = self._make_query_for_kind(kind)
            req['ProjectionExpression'] = '#namespace, #key'
            req['ExpressionAttributeNames'] = {
                '#namespace': self.PARTITION_KEY,
                '#key': self.SORT_KEY
            }
            paginator = self._client.get_paginator('query')
            for resp in paginator.paginate(**req):
                for item in resp['Items']:
                    namespace = item[self.PARTITION_KEY]['S']
                    key = item[self.SORT_KEY]['S']
                    keys.add((namespace, key))
        return keys

    def _marshal_item(self, kind, item):
        json_str = json.dumps(item)
        ret = self._make_keys(self._namespace_for_kind(kind), item['key'])
        ret[self.VERSION_ATTRIBUTE] = { 'N': str(item['version']) }
        ret[self.ITEM_JSON_ATTRIBUTE] = { 'S': json_str }
        return ret

    def _unmarshal_item(self, item):
        if item is None:
            return None
        json_attr = item.get(self.ITEM_JSON_ATTRIBUTE)
        return None if json_attr is None else json.loads(json_attr['S'])


class _DynamoDBHelpers:
    @staticmethod
    def batch_write_requests(client, table_name, requests):
        batch_size = 25
        for batch in (requests[i:i+batch_size] for i in range(0, len(requests), batch_size)):
            client.batch_write_item(RequestItems={ table_name: batch })
