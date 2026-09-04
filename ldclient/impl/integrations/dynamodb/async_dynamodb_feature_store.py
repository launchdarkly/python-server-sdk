import asyncio
import json
from contextlib import AsyncExitStack
from typing import Any, Mapping, Optional, cast

from ldclient.impl.util import log
from ldclient.interfaces import AsyncFeatureStoreCore, DiagnosticDescription
from ldclient.versioned_data_kind import VersionedDataKind

have_aioboto3 = False
try:
    import aioboto3

    have_aioboto3 = True
except ImportError:
    pass


#
# Internal implementation of the async DynamoDB feature store.
#
# Implementation notes:
#
# * Feature flags, segments, and any other kind of entity are all put in the same table. The two
# required attributes are "key" (present in all storeable entities) and "namespace" (used to
# disambiguate between flags and segments).
#
# * Because of DynamoDB's restrictions on attribute values (e.g. empty strings are not allowed), the
# standard DynamoDB marshaling with one attribute per object property is not used. Instead, the
# entire object is serialized to JSON and stored in a single attribute, "item". The "version"
# property is also stored as a separate attribute since it is used for updates.
#
# * Since DynamoDB has no transactions, init() - which replaces the entire data store - is not
# atomic, so there can be a race condition if another process is adding new data via upsert(). To
# minimize this, we do not delete all the data at the start; instead, we update the items we have
# received, and then delete all other items. That could delete new data from another process, but
# that would happen anyway if the init() ran later than the upsert(); we rely on the fact that the
# process that did the init() will normally receive the new data shortly and do its own upsert().
#
# * DynamoDB has a maximum item size of 400KB. Since each feature flag or user segment is stored as
# a single item, this mechanism will not work for extremely large flags or segments.
#
# * aioboto3 clients are async context managers, so unlike the synchronous boto3 client they cannot
# be created in __init__. The client is created and entered lazily on first use inside the running
# event loop, kept for the lifetime of the store, and released in close().
#


class _AsyncDynamoDBFeatureStoreCore(DiagnosticDescription, AsyncFeatureStoreCore):
    PARTITION_KEY = 'namespace'
    SORT_KEY = 'key'
    VERSION_ATTRIBUTE = 'version'
    ITEM_JSON_ATTRIBUTE = 'item'

    def __init__(self, table_name: str, prefix: Optional[str], dynamodb_opts: Mapping[str, Any]):
        if not have_aioboto3:
            raise NotImplementedError("Cannot use async DynamoDB feature store because aioboto3 package is not installed")
        self._table_name = table_name
        self._prefix = (prefix + ":") if prefix else ""
        self._dynamodb_opts = dict(dynamodb_opts)
        self._session = aioboto3.Session()
        self._exit_stack = AsyncExitStack()
        self._client: Optional[Any] = None
        self._closed = False
        # Guards lazy client creation and close so they cannot interleave: a
        # close must not race a client that is still being created.
        self._client_lock = asyncio.Lock()

    async def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        async with self._client_lock:
            if self._closed:
                raise RuntimeError("DynamoDB feature store is closed")
            if self._client is None:
                self._client = await self._exit_stack.enter_async_context(self._session.client('dynamodb', **self._dynamodb_opts))
        return self._client

    async def is_available(self) -> bool:
        try:
            inited_key = self._inited_key()
            client = await self._get_client()
            await self._get_item_by_keys(client, inited_key, inited_key)
            return True
        except BaseException:
            return False

    async def init_internal(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        client = await self._get_client()
        # Start by reading the existing keys; we will later delete any of these that were not in all_data.
        unused_old_keys = await self._read_existing_keys(client, all_data.keys())
        requests = []
        num_items = 0
        inited_key = self._inited_key()

        # Insert or update every provided item
        for kind, items in all_data.items():
            for key, item in items.items():
                encoded_item = self._marshal_item(kind, item)
                requests.append({'PutRequest': {'Item': encoded_item}})
                combined_key = (self._namespace_for_kind(kind), key)
                unused_old_keys.discard(combined_key)
                num_items = num_items + 1

        # Now delete any previously existing items whose keys were not in the current data
        for combined_key in unused_old_keys:
            if combined_key[0] != inited_key:
                requests.append({'DeleteRequest': {'Key': self._make_keys(combined_key[0], combined_key[1])}})

        # Now set the special key that we check in initialized_internal()
        requests.append({'PutRequest': {'Item': self._make_keys(inited_key, inited_key)}})

        await _AsyncDynamoDBHelpers.batch_write_requests(client, self._table_name, requests)
        log.info('Initialized table %s with %d items', self._table_name, num_items)

    async def get_internal(self, kind: VersionedDataKind, key: str) -> Optional[dict]:
        client = await self._get_client()
        resp = await self._get_item_by_keys(client, self._namespace_for_kind(kind), key)
        return self._unmarshal_item(resp.get('Item'))

    async def get_all_internal(self, kind: VersionedDataKind) -> Mapping[str, dict]:
        client = await self._get_client()
        items_out = {}
        paginator = client.get_paginator('query')
        async for resp in paginator.paginate(**self._make_query_for_kind(kind)):
            for item in resp['Items']:
                # Every stored item carries the JSON attribute, so _unmarshal_item never returns None here.
                item_out = cast(dict, self._unmarshal_item(item))
                items_out[item_out['key']] = item_out
        return items_out

    async def upsert_internal(self, kind: VersionedDataKind, item: dict) -> dict:
        client = await self._get_client()
        encoded_item = self._marshal_item(kind, item)
        try:
            req = {
                'TableName': self._table_name,
                'Item': encoded_item,
                'ConditionExpression': 'attribute_not_exists(#namespace) or attribute_not_exists(#key) or :version > #version',
                'ExpressionAttributeNames': {'#namespace': self.PARTITION_KEY, '#key': self.SORT_KEY, '#version': self.VERSION_ATTRIBUTE},
                'ExpressionAttributeValues': {':version': {'N': str(item['version'])}},
            }
            await client.put_item(**req)
        except client.exceptions.ConditionalCheckFailedException:
            # The item was not updated because there's a newer item in the database. We must now
            # read the item that's in the database and return it, so the wrapper can cache it.
            return cast(dict, await self.get_internal(kind, item['key']))
        return item

    async def initialized_internal(self) -> bool:
        client = await self._get_client()
        resp = await self._get_item_by_keys(client, self._inited_key(), self._inited_key())
        return resp.get('Item') is not None and len(resp['Item']) > 0

    async def close(self) -> None:
        async with self._client_lock:
            self._closed = True
            await self._exit_stack.aclose()
            self._client = None

    def describe_configuration(self, config) -> str:
        return 'DynamoDB'

    def _prefixed_namespace(self, base: str) -> str:
        return self._prefix + base

    def _namespace_for_kind(self, kind: VersionedDataKind) -> str:
        return self._prefixed_namespace(kind.namespace)

    def _inited_key(self) -> str:
        return self._prefixed_namespace('$inited')

    def _make_keys(self, namespace: str, key: str) -> dict:
        return {self.PARTITION_KEY: {'S': namespace}, self.SORT_KEY: {'S': key}}

    def _make_query_for_kind(self, kind: VersionedDataKind) -> dict:
        return {
            'TableName': self._table_name,
            'ConsistentRead': True,
            'KeyConditions': {self.PARTITION_KEY: {'AttributeValueList': [{'S': self._namespace_for_kind(kind)}], 'ComparisonOperator': 'EQ'}},
        }

    async def _get_item_by_keys(self, client: Any, namespace: str, key: str) -> dict:
        return await client.get_item(TableName=self._table_name, Key=self._make_keys(namespace, key))

    async def _read_existing_keys(self, client: Any, kinds) -> set:
        keys: set = set()
        for kind in kinds:
            req = self._make_query_for_kind(kind)
            req['ProjectionExpression'] = '#namespace, #key'
            req['ExpressionAttributeNames'] = {'#namespace': self.PARTITION_KEY, '#key': self.SORT_KEY}
            paginator = client.get_paginator('query')
            async for resp in paginator.paginate(**req):
                for item in resp['Items']:
                    namespace = item[self.PARTITION_KEY]['S']
                    key = item[self.SORT_KEY]['S']
                    keys.add((namespace, key))
        return keys

    def _marshal_item(self, kind: VersionedDataKind, item: dict) -> dict:
        json_str = json.dumps(item)
        ret = self._make_keys(self._namespace_for_kind(kind), item['key'])
        ret[self.VERSION_ATTRIBUTE] = {'N': str(item['version'])}
        ret[self.ITEM_JSON_ATTRIBUTE] = {'S': json_str}
        return ret

    def _unmarshal_item(self, item: Optional[dict]) -> Optional[dict]:
        if item is None:
            return None
        json_attr = item.get(self.ITEM_JSON_ATTRIBUTE)
        return None if json_attr is None else json.loads(json_attr['S'])


class _AsyncDynamoDBHelpers:
    @staticmethod
    async def batch_write_requests(client: Any, table_name: str, requests: list) -> None:
        batch_size = 25
        for batch in (requests[i: i + batch_size] for i in range(0, len(requests), batch_size)):
            await client.batch_write_item(RequestItems={table_name: batch})
