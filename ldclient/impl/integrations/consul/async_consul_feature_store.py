import json
from typing import Any, Dict, Mapping, Optional

from ldclient import log
from ldclient.interfaces import AsyncFeatureStoreCore, DiagnosticDescription
from ldclient.versioned_data_kind import VersionedDataKind

have_async_consul = False
try:
    import consul.aio

    have_async_consul = True
except ImportError:
    pass

#
# Internal implementation of the async Consul feature store.
#
# It uses the same Consul KV layout as the synchronous Consul feature store, so an async and a
# synchronous SDK can share one Consul instance.
#
# Implementation notes:
#
# * Feature flags, segments, and any other kind of entity the LaunchDarkly client may wish
# to store, are stored as individual items with the key "{prefix}/features/{flag-key}",
# "{prefix}/segments/{segment-key}", etc.
#
# * The special key "{prefix}/$inited" indicates that the store contains a complete data set.
#
# * Since Consul has limited support for transactions (they can't contain more than 64
# operations), the init method-- which replaces the entire data store-- is not guaranteed to
# be atomic, so there can be a race condition if another process is adding new data via
# Upsert. To minimize this, we don't delete all the data at the start; instead, we update
# the items we've received, and then delete all other items. That could potentially result in
# deleting new data from another process, but that would be the case anyway if the Init
# happened to execute later than the Upsert; we are relying on the fact that normally the
# process that did the Init will also receive the new data shortly and do its own Upsert.
#


class _AsyncConsulFeatureStoreCore(DiagnosticDescription, AsyncFeatureStoreCore):
    """Async Consul implementation of :class:`ldclient.interfaces.AsyncFeatureStoreCore`.

    It stores data in the same Consul KV layout as the synchronous Consul feature store, so an async
    and a synchronous SDK can share one Consul instance.
    """

    def __init__(self, host: Optional[str], port: Optional[int], prefix: Optional[str], consul_opts: Optional[dict]):
        if not have_async_consul:
            raise NotImplementedError("Cannot use async Consul feature store because the py-consul package is not installed")
        opts = dict(consul_opts or {})
        if host is not None:
            opts['host'] = host
        if port is not None:
            opts['port'] = port
        self._opts = opts
        self._prefix = ("launchdarkly" if prefix is None else prefix) + "/"
        self._client: Optional[Any] = None

    def _get_client(self):
        # py-consul's asyncio client builds its aiohttp session when it is constructed, which needs a
        # running event loop. We create the client lazily on the first store operation so the factory
        # method can be called synchronously while building configuration.
        if self._client is None:
            self._client = consul.aio.Consul(**self._opts)
        return self._client

    async def is_available(self) -> bool:
        try:
            await self._get_client().kv.get(self._inited_key())
            return True
        except BaseException:
            return False

    async def init_internal(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        client = self._get_client()

        # Start by reading the existing keys; we will later delete any of these that weren't in all_data.
        index, keys = await client.kv.get(self._prefix, recurse=True, keys=True)
        unused_old_keys = set(keys or [])

        num_items = 0
        inited_key = self._inited_key()
        unused_old_keys.discard(inited_key)

        # Insert or update every provided item. Note that this Consul client doesn't support batch
        # operations (the "txn" method), so we'll write them one at a time.
        for kind, items in all_data.items():
            for key, item in items.items():
                encoded_item = json.dumps(item)
                db_key = self._item_key(kind, item['key'])
                await client.kv.put(db_key, encoded_item)
                unused_old_keys.discard(db_key)
                num_items = num_items + 1

        # Now delete any previously existing items whose keys were not in the current data
        for key in unused_old_keys:
            await client.kv.delete(key)

        # Now set the special key that we check in initialized_internal()
        await client.kv.put(inited_key, "")

        log.info('Initialized async Consul store with %d items', num_items)

    async def get_internal(self, kind: VersionedDataKind, key: str) -> Optional[dict]:
        index, resp = await self._get_client().kv.get(self._item_key(kind, key))
        return None if resp is None else json.loads(resp['Value'].decode('utf-8'))

    async def get_all_internal(self, kind: VersionedDataKind) -> Mapping[str, dict]:
        items_out: Dict[str, dict] = {}
        index, results = await self._get_client().kv.get(self._kind_key(kind), recurse=True)
        for result in results or []:
            item = json.loads(result['Value'].decode('utf-8'))
            items_out[item['key']] = item
        return items_out

    async def upsert_internal(self, kind: VersionedDataKind, new_item: dict) -> dict:
        client = self._get_client()
        key = self._item_key(kind, new_item['key'])
        encoded_item = json.dumps(new_item)

        # We will potentially keep retrying indefinitely until someone's write succeeds
        while True:
            index, old_value = await client.kv.get(key)
            if old_value is None:
                mod_index = 0
            else:
                old_item = json.loads(old_value['Value'].decode('utf-8'))
                # Check whether the item is stale. If so, don't do the update (and return the existing item to
                # AsyncCachingStoreWrapper so it can be cached)
                if old_item['version'] >= new_item['version']:
                    return old_item
                mod_index = old_value['ModifyIndex']

            # Otherwise, try to write. We will do a compare-and-set operation, so the write will only succeed if
            # the key's ModifyIndex is still equal to the previous value. If the previous ModifyIndex was zero,
            # it means the key did not previously exist and the write will only succeed if it still doesn't exist.
            success = await client.kv.put(key, encoded_item, cas=mod_index)
            if success:
                return new_item

            log.debug('Concurrent modification detected, retrying')

    async def initialized_internal(self) -> bool:
        index, resp = await self._get_client().kv.get(self._inited_key())
        return resp is not None

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()

    def describe_configuration(self, config) -> str:
        return 'Consul'

    def _kind_key(self, kind: VersionedDataKind) -> str:
        return self._prefix + kind.namespace

    def _item_key(self, kind: VersionedDataKind, key: str) -> str:
        return self._kind_key(kind) + '/' + key

    def _inited_key(self) -> str:
        return self._prefix + '$inited'
