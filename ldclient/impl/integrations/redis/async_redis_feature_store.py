import json
from typing import Any, Callable, Dict, Mapping, Optional

from ldclient.feature_store_helpers import CachingStoreWrapper
from ldclient.impl.util import log, redact_password
from ldclient.interfaces import AsyncFeatureStoreCore, DiagnosticDescription
from ldclient.versioned_data_kind import VersionedDataKind

have_async_redis = False
try:
    import redis.asyncio as redis_client
    from redis.exceptions import WatchError

    have_async_redis = True
except ImportError:
    pass

# Cap the WATCH-retry loop so a hot-contended key can't starve upsert_internal forever; matches the LaunchDarkly Go Redis stores.
_MAX_UPSERT_RETRIES = 10


class _AsyncRedisFeatureStoreCore(DiagnosticDescription, AsyncFeatureStoreCore):
    """Async Redis implementation of :class:`ldclient.interfaces.AsyncFeatureStoreCore`.

    It stores data in the same Redis key layout as the synchronous Redis feature store, so an async
    and a synchronous SDK can share one Redis instance.
    """

    def __init__(self, url: str, prefix: Optional[str], redis_opts: Dict[str, Any]):
        if not have_async_redis:
            raise NotImplementedError("Cannot use async Redis feature store because redis package is not installed")
        self._prefix = prefix or 'launchdarkly'
        self._init_key = "{0}:{1}".format(self._prefix, CachingStoreWrapper.__INITED_CACHE_KEY__)
        self._client = redis_client.from_url(url, **redis_opts)
        self.test_update_hook: Optional[Callable[[str, str], None]] = None  # exposed for testing
        log.info("Started AsyncRedisFeatureStore connected to URL: " + redact_password(url) + " using prefix: " + self._prefix)

    async def is_available(self) -> bool:
        try:
            await self.initialized_internal()
            return True
        except BaseException:
            return False

    def _items_key(self, kind: VersionedDataKind) -> str:
        return "{0}:{1}".format(self._prefix, kind.namespace)

    async def init_internal(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        all_count = 0
        async with self._client.pipeline() as pipe:
            for kind, items in all_data.items():
                base_key = self._items_key(kind)
                pipe.delete(base_key)
                for key, item in items.items():
                    pipe.hset(base_key, key, json.dumps(item))
                all_count = all_count + len(items)
            pipe.set(self._init_key, self._init_key)
            await pipe.execute()
        log.info("Initialized AsyncRedisFeatureStore with %d items", all_count)

    async def get_all_internal(self, kind: VersionedDataKind) -> Mapping[str, dict]:
        all_items = await self._client.hgetall(self._items_key(kind))
        if not all_items:
            return {}
        results = {}
        for key, item_json in all_items.items():
            results[key.decode('utf-8')] = json.loads(item_json.decode('utf-8'))
        return results

    async def get_internal(self, kind: VersionedDataKind, key: str) -> Optional[dict]:
        item_json = await self._client.hget(self._items_key(kind), key)
        if not item_json:
            log.debug("AsyncRedisFeatureStore: key %s not found in '%s'. Returning None.", key, kind.namespace)
            return None
        return json.loads(item_json.decode('utf-8'))

    async def upsert_internal(self, kind: VersionedDataKind, item: dict) -> dict:
        base_key = self._items_key(kind)
        key = item['key']
        item_json = json.dumps(item)

        for _ in range(_MAX_UPSERT_RETRIES):
            async with self._client.pipeline() as pipe:
                try:
                    await pipe.watch(base_key)
                    old = await self.get_internal(kind, key)
                    if self.test_update_hook is not None:
                        self.test_update_hook(base_key, key)
                    if old and old['version'] >= item['version']:
                        log.debug(
                            'AsyncRedisFeatureStore: Attempted to %s key: %s version %d with a version that is the same or older: %d in "%s"',
                            'delete' if item.get('deleted') else 'update',
                            key,
                            old['version'],
                            item['version'],
                            kind.namespace,
                        )
                        await pipe.unwatch()
                        return old
                    pipe.multi()
                    pipe.hset(base_key, key, item_json)
                    # A concurrent change to the watched key makes execute() raise WatchError,
                    # rather than returning a null result as on some other platforms.
                    await pipe.execute()
                    return item
                except WatchError:
                    log.debug("AsyncRedisFeatureStore: concurrent modification detected, retrying")
                    continue

        raise RuntimeError("failed to update key %s in '%s' after %d attempts" % (key, kind.namespace, _MAX_UPSERT_RETRIES))

    async def initialized_internal(self) -> bool:
        return bool(await self._client.exists(self._init_key))

    async def close(self) -> None:
        # Prefer aclose() (redis-py 5.0.1+); older supported versions (>= 4.2) only have close().
        if hasattr(self._client, "aclose"):
            await self._client.aclose()
        else:
            await self._client.close()

    def describe_configuration(self, config) -> str:
        return 'Redis'
