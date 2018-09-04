import json
from pprint import pprint

import redis

from ldclient import log
from ldclient.expiringdict import ExpiringDict
from ldclient.interfaces import FeatureStore
from ldclient.memoized_value import MemoizedValue
from ldclient.versioned_data_kind import FEATURES


class ForgetfulDict(dict):
    def __setitem__(self, key, value):
        pass


class RedisFeatureStore(FeatureStore):
    def __init__(self,
                 url='redis://localhost:6379/0',
                 prefix='launchdarkly',
                 max_connections=16,
                 expiration=15,
                 capacity=1000):

        self._prefix = prefix
        self._cache = ForgetfulDict() if expiration == 0 else ExpiringDict(max_len=capacity,
                                                                           max_age_seconds=expiration)
        self._pool = redis.ConnectionPool.from_url(url=url, max_connections=max_connections)
        self._inited = MemoizedValue(lambda: self._query_init())
        log.info("Started RedisFeatureStore connected to URL: " + url + " using prefix: " + prefix)

    def _items_key(self, kind):
        return "{0}:{1}".format(self._prefix, kind.namespace)

    def _cache_key(self, kind, key):
        return "{0}:{1}".format(kind.namespace, key)

    def init(self, all_data):
        pipe = redis.Redis(connection_pool=self._pool).pipeline()
        
        self._cache.clear()
        all_count = 0

        for kind, items in all_data.items():
            base_key = self._items_key(kind)
            pipe.delete(base_key)
            for key, item in items.items():
                item_json = json.dumps(item)
                pipe.hset(base_key, key, item_json)
                self._cache[self._cache_key(kind, key)] = item
            all_count = all_count + len(items)
        try:
            pipe.execute()
        except:
            self._cache.clear()
            raise
        log.info("Initialized RedisFeatureStore with %d items", all_count)
        self._inited.set(True)

    def all(self, kind, callback):
        r = redis.Redis(connection_pool=self._pool)
        try:
            all_items = r.hgetall(self._items_key(kind))
        except BaseException as e:
            log.error("RedisFeatureStore: Could not retrieve '%s' from Redis with error: %s. Returning None.",
                kind.namespace, e)
            return callback(None)

        if all_items is None or all_items is "":
            log.warn("RedisFeatureStore: call to get all '%s' returned no results. Returning None.", kind.namespace)
            return callback(None)

        results = {}
        for key, item_json in all_items.items():
            key = key.decode('utf-8')  # necessary in Python 3
            item = json.loads(item_json.decode('utf-8'))
            if item.get('deleted', False) is False:
                results[key] = item
        return callback(results)

    def get(self, kind, key, callback=lambda x: x):
        item = self._get_even_if_deleted(kind, key, check_cache=True)
        if item is not None and item.get('deleted', False) is True:
            log.debug("RedisFeatureStore: get returned deleted item %s in '%s'. Returning None.", key, kind.namespace)
            return callback(None)
        return callback(item)

    def _get_even_if_deleted(self, kind, key, check_cache = True):
        cacheKey = self._cache_key(kind, key)
        if check_cache:
            item = self._cache.get(cacheKey)
            if item is not None:
                # reset ttl
                self._cache[cacheKey] = item
                return item

        try:
            r = redis.Redis(connection_pool=self._pool)
            item_json = r.hget(self._items_key(kind), key)
        except BaseException as e:
            log.error("RedisFeatureStore: Could not retrieve key %s from '%s' with error: %s",
                key, kind.namespace, e)
            return None

        if item_json is None or item_json is "":
            log.debug("RedisFeatureStore: key %s not found in '%s'. Returning None.", key, kind.namespace)
            return None

        item = json.loads(item_json.decode('utf-8'))
        self._cache[cacheKey] = item
        return item

    def delete(self, kind, key, version):
        deleted_item = { "key": key, "version": version, "deleted": True }
        self._update_with_versioning(kind, deleted_item)

    def upsert(self, kind, item):
        self._update_with_versioning(kind, item)

    @property
    def initialized(self):
        return self._inited.get()

    def _query_init(self):
        r = redis.Redis(connection_pool=self._pool)
        return r.exists(self._items_key(FEATURES))

    def _update_with_versioning(self, kind, item):
        r = redis.Redis(connection_pool=self._pool)
        base_key = self._items_key(kind)
        key = item['key']
        item_json = json.dumps(item)

        while True:
            pipeline = r.pipeline()
            pipeline.watch(base_key)
            old = self._get_even_if_deleted(kind, key, check_cache=False)
            self._before_update_transaction(base_key, key)
            if old and old['version'] >= item['version']:
                log.debug('RedisFeatureStore: Attempted to %s key: %s version %d with a version that is the same or older: %d in "%s"',
                    'delete' if item.get('deleted') else 'update',
                    key, old['version'], item['version'], kind.namespace)
                pipeline.unwatch()
                break
            else:
                pipeline.multi()
                pipeline.hset(base_key, key, item_json)
                try:
                    pipeline.execute()
                    # Unlike Redis implementations for other platforms, in redis-py a failed WATCH
                    # produces an exception rather than a null result from execute().
                except redis.exceptions.WatchError:
                    log.debug("RedisFeatureStore: concurrent modification detected, retrying")
                    continue
            self._cache[self._cache_key(kind, key)] = item
            break

    def _before_update_transaction(self, base_key, key):
        # exposed for testing
        pass
