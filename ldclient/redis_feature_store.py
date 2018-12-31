import json

have_redis = False
try:
    import redis
    have_redis = True
except ImportError:
    pass

from ldclient import log
from ldclient.feature_store import CacheConfig
from ldclient.feature_store_helpers import CachingStoreWrapper
from ldclient.interfaces import FeatureStore, FeatureStoreCore
from ldclient.versioned_data_kind import FEATURES


# Note that this class is now just a facade around CachingStoreWrapper, which is in turn delegating
# to _RedisFeatureStoreCore where the actual database logic is. This class was retained for historical
# reasons, to support existing code that calls the RedisFeatureStore constructor. In the future, we
# will migrate away from exposing these concrete classes and use only the factory methods.

class RedisFeatureStore(FeatureStore):
    """A Redis-backed implementation of :class:`ldclient.feature_store.FeatureStore`.

    This implementation class is deprecated and may be changed or removed in the future. Please use
    :func:`ldclient.integrations.Redis.new_feature_store()`.
    """
    def __init__(self,
                 url='redis://localhost:6379/0',
                 prefix='launchdarkly',
                 max_connections=16,
                 expiration=15,
                 capacity=1000):
        if not have_redis:
            raise NotImplementedError("Cannot use Redis feature store because redis package is not installed")
        self.core = _RedisFeatureStoreCore(url, prefix, max_connections)  # exposed for testing
        self._wrapper = CachingStoreWrapper(self.core, CacheConfig(expiration=expiration, capacity=capacity))

    def get(self, kind, key, callback = lambda x: x):
        return self._wrapper.get(kind, key, callback)
    
    def all(self, kind, callback):
        return self._wrapper.all(kind, callback)
    
    def init(self, all_data):
        return self._wrapper.init(all_data)
    
    def upsert(self, kind, item):
        return self._wrapper.upsert(kind, item)
    
    def delete(self, kind, key, version):
        return self._wrapper.delete(kind, key, version)
    
    @property
    def initialized(self):
        return self._wrapper.initialized


class _RedisFeatureStoreCore(FeatureStoreCore):
    def __init__(self, url, prefix, max_connections):
        
        self._prefix = prefix
        self._pool = redis.ConnectionPool.from_url(url=url, max_connections=max_connections)
        self.test_update_hook = None  # exposed for testing
        log.info("Started RedisFeatureStore connected to URL: " + url + " using prefix: " + prefix)

    def _items_key(self, kind):
        return "{0}:{1}".format(self._prefix, kind.namespace)

    def init_internal(self, all_data):
        pipe = redis.Redis(connection_pool=self._pool).pipeline()
        
        all_count = 0

        for kind, items in all_data.items():
            base_key = self._items_key(kind)
            pipe.delete(base_key)
            for key, item in items.items():
                item_json = json.dumps(item)
                pipe.hset(base_key, key, item_json)
            all_count = all_count + len(items)
        pipe.execute()
        log.info("Initialized RedisFeatureStore with %d items", all_count)

    def get_all_internal(self, kind):
        r = redis.Redis(connection_pool=self._pool)
        all_items = r.hgetall(self._items_key(kind))

        if all_items is None or all_items is "":
            all_items = {}

        results = {}
        for key, item_json in all_items.items():
            key = key.decode('utf-8')  # necessary in Python 3
            results[key] = json.loads(item_json.decode('utf-8'))
        return results

    def get_internal(self, kind, key):
        r = redis.Redis(connection_pool=self._pool)
        item_json = r.hget(self._items_key(kind), key)

        if item_json is None or item_json is "":
            log.debug("RedisFeatureStore: key %s not found in '%s'. Returning None.", key, kind.namespace)
            return None

        return json.loads(item_json.decode('utf-8'))

    def upsert_internal(self, kind, item):
        r = redis.Redis(connection_pool=self._pool)
        base_key = self._items_key(kind)
        key = item['key']
        item_json = json.dumps(item)

        while True:
            pipeline = r.pipeline()
            pipeline.watch(base_key)
            old = self.get_internal(kind, key)
            if self.test_update_hook is not None:
                self.test_update_hook(base_key, key)
            if old and old['version'] >= item['version']:
                log.debug('RedisFeatureStore: Attempted to %s key: %s version %d with a version that is the same or older: %d in "%s"',
                    'delete' if item.get('deleted') else 'update',
                    key, old['version'], item['version'], kind.namespace)
                pipeline.unwatch()
                return old
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
            return item

    def initialized_internal(self):
        r = redis.Redis(connection_pool=self._pool)
        return r.exists(self._items_key(FEATURES))

    def _before_update_transaction(self, base_key, key):
        # exposed for testing
        pass
