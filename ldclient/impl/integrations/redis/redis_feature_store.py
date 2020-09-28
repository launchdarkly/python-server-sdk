import json

have_redis = False
try:
    import redis
    have_redis = True
except ImportError:
    pass

from ldclient import log
from ldclient.interfaces import DiagnosticDescription, FeatureStoreCore
from ldclient.versioned_data_kind import FEATURES


class _RedisFeatureStoreCore(DiagnosticDescription, FeatureStoreCore):
    def __init__(self, url, prefix, max_connections):
        if not have_redis:
            raise NotImplementedError("Cannot use Redis feature store because redis package is not installed")
        self._prefix = prefix or 'launchdarkly'
        self._pool = redis.ConnectionPool.from_url(url=url, max_connections=max_connections)
        self.test_update_hook = None  # exposed for testing
        log.info("Started RedisFeatureStore connected to URL: " + url + " using prefix: " + self._prefix)

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

        if all_items is None or all_items == "":
            all_items = {}

        results = {}
        for key, item_json in all_items.items():
            key = key.decode('utf-8')  # necessary in Python 3
            results[key] = json.loads(item_json.decode('utf-8'))
        return results

    def get_internal(self, kind, key):
        r = redis.Redis(connection_pool=self._pool)
        item_json = r.hget(self._items_key(kind), key)

        if item_json is None or item_json == "":
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

    def describe_configuration(self, config):
        return 'Redis'
    
    def _before_update_transaction(self, base_key, key):
        # exposed for testing
        pass
