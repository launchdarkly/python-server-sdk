from collections import defaultdict
from ldclient.util import log
from ldclient.interfaces import FeatureStore
from ldclient.rwlock import ReadWriteLock


class CacheConfig:
    """Encapsulates caching parameters for feature store implementations that support local caching.
    """

    DEFAULT_EXPIRATION = 15
    DEFAULT_CAPACITY = 1000

    def __init__(self,
                 expiration = DEFAULT_EXPIRATION,
                 capacity = DEFAULT_CAPACITY):
        """Constructs an instance of CacheConfig.
        :param float expiration: The cache TTL, in seconds. Items will be evicted from the cache after
          this amount of time from the time when they were originally cached. If the time is less than or
          equal to zero, caching is disabled.
        :param int capacity: The maximum number of items that can be in the cache at a time.
        """
        self._expiration = expiration
        self._capacity = capacity

    @staticmethod
    def default():
        """Returns an instance of CacheConfig with default properties. By default, caching is enabled.
        This is the same as calling the constructor with no parameters.
        :rtype: CacheConfig
        """
        return CacheConfig()
    
    @staticmethod
    def disabled():
        """Returns an instance of CacheConfig specifying that caching should be disabled.
        :rtype: CacheConfig
        """
        return CacheConfig(expiration = 0)
    
    @property
    def enabled(self):
        return self._expiration > 0
    
    @property
    def expiration(self):
        return self._expiration
    
    @property
    def capacity(self):
        return self._capacity


class InMemoryFeatureStore(FeatureStore):
    """
    In-memory implementation of a store that holds feature flags and related data received from the streaming API.
    """

    def __init__(self):
        self._lock = ReadWriteLock()
        self._initialized = False
        self._items = defaultdict(dict)

    def get(self, kind, key, callback):
        try:
            self._lock.rlock()
            itemsOfKind = self._items[kind]
            item = itemsOfKind.get(key)
            if item is None:
                log.debug("Attempted to get missing key %s in '%s', returning None", key, kind.namespace)
                return callback(None)
            if 'deleted' in item and item['deleted']:
                log.debug("Attempted to get deleted key %s in '%s', returning None", key, kind.namespace)
                return callback(None)
            return callback(item)
        finally:
            self._lock.runlock()

    def all(self, kind, callback):
        try:
            self._lock.rlock()
            itemsOfKind = self._items[kind]
            return callback(dict((k, i) for k, i in itemsOfKind.items() if ('deleted' not in i) or not i['deleted']))
        finally:
            self._lock.runlock()

    def init(self, all_data):
        try:
            self._lock.rlock()
            self._items.clear()
            self._items.update(all_data)
            self._initialized = True
            for k in all_data:
                log.debug("Initialized '%s' store with %d items", k.namespace, len(all_data[k]))
        finally:
            self._lock.runlock()

    # noinspection PyShadowingNames
    def delete(self, kind, key, version):
        try:
            self._lock.rlock()
            itemsOfKind = self._items[kind]
            i = itemsOfKind.get(key)
            if i is None or i['version'] < version:
                i = {'deleted': True, 'version': version}
                itemsOfKind[key] = i
        finally:
            self._lock.runlock()

    def upsert(self, kind, item):
        key = item['key']
        try:
            self._lock.rlock()
            itemsOfKind = self._items[kind]
            i = itemsOfKind.get(key)
            if i is None or i['version'] < item['version']:
                itemsOfKind[key] = item
                log.debug("Updated %s in '%s' to version %d", key, kind.namespace, item['version'])
        finally:
            self._lock.runlock()

    @property
    def initialized(self):
        try:
            self._lock.rlock()
            return self._initialized
        finally:
            self._lock.runlock()
