from ldclient.util import log
from ldclient.interfaces import FeatureStore, SegmentStore
from ldclient.rwlock import ReadWriteLock


class InMemoryFeatureStore(object):
    """
    In-memory implementation of a store that holds feature flags and related data received from the streaming API.
    """

    def __init__(self):
        self._lock = ReadWriteLock()
        self._initialized = False
        self._items = {}

    def get(self, kind, key, callback):
        try:
            self._lock.rlock()
            itemsOfKind = self._items.get(kind, {})
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
            itemsOfKind = self._items.get(kind, {})
            return callback(dict((k, i) for k, i in itemsOfKind.items() if ('deleted' not in i) or not i['deleted']))
        finally:
            self._lock.runlock()

    def init(self, allData):
        try:
            self._lock.lock()
            self._items = dict(allData)
            self._initialized = True
            for k in allData:
                log.debug("Initialized '%s' store with %d items", k.namespace, len(allData[k]))
        finally:
            self._lock.unlock()

    # noinspection PyShadowingNames
    def delete(self, kind, key, version):
        try:
            self._lock.lock()
            itemsOfKind = self._items.get(kind)
            if itemsOfKind is None:
                itemsOfKind = dict()
                self._items[kind] = itemsOfKind
            i = itemsOfKind.get(key)
            if i is None or i['version'] < version:
                i = {'deleted': True, 'version': version}
                itemsOfKind[key] = i
        finally:
            self._lock.unlock()

    def upsert(self, kind, item):
        key = item['key']
        try:
            self._lock.lock()
            itemsOfKind = self._items.get(kind)
            if itemsOfKind is None:
                itemsOfKind = dict()
                self._items[kind] = itemsOfKind
            i = itemsOfKind.get(key)
            if i is None or i['version'] < item['version']:
                itemsOfKind[key] = item
                log.debug("Updated %s in '%s' to version %d", key, kind.namespace, item['version'])
        finally:
            self._lock.unlock()

    @property
    def initialized(self):
        try:
            self._lock.rlock()
            return self._initialized
        finally:
            self._lock.runlock()
