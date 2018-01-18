from abc import ABCMeta, abstractmethod

from ldclient.util import log
from ldclient.interfaces import FeatureStore, SegmentStore
from ldclient.rwlock import ReadWriteLock


class InMemoryStoreBase(object):
    """
    Abstract base class for in-memory data stores.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        self._lock = ReadWriteLock()
        self._initialized = False
        self._items = {}

    def get(self, key, callback):
        try:
            self._lock.rlock()
            item = self._items.get(key)
            if item is None:
                log.debug("Attempted to get missing %s: %s, returning None", self.item_name(), key)
                return callback(None)
            if 'deleted' in item and item['deleted']:
                log.debug("Attempted to get deleted %s: %s, returning None", self.item_name(), key)
                return callback(None)
            return callback(item)
        finally:
            self._lock.runlock()

    def all(self, callback):
        try:
            self._lock.rlock()
            return callback(dict((k, i) for k, i in self._items.items() if ('deleted' not in i) or not i['deleted']))
        finally:
            self._lock.runlock()

    def init(self, items):
        try:
            self._lock.lock()
            self._items = dict(items)
            self._initialized = True
            log.debug("Initialized %s store with %d items", self.item_name(), len(items))
        finally:
            self._lock.unlock()

    # noinspection PyShadowingNames
    def delete(self, key, version):
        try:
            self._lock.lock()
            i = self._items.get(key)
            if i is not None and i['version'] < version:
                i['deleted'] = True
                i['version'] = version
            elif i is None:
                i = {'deleted': True, 'version': version}
                self._items[key] = i
        finally:
            self._lock.unlock()

    def upsert(self, key, item):
        try:
            self._lock.lock()
            i = self._items.get(key)
            if i is None or i['version'] < item['version']:
                self._items[key] = item
                log.debug("Updated %s %s to version %d", self.item_name(), key, item['version'])
        finally:
            self._lock.unlock()

    @property
    def initialized(self):
        try:
            self._lock.rlock()
            return self._initialized
        finally:
            self._lock.runlock()

    @abstractmethod
    def item_name(self):
        """
        Returns a description of the kind of item held in this store (feature or segment).
        """


class InMemoryFeatureStore(InMemoryStoreBase, FeatureStore):
    def __init__(self):
        InMemoryStoreBase.__init__(self)

    def item_name(self):
        return 'feature'


class InMemorySegmentStore(InMemoryStoreBase, SegmentStore):
    def __init__(self):
        InMemoryStoreBase.__init__(self)

    def item_name(self):
        return 'segment'
