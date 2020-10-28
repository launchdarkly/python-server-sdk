"""
This submodule contains basic classes related to the feature store.

The feature store is the SDK component that holds the last known state of all feature flags, as
received from LaunchDarkly. This submodule does not include specific integrations with external
storage systems; those are in :class:`ldclient.integrations`.
"""

from typing import Callable, Any

from collections import OrderedDict, defaultdict
from ldclient.util import log
from ldclient.interfaces import DiagnosticDescription, FeatureStore
from ldclient.rwlock import ReadWriteLock
from ldclient.versioned_data_kind import VersionedDataKind


class CacheConfig:
    """Encapsulates caching parameters for feature store implementations that support local caching.
    """

    DEFAULT_EXPIRATION = 15.0
    DEFAULT_CAPACITY = 1000

    def __init__(self,
                 expiration: float = DEFAULT_EXPIRATION,
                 capacity: int = DEFAULT_CAPACITY):
        """Constructs an instance of CacheConfig.

        :param expiration: the cache TTL, in seconds. Items will be evicted from the cache after
          this amount of time from the time when they were originally cached. If the time is less than or
          equal to zero, caching is disabled.
        :param capacity: the maximum number of items that can be in the cache at a time
        """
        self._expiration = expiration
        self._capacity = capacity

    @staticmethod
    def default() -> 'CacheConfig':
        """Returns an instance of CacheConfig with default properties. By default, caching is enabled.
        This is the same as calling the constructor with no parameters.
        """
        return CacheConfig()

    @staticmethod
    def disabled() -> 'CacheConfig':
        """Returns an instance of CacheConfig specifying that caching should be disabled.
        """
        return CacheConfig(expiration = 0)

    @property
    def enabled(self) -> bool:
        """Returns True if caching is enabled in this configuration.
        """
        return self._expiration > 0

    @property
    def expiration(self) -> float:
        """Returns the configured cache TTL, in seconds.
        """
        return self._expiration

    @property
    def capacity(self) -> int:
        """Returns the configured maximum number of cacheable items.
        """
        return self._capacity


class InMemoryFeatureStore(FeatureStore, DiagnosticDescription):
    """The default feature store implementation, which holds all data in a thread-safe data structure in memory.
    """

    def __init__(self):
        """Constructs an instance of InMemoryFeatureStore.
        """
        self._lock = ReadWriteLock()
        self._initialized = False
        self._items = defaultdict(dict)

    def get(self, kind: VersionedDataKind, key: str, callback: Callable[[Any], Any]=lambda x: x) -> Any:
        """
        """
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
        """
        """
        try:
            self._lock.rlock()
            itemsOfKind = self._items[kind]
            return callback(dict((k, i) for k, i in itemsOfKind.items() if ('deleted' not in i) or not i['deleted']))
        finally:
            self._lock.runlock()

    def init(self, all_data):
        """
        """
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
    def delete(self, kind, key: str, version: int):
        """
        """
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
        """
        """
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
    def initialized(self) -> bool:
        """
        """
        try:
            self._lock.rlock()
            return self._initialized
        finally:
            self._lock.runlock()

    def describe_configuration(self, config):
        return 'memory'


class _FeatureStoreDataSetSorter:
    """
    Implements a dependency graph ordering for data to be stored in a feature store. We must use this
    on every data set that will be passed to the feature store's init() method.
    """
    @staticmethod
    def sort_all_collections(all_data):
        """ Returns a copy of the input data that has the following guarantees: the iteration order of the outer
        dictionary will be in ascending order by the VersionDataKind's :priority property (if any), and for each
        data kind that has a "get_dependency_keys" function, the inner dictionary will have an iteration order
        where B is before A if A has a dependency on B.
        """
        outer_hash = OrderedDict()
        kinds = list(all_data.keys())
        def priority_order(kind):
            if hasattr(kind, 'priority'):
                return kind.priority
            return len(kind.namespace)  # use arbitrary order if there's no priority
        kinds.sort(key=priority_order)
        for kind in kinds:
            items = all_data[kind]
            outer_hash[kind] = _FeatureStoreDataSetSorter._sort_collection(kind, items)
        return outer_hash

    @staticmethod
    def _sort_collection(kind, input):
        if len(input) == 0 or not hasattr(kind, 'get_dependency_keys'):
            return input
        dependency_fn = kind.get_dependency_keys
        if dependency_fn is None or len(input) == 0:
            return input
        remaining_items = input.copy()
        items_out = OrderedDict()
        while len(remaining_items) > 0:
            # pick a random item that hasn't been updated yet
            for key, item in remaining_items.items():
                _FeatureStoreDataSetSorter._add_with_dependencies_first(item, dependency_fn, remaining_items, items_out)
                break
        return items_out

    @staticmethod
    def _add_with_dependencies_first(item, dependency_fn, remaining_items, items_out):
        key = item.get('key')
        del remaining_items[key]  # we won't need to visit this item again
        for dep_key in dependency_fn(item):
            dep_item = remaining_items.get(dep_key)
            if dep_item is not None:
                _FeatureStoreDataSetSorter._add_with_dependencies_first(dep_item, dependency_fn, remaining_items, items_out)
        items_out[key] = item
