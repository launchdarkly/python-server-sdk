"""
This submodule contains support code for writing feature store implementations.
"""

from typing import Dict, Mapping, Any
from expiringdict import ExpiringDict

from ldclient.interfaces import DiagnosticDescription, FeatureStore, FeatureStoreCore
from ldclient.versioned_data_kind import VersionedDataKind
from ldclient.feature_store import CacheConfig

class CachingStoreWrapper(DiagnosticDescription, FeatureStore):
    """A partial implementation of :class:`ldclient.interfaces.FeatureStore`.

    This class delegates the basic functionality to an implementation of
    :class:`ldclient.interfaces.FeatureStoreCore` - while adding optional caching behavior and other logic
    that would otherwise be repeated in every feature store implementation. This makes it easier to create
    new database integrations by implementing only the database-specific logic. 
    """
    __INITED_CACHE_KEY__ = "$inited"

    def __init__(self, core: FeatureStoreCore, cache_config: CacheConfig):
        """Constructs an instance by wrapping a core implementation object.

        :param core: the implementation object
        :param cache_config: the caching parameters
        """
        self._core = core
        if cache_config.enabled:
            self._cache = ExpiringDict(max_len=cache_config.capacity, max_age_seconds=cache_config.expiration)
        else:
            self._cache = None
        self._inited = False

    def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, Dict[Any, Any]]]):
        """
        """
        self._core.init_internal(all_data)
        if self._cache is not None:
            self._cache.clear()
            for kind, items in all_data.items():
                self._cache[self._all_cache_key(kind)] = self._items_if_not_deleted(items)
                for key, item in items.items():
                    self._cache[self._item_cache_key(kind, key)] = [item] # note array wrapper
        self._inited = True

    def get(self, kind, key, callback=lambda x: x):
        """
        """
        if self._cache is not None:
            cache_key = self._item_cache_key(kind, key)
            cached_item = self._cache.get(cache_key)
            # note, cached items are wrapped in an array so we can cache None values
            if cached_item is not None:
                return callback(self._item_if_not_deleted(cached_item[0]))
        item = self._core.get_internal(kind, key)
        if self._cache is not None:
            self._cache[cache_key] = [item]
        return callback(self._item_if_not_deleted(item))

    def all(self, kind, callback=lambda x: x):
        """
        """
        if self._cache is not None:
            cache_key = self._all_cache_key(kind)
            cached_items = self._cache.get(cache_key)
            if cached_items is not None:
                return callback(cached_items)
        items = self._items_if_not_deleted(self._core.get_all_internal(kind))
        if self._cache is not None:
            self._cache[cache_key] = items
        return callback(items)
    
    def delete(self, kind, key, version):
        """
        """
        deleted_item = { "key": key, "version": version, "deleted": True }
        self.upsert(kind, deleted_item)

    def upsert(self, kind, item):
        """
        """
        new_state = self._core.upsert_internal(kind, item)
        if self._cache is not None:
            self._cache[self._item_cache_key(kind, item.get('key'))] = [new_state]
            self._cache.pop(self._all_cache_key(kind), None)

    @property
    def initialized(self) -> bool:
        """
        """
        if self._inited:
            return True
        if self._cache is None:
            result = bool(self._core.initialized_internal())
        else:
            result = self._cache.get(CachingStoreWrapper.__INITED_CACHE_KEY__)
            if result is None:
                result = bool(self._core.initialized_internal())
                self._cache[CachingStoreWrapper.__INITED_CACHE_KEY__] = result
        if result:
            self._inited = True
        return result

    def describe_configuration(self, config):
        if callable(getattr(self._core, 'describe_configuration', None)):
            return self._core.describe_configuration(config)
        return "custom"
    
    @staticmethod
    def _item_cache_key(kind, key):
        return "{0}:{1}".format(kind.namespace, key)

    @staticmethod
    def _all_cache_key(kind):
        return kind.namespace
    
    @staticmethod
    def _item_if_not_deleted(item):
        if item is not None and item.get('deleted', False):
            return None
        return item
    
    @staticmethod
    def _items_if_not_deleted(items):
        results = {}
        if items is not None:
            for key, item in items.items():
                if not item.get('deleted', False):
                    results[key] = item
        return results
    