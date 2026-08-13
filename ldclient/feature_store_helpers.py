"""
This submodule contains support code for writing feature store implementations.
"""

from typing import Any, Dict, Mapping

from expiringdict import ExpiringDict

from ldclient.feature_store import CacheConfig
from ldclient.impl.util import log
from ldclient.interfaces import (
    DiagnosticDescription,
    FeatureStore,
    FeatureStoreCore
)
from ldclient.versioned_data_kind import VersionedDataKind


def _ensure_encoded(kind, item):
    return item if isinstance(item, dict) else kind.encode(item)


def _is_deleted(item):
    return item is not None and item.get('deleted') is True


class _NoopCache:
    """A cache replacement whose operations are all no-ops.

    Used both when caching is disabled at config time and when the FDv2
    in-memory store has taken over and the persistent-store cache is no
    longer useful. Implements only the subset of the dict-like surface
    that the caching wrappers exercise.
    """

    __slots__ = ()

    def get(self, key, default=None):
        return default

    def __setitem__(self, key, value):
        pass

    def pop(self, key, default=None):
        return default

    def clear(self):
        pass


_NOOP_CACHE = _NoopCache()


class _CachingStoreWrapperBase:
    """Provides common cache methods for a feature store wrapper. Subclass it to reuse the cache
    setup, the sans-I/O cache bookkeeping over ``self._cache``, and the shared non-I/O helpers.
    """

    _cache: Any
    _inited: bool
    _has_available_method: bool

    def __init__(self, cache_config: CacheConfig):
        """Sets up the cache from the caching parameters.

        :param cache_config: the caching parameters
        """
        if cache_config.enabled:
            self._cache = ExpiringDict(max_len=cache_config.capacity, max_age_seconds=cache_config.expiration)
        else:
            self._cache = _NOOP_CACHE
        self._inited = False

    def is_monitoring_enabled(self) -> bool:
        return self._has_available_method

    def disable_cache(self) -> None:
        """Replace the in-memory cache with a no-op so further operations don't populate it.

        Called by the FDv2 store coordinator once the in-memory store has become the
        source of truth and the persistent-store cache is no longer useful. Safe to
        call multiple times. Internal -- not part of the public API.
        """
        cache = self._cache
        if cache is _NOOP_CACHE:
            return
        self._cache = _NOOP_CACHE  # readers from this point forward see the no-op
        try:
            cache.clear()  # release the entries the old dict was holding
        except Exception as e:
            log.warning("Error clearing persistent store cache: %s", e)
        log.debug("Persistent store cache replaced with no-op; in-memory store is now active")

    # The methods below hold the cache logic that both wrappers share. They do no I/O; each one is
    # the pre-work or post-work that surrounds a single core call in a wrapper method.

    def _cache_get_item(self, kind, key):
        """Looks up a single item in the cache.

        Returns a ``(hit, value)`` pair. ``hit`` is True if the item was in the cache. ``value`` is
        the item to return, which is None if the cached entry is missing or deleted.
        """
        cached_item = self._cache.get(self._item_cache_key(kind, key))
        # note, cached items are wrapped in an array so we can cache None values
        if cached_item is None:
            return (False, None)
        item = cached_item[0]
        return (True, None if _is_deleted(item) else item)

    def _cache_put_item(self, kind, key, encoded_item):
        """Decodes an item fetched from the core, caches it, and returns the value to return.

        The returned value is None if the item is missing or deleted.
        """
        item = None if encoded_item is None else kind.decode(encoded_item)
        self._cache[self._item_cache_key(kind, key)] = [item]
        return None if _is_deleted(item) else item

    def _cache_get_all(self, kind):
        """Looks up the full set of items of a kind in the cache.

        Returns a ``(hit, value)`` pair. ``hit`` is True if the set was in the cache, in which case
        ``value`` is the cached dict of items.
        """
        cached_items = self._cache.get(self._all_cache_key(kind))
        if cached_items is None:
            return (False, None)
        return (True, cached_items)

    def _cache_put_all(self, kind, encoded_items):
        """Decodes all items fetched from the core, drops deleted ones, caches the result, and returns it."""
        all_items = {}
        if encoded_items is not None:
            for key, item in encoded_items.items():
                all_items[key] = kind.decode(item)
        items = self._items_if_not_deleted(all_items)
        self._cache[self._all_cache_key(kind)] = items
        return items

    def _cache_init(self, all_encoded_data):
        """Populates the cache from a full data set. Does nothing when caching is off (a no-op cache)."""
        cache = self._cache
        if cache is _NOOP_CACHE:
            return
        cache.clear()
        for kind, items in all_encoded_data.items():
            decoded_items = {}  # we cache FeatureFlags/Segments, not raw dicts
            for key, item in items.items():
                decoded_item = kind.decode(item)
                cache[self._item_cache_key(kind, key)] = [decoded_item]  # note array wrapper
                if not _is_deleted(decoded_item):
                    decoded_items[key] = decoded_item
            cache[self._all_cache_key(kind)] = decoded_items

    def _cache_put_upsert(self, kind, new_state):
        """Updates the cache after an upsert.

        Caches the item the core returned and drops the now-stale all-items entry. Returns the
        decoded item.
        """
        new_decoded_item = kind.decode(new_state)
        self._cache[self._item_cache_key(kind, new_decoded_item.get('key'))] = [new_decoded_item]
        self._cache.pop(self._all_cache_key(kind), None)
        return new_decoded_item

    @staticmethod
    def _item_cache_key(kind, key):
        return "{0}:{1}".format(kind.namespace, key)

    @staticmethod
    def _all_cache_key(kind):
        return kind.namespace

    @staticmethod
    def _items_if_not_deleted(items):
        results = {}
        if items is not None:
            for key, item in items.items():
                if not item.get('deleted', False):
                    results[key] = item
        return results


class CachingStoreWrapper(_CachingStoreWrapperBase, DiagnosticDescription, FeatureStore):
    """A partial implementation of :class:`ldclient.interfaces.FeatureStore`.

    This class delegates the basic functionality to an implementation of
    :class:`ldclient.interfaces.FeatureStoreCore` - while adding optional caching behavior and other logic
    that would otherwise be repeated in every feature store implementation. This makes it easier to create
    new database integrations by implementing only the database-specific logic.
    """

    __INITED_CACHE_KEY__ = "$inited"

    _core: FeatureStoreCore

    def __init__(self, core: FeatureStoreCore, cache_config: CacheConfig):
        """Constructs an instance by wrapping a core implementation object.

        :param core: the implementation object
        :param cache_config: the caching parameters
        """
        self._core = core
        self._has_available_method = callable(getattr(core, 'is_available', None))
        super().__init__(cache_config)

    def is_available(self) -> bool:
        # We know is_available exists since we are checking _has_available_method
        return self._core.is_available() if self._has_available_method else False  # type: ignore

    def init(self, all_encoded_data: Mapping[VersionedDataKind, Mapping[str, Dict[Any, Any]]]):
        """ """
        self._core.init_internal(all_encoded_data)  # currently FeatureStoreCore expects to receive dicts
        self._cache_init(all_encoded_data)
        self._inited = True

    def get(self, kind, key, callback=lambda x: x):
        """ """
        hit, value = self._cache_get_item(kind, key)
        if hit:
            return callback(value)
        encoded_item = self._core.get_internal(kind, key)  # currently FeatureStoreCore returns dicts
        return callback(self._cache_put_item(kind, key, encoded_item))

    def all(self, kind, callback=lambda x: x):
        """ """
        hit, value = self._cache_get_all(kind)
        if hit:
            return callback(value)
        encoded_items = self._core.get_all_internal(kind)
        return callback(self._cache_put_all(kind, encoded_items))

    def delete(self, kind, key, version):
        """ """
        deleted_item = {"key": key, "version": version, "deleted": True}
        self.upsert(kind, deleted_item)

    def upsert(self, kind, encoded_item):
        """ """
        encoded_item = _ensure_encoded(kind, encoded_item)
        new_state = self._core.upsert_internal(kind, encoded_item)
        self._cache_put_upsert(kind, new_state)

    @property
    def initialized(self) -> bool:
        """ """
        if self._inited:
            return True
        result = self._cache.get(CachingStoreWrapper.__INITED_CACHE_KEY__)
        if result is None:
            result = bool(self._core.initialized_internal())
            self._cache[CachingStoreWrapper.__INITED_CACHE_KEY__] = result
        if result:
            self._inited = True
        return result

    def close(self) -> None:
        """Release the cache and close the underlying core if it supports it."""
        self.disable_cache()
        if hasattr(self._core, "close"):
            self._core.close()  # type: ignore

    def describe_configuration(self, config):
        describe = getattr(self._core, 'describe_configuration', None)
        if callable(describe):
            return describe(config)
        return "custom"
