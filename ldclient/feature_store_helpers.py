"""
This submodule contains support code for writing feature store implementations.
"""

import json
from typing import Any, Dict, Mapping, Optional

from expiringdict import ExpiringDict

from ldclient.feature_store import CacheConfig
from ldclient.impl.util import log
from ldclient.interfaces import (
    DiagnosticDescription,
    FeatureStore,
    FeatureStoreCore,
    SerializedItemDescriptor
)
from ldclient.versioned_data_kind import VersionedDataKind


def _ensure_encoded(kind, item):
    return item if isinstance(item, dict) else kind.encode(item)


def _is_deleted(item):
    return item is not None and item.get('deleted') is True


def _serialize(encoded_item: dict) -> SerializedItemDescriptor:
    """Serializes an encoded item, which can be a deleted item, for a persistent store core."""
    return SerializedItemDescriptor(
        version=encoded_item.get('version', 0),
        deleted=encoded_item.get('deleted') is True,
        serialized_item=json.dumps(encoded_item),
    )


def _serialize_all(all_data) -> Dict[VersionedDataKind, Dict[str, SerializedItemDescriptor]]:
    """Serializes a full data set. The order of the kinds and of the items does not change."""
    return {kind: {key: _serialize(kind.encode(item)) for key, item in items.items()} for kind, items in all_data.items()}


def _deserialize(kind, key: str, descriptor: Optional[SerializedItemDescriptor]):
    """Deserializes an item that a persistent store core returned.

    Returns None if the core found no item. A deleted item becomes a decoded placeholder with the
    deleted flag. If the core gives a version that differs from the version in the serialized item,
    the version from the core is used.
    """
    if descriptor is None:
        return None
    if descriptor.deleted:
        return kind.decode({'key': key, 'version': descriptor.version, 'deleted': True})
    data = json.loads(descriptor.serialized_item)
    if descriptor.version != 0 and data.get('version') != descriptor.version:
        data['version'] = descriptor.version
    return kind.decode(data)


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
        """Returns True. The wrapper reports status monitoring as enabled for all cores."""
        return True

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

    def _cache_put_item(self, kind, key, descriptor):
        """Deserializes an item fetched from the core, caches it, and returns the value to return.

        The returned value is None if the item is missing or deleted.
        """
        item = _deserialize(kind, key, descriptor)
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

    def _cache_put_all(self, kind, descriptors):
        """Deserializes all items fetched from the core, drops deleted ones, caches the result, and returns it."""
        all_items = {}
        if descriptors is not None:
            for key, descriptor in descriptors.items():
                all_items[key] = _deserialize(kind, key, descriptor)
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

    def _cache_put_upsert(self, kind, key, encoded_item):
        """Updates the cache after the core wrote an item, and drops the now-stale all-items entry."""
        self._cache[self._item_cache_key(kind, key)] = [kind.decode(encoded_item)]
        self._cache.pop(self._all_cache_key(kind), None)

    def _caching(self) -> bool:
        return self._cache is not _NOOP_CACHE

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
        super().__init__(cache_config)

    def is_available(self) -> bool:
        """Returns the result of the availability check of the core."""
        return self._core.is_available()

    def init(self, all_encoded_data: Mapping[VersionedDataKind, Mapping[str, Dict[Any, Any]]]):
        """ """
        self._core.init_internal(_serialize_all(all_encoded_data))
        self._cache_init(all_encoded_data)
        self._inited = True

    def get(self, kind, key, callback=lambda x: x):
        """ """
        hit, value = self._cache_get_item(kind, key)
        if hit:
            return callback(value)
        descriptor = self._core.get_internal(kind, key)
        return callback(self._cache_put_item(kind, key, descriptor))

    def all(self, kind, callback=lambda x: x):
        """ """
        hit, value = self._cache_get_all(kind)
        if hit:
            return callback(value)
        descriptors = self._core.get_all_internal(kind)
        return callback(self._cache_put_all(kind, descriptors))

    def delete(self, kind, key, version) -> bool:
        """ """
        deleted_item = {"key": key, "version": version, "deleted": True}
        return self.upsert(kind, deleted_item)

    def upsert(self, kind, encoded_item) -> bool:
        """ """
        encoded_item = _ensure_encoded(kind, encoded_item)
        key = encoded_item['key']
        updated = self._core.upsert_internal(kind, key, _serialize(encoded_item))
        if updated:
            self._cache_put_upsert(kind, key, encoded_item)
        elif self._caching():
            # The store has a newer item. Read it, so that the cache does not keep an older one.
            self._cache_put_item(kind, key, self._core.get_internal(kind, key))
        return updated

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
        """Releases the cache and closes the core."""
        self.disable_cache()
        self._core.close()

    def describe_configuration(self, config):
        describe = getattr(self._core, 'describe_configuration', None)
        if callable(describe):
            return describe(config)
        return "custom"
