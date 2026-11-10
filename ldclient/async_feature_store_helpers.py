"""
This submodule contains support code for writing async feature store implementations.
"""

from typing import Any, Dict, Mapping, Optional

from ldclient.feature_store import CacheConfig
from ldclient.feature_store_helpers import (
    _CachingStoreWrapperBase,
    _ensure_encoded,
    _serialize,
    _serialize_all
)
from ldclient.interfaces import (
    AsyncFeatureStore,
    AsyncFeatureStoreCore,
    DiagnosticDescription
)
from ldclient.versioned_data_kind import VersionedDataKind


class AsyncCachingStoreWrapper(_CachingStoreWrapperBase, DiagnosticDescription, AsyncFeatureStore):
    """A partial implementation of :class:`ldclient.interfaces.AsyncFeatureStore`.

    This class delegates the database-specific work to an implementation of
    :class:`ldclient.interfaces.AsyncFeatureStoreCore`, while adding optional caching behavior and
    other logic that would otherwise be repeated in every async feature store implementation. This
    makes it easier to create new async database integrations by implementing only the
    database-specific logic.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees. Pin to a specific minor version and review the changelog
        before upgrading.

    The cache is a plain in-memory dict, which is safe for concurrent access within a single asyncio
    event loop because its reads and writes never suspend between one another.
    """

    __INITED_CACHE_KEY__ = "$inited"

    _core: AsyncFeatureStoreCore

    def __init__(self, core: AsyncFeatureStoreCore, cache_config: CacheConfig):
        """Constructs an instance by wrapping a core implementation object.

        :param core: the implementation object
        :param cache_config: the caching parameters
        """
        self._core = core
        super().__init__(cache_config)

    async def is_available(self) -> bool:
        """Returns the result of the availability check of the core."""
        return await self._core.is_available()

    async def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        """ """
        await self._core.init_internal(_serialize_all(all_data))
        self._cache_init(all_data)
        self._inited = True

    async def get(self, kind: VersionedDataKind, key: str) -> Optional[Any]:
        """ """
        hit, value = self._cache_get_item(kind, key)
        if hit:
            return value
        descriptor = await self._core.get_internal(kind, key)
        return self._cache_put_item(kind, key, descriptor)

    async def all(self, kind: VersionedDataKind) -> Dict[str, Any]:
        """ """
        hit, value = self._cache_get_all(kind)
        if hit:
            return value
        descriptors = await self._core.get_all_internal(kind)
        return self._cache_put_all(kind, descriptors)

    async def delete(self, kind: VersionedDataKind, key: str, version: int) -> bool:
        """ """
        deleted_item = {"key": key, "version": version, "deleted": True}
        return await self.upsert(kind, deleted_item)

    async def upsert(self, kind: VersionedDataKind, item: dict) -> bool:
        """ """
        encoded_item = _ensure_encoded(kind, item)
        key = encoded_item['key']
        updated = await self._core.upsert_internal(kind, key, _serialize(encoded_item))
        if updated:
            self._cache_put_upsert(kind, key, encoded_item)
        elif self._caching():
            # The store has a newer item. Read it, so that the cache does not keep an older one.
            self._cache_put_rejected_upsert(kind, key, await self._core.get_internal(kind, key))
        return updated

    @property
    def initialized(self) -> bool:
        """Returns the store's last observed initialized state."""
        return self._inited

    async def is_initialized(self) -> bool:
        """Queries the store's initialized state, updating :attr:`initialized`.

        Honors the cache: with caching off the store is queried on every call;
        with a TTL it is queried once per interval; with an infinite TTL it is
        queried once. Once the store reports initialized the state latches and
        later calls return without I/O.
        """
        if self._inited:
            return True
        result = self._cache.get(AsyncCachingStoreWrapper.__INITED_CACHE_KEY__)
        if result is None:
            result = bool(await self._core.initialized_internal())
            self._cache[AsyncCachingStoreWrapper.__INITED_CACHE_KEY__] = result
        if result:
            self._inited = True
        return result

    async def close(self) -> None:
        """Releases the cache and closes the core."""
        self.disable_cache()
        await self._core.close()

    def describe_configuration(self, config) -> str:
        describe = getattr(self._core, 'describe_configuration', None)
        if callable(describe):
            return describe(config)
        return "custom"
