"""
This submodule contains support code for writing async feature store implementations.
"""

import inspect
from typing import Any, Dict, Mapping, Optional

from ldclient.feature_store import CacheConfig
from ldclient.feature_store_helpers import (
    _CachingStoreWrapperBase,
    _ensure_encoded
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

    _core: AsyncFeatureStoreCore

    def __init__(self, core: AsyncFeatureStoreCore, cache_config: CacheConfig):
        """Constructs an instance by wrapping a core implementation object.

        :param core: the implementation object
        :param cache_config: the caching parameters
        """
        self._core = core
        self._has_available_method = callable(getattr(core, 'is_available', None))
        super().__init__(cache_config)

    async def is_available(self) -> bool:
        """Tests whether the underlying store seems to be reachable.

        Returns False if the core does not provide an availability check.
        """
        # We know is_available exists since we are checking _has_available_method.
        return await self._core.is_available() if self._has_available_method else False  # type: ignore

    async def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        """ """
        await self._core.init_internal(all_data)
        self._cache_init(all_data)
        self._inited = True

    async def get(self, kind: VersionedDataKind, key: str) -> Optional[Any]:
        """ """
        hit, value = self._cache_get_item(kind, key)
        if hit:
            return value
        encoded_item = await self._core.get_internal(kind, key)
        return self._cache_put_item(kind, key, encoded_item)

    async def all(self, kind: VersionedDataKind) -> Dict[str, Any]:
        """ """
        hit, value = self._cache_get_all(kind)
        if hit:
            return value
        encoded_items = await self._core.get_all_internal(kind)
        return self._cache_put_all(kind, encoded_items)

    async def delete(self, kind: VersionedDataKind, key: str, version: int) -> bool:
        """ """
        deleted_item = {"key": key, "version": version, "deleted": True}
        return await self.upsert(kind, deleted_item)

    async def upsert(self, kind: VersionedDataKind, item: dict) -> bool:
        """ """
        encoded_item = _ensure_encoded(kind, item)
        new_state = await self._core.upsert_internal(kind, encoded_item)
        self._cache_put_upsert(kind, new_state)
        # The core returns the item we passed in if the write was applied, or the existing item if it
        # was rejected by the version check. Identity therefore tells us whether the store changed.
        return new_state is encoded_item

    @property
    def initialized(self) -> bool:
        """Returns whether ``init`` has completed in this process.

        This property does not query the store: it is synchronous, but a persistent-store query is
        a coroutine, so it reflects only whether this process has initialized the store.
        """
        return self._inited

    async def close(self) -> None:
        """Releases the cache and closes the underlying core if it supports it."""
        self.disable_cache()
        core_close = getattr(self._core, "close", None)
        if callable(core_close):
            result = core_close()
            if inspect.isawaitable(result):
                await result

    def describe_configuration(self, config) -> str:
        describe = getattr(self._core, 'describe_configuration', None)
        if callable(describe):
            return describe(config)
        return "custom"
