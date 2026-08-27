"""
Async persist sibling of the FDv2 store.

:class:`AsyncStore` shares the store-agnostic engine of
:class:`ldclient.impl.datasystem.store._StoreBase` with the synchronous
:class:`ldclient.impl.datasystem.store.Store`, but owns an async persistent
store and writes to it through awaited I/O run outside the synchronous lock.
"""

from typing import Any, Callable, Dict, Optional

from ldclient.impl.aio.concurrency import AsyncLock
from ldclient.impl.datasystem.store import Collections, _StoreBase
from ldclient.impl.listeners import Listeners
from ldclient.impl.model.entity import ModelEntity
from ldclient.impl.util import log
from ldclient.interfaces import (
    AsyncFeatureStore,
    ChangeSet,
    DataStoreStatusProvider,
    IntentCode
)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind


class AsyncStore(_StoreBase):
    """
    AsyncStore is a dual-mode persistent/in-memory store that persists asynchronously.

    It behaves like :class:`ldclient.impl.datasystem.store.Store` for in-memory
    reads and change notification, but writes to an async persistent store
    through awaited I/O run outside the synchronous lock.
    """

    def __init__(
        self,
        flag_change_listeners: Listeners,
        change_set_listeners: Listeners,
    ):
        super().__init__(flag_change_listeners, change_set_listeners)

        self._persistent_store: Optional[AsyncFeatureStore] = None
        self._persistent_store_status_provider: Optional[DataStoreStatusProvider] = None
        self._persistent_store_writable = False

        # True if the data in the memory store may be written to the persistent
        # store. Set on each successful apply from its persist flag.
        self._persist = False

        # Serializes async store writes; held only across the awaited I/O, never with self._lock.
        self._async_persist_lock = AsyncLock()

    def with_async_persistence(
        self,
        async_store: AsyncFeatureStore,
        writable: bool,
        status_provider: Optional[DataStoreStatusProvider] = None,
    ) -> "AsyncStore":
        """
        Configure the store with an async persistent store for read-only or read-write access.

        Args:
            async_store: The async persistent store implementation
            writable: Whether the persistent store should be written to
            status_provider: Optional status provider for the persistent store

        Returns:
            Self for method chaining
        """
        with self._lock:
            self._persistent_store = async_store
            self._persistent_store_writable = writable
            self._persistent_store_status_provider = status_provider

            # Initially use persistent store as active until memory store has data
            self._active_store = async_store  # type: ignore[assignment]

        return self

    def _should_persist(self) -> bool:
        """Returns whether data should be persisted to the persistent store."""
        return (
            self._persist
            and self._persistent_store is not None
            and self._persistent_store_writable
        )

    def _on_memory_store_active(self) -> None:
        # In-memory store is now authoritative. Replace the persistent-store
        # cache with a no-op so we don't hold a duplicate copy of every flag.
        # Done before the persist step so the wrapper's init can skip its decode
        # loop now that the cache is disabled.
        if self._persistent_store is not None and hasattr(
            self._persistent_store, "disable_cache"
        ):
            try:
                self._persistent_store.disable_cache()  # type: ignore[attr-defined]
            except Exception as e:
                log.warning("Failed to disable persistent store cache: %s", e)

    async def apply(self, change_set: ChangeSet, persist: bool) -> None:
        """
        Apply a changeset to the in-memory store and, if configured, the async
        persistent store.

        Args:
            change_set: The changeset to apply
            persist: Whether the changes should be persisted to the persistent store
        """
        collections = self._changes_to_store_data(change_set.changes)

        applied = False
        is_full = False

        with self._lock:
            try:
                if change_set.intent_code == IntentCode.TRANSFER_FULL:
                    applied = self._set_basis(collections, change_set.selector)
                    is_full = True
                elif change_set.intent_code == IntentCode.TRANSFER_CHANGES:
                    applied = self._apply_delta(collections, change_set.selector)
                elif change_set.intent_code == IntentCode.TRANSFER_NONE:
                    return

                self._change_set_listeners.notify(change_set)

                if applied:
                    # Memory now holds this data, so it may be persisted.
                    self._persist = persist
            except Exception as e:
                log.error("Store: couldn't apply changeset: %s", str(e))
                return

        if not applied or not self._should_persist():
            return

        store = self._persistent_store
        if store is None:
            return

        async with self._async_persist_lock:
            try:
                if is_full:
                    await store.init(collections)
                else:
                    for kind in collections:
                        kind_data = collections[kind]
                        for key in kind_data:
                            await store.upsert(kind, kind_data[key])
            except Exception as e:
                log.error("Store: couldn't persist changeset: %s", str(e))

    async def commit(self) -> Optional[Exception]:
        """
        Persist the contents of the memory store to the async persistent store,
        if configured.

        Returns:
            Exception if the commit failed, None otherwise
        """
        def __mapping_from_kind(kind: VersionedDataKind) -> Callable[[Dict[str, ModelEntity]], Dict[str, Dict[str, Any]]]:
            def __mapping(data: Dict[str, ModelEntity]) -> Dict[str, Dict[str, Any]]:
                return {k: kind.encode(v) for k, v in data.items()}

            return __mapping

        store = self._persistent_store
        if store is None:
            return None

        async with self._async_persist_lock:
            try:
                all_data: Optional[Collections] = None
                with self._lock:
                    if self._should_persist():
                        all_data = {}
                        for kind in [FEATURES, SEGMENTS]:
                            all_data[kind] = self._memory_store.all(kind, __mapping_from_kind(kind))

                if all_data is None:
                    return None

                await store.init(all_data)
            except Exception as e:
                return e
        return None

    async def close(self) -> None:
        """Close the store and the async persistent store, if configured."""
        store = self._persistent_store
        if store is None:
            return
        try:
            await store.close()
        except Exception as e:
            log.warning("Error closing the persistent store: %s", e)

    def get_data_store_status_provider(self) -> Optional[DataStoreStatusProvider]:
        """Get the data store status provider for the persistent store, if configured."""
        with self._lock:
            return self._persistent_store_status_provider

    async def is_ready(self) -> bool:
        """Reports whether the active store holds usable data.

        Once the in-memory store is active its readiness is authoritative and no
        query is made. While the persistent store is active, its readiness is
        queried (awaiting the store), so a store populated by another process is
        recognized.
        """
        store = self._persistent_store
        if store is None or self._active_store is self._memory_store:
            return self._active_store.initialized
        return await store.is_initialized()


__all__ = ["AsyncStore"]
