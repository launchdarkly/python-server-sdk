"""
Async persistent-store availability tracking.

This module provides :class:`AsyncFeatureStoreClientWrapper`, the async analog of
``ldclient.impl.datasystem.fdv2_common.FeatureStoreClientWrapper``. It wraps an
async feature store, sorts collections on ``init``, and watches the store for
outages so that recovery can be reported to a status sink.

The wrapper reports status through a generic callable sink, so it carries no
dependency on the FDv2 data system.
"""

import inspect
from typing import Any, Callable, Dict, Mapping, Optional

from ldclient.feature_store import _FeatureStoreDataSetSorter
from ldclient.impl.aio.concurrency import AsyncRepeatingTask
from ldclient.impl.util import log
from ldclient.interfaces import AsyncFeatureStore, DataStoreStatus
from ldclient.versioned_data_kind import VersionedDataKind


class AsyncFeatureStoreClientWrapper(AsyncFeatureStore):
    """Adds availability tracking around an async feature store.

    Every store operation runs through a wrapper that watches for failures. When
    an operation fails, the wrapper marks the store unavailable and starts a
    background task that polls the store's ``is_available`` method every half
    second. When the store recovers, the wrapper reports the new status to the
    sink and stops polling.

    The status sink is any callable that accepts a :class:`DataStoreStatus`.
    """

    def __init__(self, store: AsyncFeatureStore, status_sink: Callable[[DataStoreStatus], None]):
        """Constructs an instance wrapping ``store``.

        :param store: the async feature store to wrap
        :param status_sink: a callable that receives status updates
        """
        self._store = store
        self._status_sink = status_sink
        self._monitoring_enabled = self.is_monitoring_enabled()

        self._last_available = True
        self._poller: Optional[AsyncRepeatingTask] = None
        self._closed = False

    async def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        await self._wrap(lambda: self._store.init(_FeatureStoreDataSetSorter.sort_all_collections(all_data)))

    async def get(self, kind: VersionedDataKind, key: str) -> Optional[Any]:
        return await self._wrap(lambda: self._store.get(kind, key))

    async def all(self, kind: VersionedDataKind) -> Dict[str, Any]:
        return await self._wrap(lambda: self._store.all(kind))

    async def upsert(self, kind: VersionedDataKind, item: dict) -> bool:
        return await self._wrap(lambda: self._store.upsert(kind, item))

    async def delete(self, kind: VersionedDataKind, key: str, version: int) -> bool:
        return await self._wrap(lambda: self._store.delete(kind, key, version))

    @property
    def initialized(self) -> bool:
        return self._store.initialized

    def disable_cache(self) -> None:
        """Disables the inner store's cache if it supports it."""
        inner_disable = getattr(self._store, "disable_cache", None)
        if callable(inner_disable):
            inner_disable()

    def is_monitoring_enabled(self) -> bool:
        """Returns whether the inner store supports availability checks.

        Availability polling requires the store to provide an ``is_available``
        method so the wrapper can detect recovery.
        """
        return callable(getattr(self._store, "is_available", None))

    async def _wrap(self, fn: Callable):
        try:
            return await fn()
        except BaseException:
            if self._monitoring_enabled:
                self._update_availability(False)
            raise

    def _update_availability(self, available: bool) -> None:
        if self._closed:
            return
        if available == self._last_available:
            return

        self._last_available = available
        poller_to_stop = None
        task_to_start = None

        if available:
            poller_to_stop = self._poller
            self._poller = None
            log.warning("Persistent store is available again")
        else:
            log.warning("Detected persistent store unavailability; updates will be cached until it recovers")
            if self._poller is None:
                task_to_start = AsyncRepeatingTask("ldclient.check-availability", 0.5, 0, self._check_availability)
                self._poller = task_to_start

        self._status_sink(DataStoreStatus(available, True))

        if poller_to_stop is not None:
            poller_to_stop.stop()

        if task_to_start is not None:
            task_to_start.start()

    async def _check_availability(self) -> None:
        try:
            if await self._store.is_available():  # type: ignore[attr-defined]
                self._update_availability(True)
        except BaseException as e:
            log.error("Unexpected error from data store status function: %s", e)

    async def close(self) -> None:
        """Stops the availability poller and closes the inner store."""
        poller_to_stop = None
        if not self._closed:
            self._closed = True
            poller_to_stop = self._poller
            self._poller = None

        if poller_to_stop is not None:
            poller_to_stop.stop()
            await poller_to_stop.wait_stopped()

        close = getattr(self._store, "close", None)
        if callable(close):
            result = close()
            if inspect.isawaitable(result):
                await result


__all__ = ["AsyncFeatureStoreClientWrapper"]
