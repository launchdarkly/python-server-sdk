"""
FDv2 data system coordinator: manages initializers and synchronizers to
obtain and keep the SDK's data up-to-date, operating with an optional
persistent store in read-only or read/write mode.
"""

import asyncio
import inspect
import time
from typing import Any, Callable, Dict, List, Mapping, Optional

from ldclient.async_config import AsyncConfig, AsyncDataSystemConfig
from ldclient.config import DataSourceBuilder
from ldclient.feature_store import _FeatureStoreDataSetSorter
from ldclient.impl.aio.concurrency import (
    AsyncEvent,
    AsyncLock,
    AsyncQueue,
    AsyncRepeatingTask,
    AsyncTaskRunner,
    TaskHandle,
    join_handle,
    spawn_handle
)
from ldclient.impl.datasystem import (
    AsyncDataSystem,
    DataAvailability,
    DiagnosticSource
)
from ldclient.impl.datasystem.async_store import AsyncStore
from ldclient.impl.datasystem.fdv2_common import (
    ConditionDirective,
    DataSourceStatusProviderImpl,
    DataStoreStatusProviderImpl,
    _FDv2Base,
    fallback_condition,
    recovery_condition
)
from ldclient.impl.datasystem.store import _decode
from ldclient.impl.listeners import Listeners
from ldclient.impl.util import _LD_FD_FALLBACK_HEADER, _Fail, log
from ldclient.interfaces import (
    AsyncFeatureStore,
    AsyncReadOnlyStore,
    AsyncSynchronizer,
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    DataStoreMode,
    DataStoreStatus
)
from ldclient.versioned_data_kind import VersionedDataKind


class _AsyncFeatureStoreClientWrapper(AsyncFeatureStore):
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

    async def is_initialized(self) -> bool:
        """Queries the inner store's initialized state.

        Runs through the availability wrapper so a failed query marks the store
        unavailable like any other operation.
        """
        return await self._wrap(lambda: self._store.is_initialized())

    def disable_cache(self) -> None:
        """Disables the inner store's cache if it supports it."""
        inner_disable = getattr(self._store, "disable_cache", None)
        if callable(inner_disable):
            inner_disable()

    def is_monitoring_enabled(self) -> bool:
        """Returns whether the inner store opts in to availability monitoring.

        Delegates to the store's own opt-in. A store that does not report
        availability is not polled, so it is never marked unavailable with no
        path back to recovery.
        """
        store_check = getattr(self._store, "is_monitoring_enabled", None)
        if not callable(store_check):
            return False
        return store_check()

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
        """Stops the availability poller and closes the inner store.

        Does nothing on a later call, so closing more than once is safe.
        """
        if self._closed:
            return
        self._closed = True

        poller_to_stop = self._poller
        self._poller = None
        if poller_to_stop is not None:
            poller_to_stop.stop()
            try:
                await asyncio.wait_for(poller_to_stop.wait_stopped(), timeout=5)
            except asyncio.TimeoutError:
                log.warning("Timed out waiting for the persistent store availability poller to stop")

        close = getattr(self._store, "close", None)
        if callable(close):
            try:
                result = close()
                if inspect.isawaitable(result):
                    await result
            except Exception as e:
                log.warning("Error closing the persistent store: %s", e)


class _AsyncReadOnlyStoreView(AsyncReadOnlyStore):
    """Async read-only view of the FDv2 data system store.

    Serves every read from the store's active store, so a held instance follows
    the swap from the persistent store to the in-memory store once it has data.
    The active store may be the synchronous in-memory store, whose reads return
    a value directly, or the async persistent store, whose reads are awaitable;
    the view awaits only when the read result is awaitable. Items that a custom
    persistent store keeps as raw dicts are decoded into model objects; items
    that are already models pass through unchanged.
    """

    def __init__(self, store: AsyncStore):
        self._store = store

    async def get(self, kind: VersionedDataKind, key: str) -> Optional[Any]:
        item = self._store.get_active_store().get(kind, key)
        if inspect.isawaitable(item):
            item = await item
        return _decode(kind, item)

    async def all(self, kind: VersionedDataKind) -> Dict[str, Any]:
        items = self._store.get_active_store().all(kind)
        if inspect.isawaitable(items):
            items = await items
        return {key: _decode(kind, value) for key, value in items.items()}


class AsyncFDv2(_FDv2Base, AsyncDataSystem):
    """
    AsyncFDv2 is an implementation of the AsyncDataSystem interface that uses the Flag Delivery V2 protocol
    for obtaining and keeping data up-to-date. Additionally, it operates with an optional persistent
    store in read-only or read/write mode.
    """

    _store: AsyncStore

    def __init__(
        self,
        config: AsyncConfig,
        data_system_config: AsyncDataSystemConfig,
    ):
        """
        Initialize a new AsyncFDv2 data system.

        :param config: the SDK configuration
        :param data_system_config: the data system configuration — initializers,
            synchronizers, and the optional persistent store
        """
        super().__init__()

        self._config = config
        self._data_system_config = data_system_config
        self._synchronizers: List[DataSourceBuilder[AsyncSynchronizer]] = list(data_system_config.synchronizers) if data_system_config.synchronizers else []
        self._fdv1_fallback_synchronizer_builder = data_system_config.fdv1_fallback_synchronizer
        self._disabled = config.offline
        self._configured_with_data_sources = (
            (data_system_config.initializers is not None and len(data_system_config.initializers) > 0)
            or len(self._synchronizers) > 0
        )

        if data_system_config.data_store is not None:
            # The provider only calls monitoring methods, which the async store also has.
            self._data_store_status_provider = DataStoreStatusProviderImpl(data_system_config.data_store, self._data_store_listeners)  # type: ignore[arg-type]
            writable = data_system_config.data_store_mode == DataStoreMode.READ_WRITE
            # The async wrapper reports status through a plain callable sink, so
            # pass the provider's update method rather than the provider itself.
            wrapper = _AsyncFeatureStoreClientWrapper(data_system_config.data_store, self._data_store_status_provider.update_status)
            self._store.with_async_persistence(wrapper, writable, self._data_store_status_provider)

        self._store_view = _AsyncReadOnlyStoreView(self._store)

        # Concurrency
        self._stop_event = AsyncEvent()
        self._lock = AsyncLock()
        self._active_synchronizer: Optional[AsyncSynchronizer] = None
        self._runner = AsyncTaskRunner()

    def _create_store(self, flag_change_listeners: Listeners, change_set_listeners: Listeners) -> AsyncStore:
        return AsyncStore(flag_change_listeners, change_set_listeners)

    def _persistent_store_outage_recovery(self, data_store_status: DataStoreStatus) -> None:
        """
        On store recovery, write the current data back to it. The commit runs on
        the task runner (so stop() cancels it) and is skipped once stopping, so
        it never writes to a store that stop() is closing.
        """
        if self._stop_event.is_set():
            return

        if not data_store_status.available:
            return

        if not data_store_status.stale:
            return

        async def _commit() -> None:
            err = await self._store.commit()
            if err is not None:
                log.error("Failed to reinitialize data store", exc_info=err)

        self._runner.spawn("AsyncFDv2-store-recovery", _commit)

    def start(self, set_on_ready: AsyncEvent):
        """
        Start the AsyncFDv2 data system.

        :param set_on_ready: Event to set when the system is ready or has failed
        """
        if self._disabled:
            log.warning("Data system is disabled, SDK will return application-defined default values")
            set_on_ready.set()
            return

        self._stop_event.clear()

        # Start the main coordination loop
        self._runner.spawn("AsyncFDv2-main", lambda: self._run_main_loop(set_on_ready))

    async def stop(self):
        """Stop the AsyncFDv2 data system and all the work it is coordinating."""
        self._stop_event.set()

        async with self._lock:
            if self._active_synchronizer is not None:
                try:
                    await self._active_synchronizer.stop()
                except Exception as e:
                    log.error("Error stopping active data source: %s", e)

        # Wait for the coordinator's background work to complete
        await self._runner.stop_all(timeout=5.0)

        # Close the store
        await self._store.close()

    async def _run_main_loop(self, set_on_ready: AsyncEvent):
        """Main coordination loop that manages initializers and synchronizers."""
        try:
            self._data_source_status_provider.update_status(
                DataSourceState.INITIALIZING, None
            )

            # Run initializers first
            fallback_requested = await self._run_initializers(set_on_ready)

            # If an initializer asked the SDK to fall back to FDv1, halt the
            # configured FDv2 chain and switch terminally to the FDv1 Fallback
            # Synchronizer (or transition to OFF if none is configured).
            if fallback_requested:
                if self._fdv1_fallback_synchronizer_builder is not None:
                    log.warning("Falling back to FDv1 protocol")
                    self._synchronizers = [self._fdv1_fallback_synchronizer_builder]
                else:
                    log.warning(
                        "Initializer requested FDv1 fallback but none configured"
                    )
                    self._synchronizers = []
                    self._data_source_status_provider.update_status(
                        DataSourceState.OFF,
                        self._data_source_status_provider.status.error,
                    )
                    set_on_ready.set()
                    return

            # Run synchronizers
            await self._run_synchronizers(set_on_ready)

        except Exception as e:
            log.error("Error in AsyncFDv2 main loop: %s", e)
            # Ensure ready event is set even on error
            if not set_on_ready.is_set():
                set_on_ready.set()

    async def _run_initializers(self, set_on_ready: AsyncEvent) -> bool:
        """
        Run initializers to get initial data.

        Returns True when an initializer requested the FDv1 Fallback Directive
        (via the X-LD-FD-Fallback response header). When that happens, any
        accompanying payload is applied first so evaluations can serve the
        server-provided data while the FDv1 synchronizer spins up; the caller
        is then responsible for switching to the FDv1 Fallback Synchronizer.
        """
        if self._data_system_config.initializers is None:
            return False

        for initializer_builder in self._data_system_config.initializers:
            if self._stop_event.is_set():
                return False

            try:
                initializer = initializer_builder.build(self._config)
                log.info("Attempting to initialize via %s", initializer.name)

                basis_result = await initializer.fetch(self._store)

                if isinstance(basis_result, _Fail):
                    log.warning("Initializer %s failed: %s", initializer.name, basis_result.error)
                    # An error response can still carry the FDv1 fallback directive.
                    if basis_result.headers is not None and \
                            basis_result.headers.get(_LD_FD_FALLBACK_HEADER) == 'true':
                        log.warning(
                            "Initializer %s requested fallback to FDv1 protocol",
                            initializer.name,
                        )
                        # Surface the underlying error on the status so
                        # programmatic monitors can see why FDv2 shut down.
                        self._data_source_status_provider.update_status(
                            DataSourceState.INITIALIZING,
                            DataSourceErrorInfo(
                                kind=DataSourceErrorKind.UNKNOWN,
                                status_code=0,
                                time=time.time(),
                                message=basis_result.error,
                            ),
                        )
                        return True
                    continue

                basis = basis_result.value
                log.info("Initialized via %s", initializer.name)

                # Apply the basis to the store
                await self._store.apply(basis.change_set, basis.persist)

                # Set ready event if and only if a selector is defined for the changeset
                selector_defined = basis.change_set.selector.is_defined()
                if selector_defined:
                    set_on_ready.set()

                if basis.fallback_to_fdv1:
                    log.warning(
                        "Initializer %s requested fallback to FDv1 protocol",
                        initializer.name,
                    )
                    return True

                if selector_defined:
                    return False
            except Exception as e:
                log.error("Initializer failed with exception: %s", e)
        return False

    async def _run_synchronizers(self, set_on_ready: AsyncEvent):
        """Run synchronizers to keep data up-to-date."""
        # If no synchronizers configured, just set ready and return
        if len(self._synchronizers) == 0:
            set_on_ready.set()
            return

        self._runner.spawn(
            "AsyncFDv2-synchronizers",
            lambda: self._synchronizer_loop(set_on_ready),
        )

    async def _synchronizer_loop(self, set_on_ready: AsyncEvent):
        try:
            # Make a working copy of the synchronizers list
            synchronizers_list = list(self._synchronizers)
            current_index = 0

            # Always ensure ready event is set when we exit
            while not self._stop_event.is_set() and len(synchronizers_list) > 0:
                try:
                    async with self._lock:
                        synchronizer: AsyncSynchronizer = synchronizers_list[current_index].build(self._config)
                        self._active_synchronizer = synchronizer
                        if isinstance(synchronizer, DiagnosticSource) and self._diagnostic_accumulator is not None:
                            synchronizer.set_diagnostic_accumulator(self._diagnostic_accumulator)

                    log.info("Synchronizer %s (index %d) is starting", synchronizer.name, current_index)

                    directive = await self._consume_synchronizer_results(
                        synchronizer, set_on_ready, current_index != 0
                    )

                    if directive == ConditionDirective.FDV1:
                        # Abandon all synchronizers and use only fdv1 fallback
                        log.warning("Falling back to FDv1 protocol")
                        if self._fdv1_fallback_synchronizer_builder is not None:
                            synchronizers_list = [self._fdv1_fallback_synchronizer_builder]
                            current_index = 0
                        else:
                            log.warning("Synchronizer requested FDv1 fallback but none configured")
                            synchronizers_list = []
                            self._data_source_status_provider.update_status(
                                DataSourceState.OFF,
                                self._data_source_status_provider.status.error
                            )
                            break
                        continue
                    elif directive == ConditionDirective.REMOVE:
                        # Permanent failure - remove synchronizer from list
                        log.warning("Synchronizer %s permanently failed, removing from list", synchronizer.name)
                        del synchronizers_list[current_index]

                        if len(synchronizers_list) == 0:
                            log.warning("No more synchronizers available")
                            self._data_source_status_provider.update_status(
                                DataSourceState.OFF,
                                self._data_source_status_provider.status.error
                            )
                            break

                        # Adjust index if we're now beyond the end of the list
                        # If we deleted the last synchronizer, wrap to the beginning
                        if current_index >= len(synchronizers_list):
                            current_index = 0
                        # Note: If we deleted a middle element, current_index now points to
                        # what was the next element (shifted down), which is correct
                        continue
                    # Condition was met - determine next synchronizer based on directive
                    elif directive == ConditionDirective.RECOVER:
                        log.info("Recovery condition met, returning to first synchronizer")
                        current_index = 0
                    elif directive == ConditionDirective.FALLBACK:
                        # Fallback to next synchronizer (wraps to 0 at end)
                        current_index = (current_index + 1) % len(synchronizers_list)
                        log.info("Fallback condition met, moving to synchronizer at index %d", current_index)

                except Exception as e:
                    log.error("Failed to build or run synchronizer: %s", e)
                    break

        except Exception as e:
            log.error("Error in synchronizer loop: %s", e)
        finally:
            # Ensure we always set the ready event when exiting
            set_on_ready.set()
            async with self._lock:
                if self._active_synchronizer is not None:
                    await self._active_synchronizer.stop()
                self._active_synchronizer = None

    async def _consume_synchronizer_results(
        self,
        synchronizer: AsyncSynchronizer,
        set_on_ready: AsyncEvent,
        check_recovery: bool,
    ) -> ConditionDirective:
        """
        Consume results from a synchronizer until a condition is met or it fails.

        :return: the ConditionDirective describing how to proceed
        """
        action_queue: AsyncQueue = AsyncQueue()
        timer = AsyncRepeatingTask(
            label="AsyncFDv2-sync-cond-timer",
            interval=10,
            initial_delay=10,
            callable=lambda: action_queue.put("check")
        )

        async def reader():
            try:
                async for update in synchronizer.sync(self._store):
                    await action_queue.put(update)
            finally:
                await action_queue.put("quit")

        sync_reader: Optional[TaskHandle] = None

        try:
            timer.start()
            sync_reader = spawn_handle("AsyncFDv2-sync-reader", reader)

            while True:
                # Honor a stop request every iteration so a queue that always has
                # an item ready cannot starve the check.
                if self._stop_event.is_set():
                    return ConditionDirective.FALLBACK
                update = await action_queue.get()
                if isinstance(update, str):
                    if update == "quit":
                        break

                    if update == "check":
                        # Check condition periodically
                        current_status = self._data_source_status_provider.status
                        if check_recovery and recovery_condition(current_status):
                            return ConditionDirective.RECOVER
                        if fallback_condition(current_status):
                            return ConditionDirective.FALLBACK
                    continue

                log.info("Synchronizer %s update: %s", synchronizer.name, update.state)
                if self._stop_event.is_set():
                    return ConditionDirective.FALLBACK

                # Handle the update
                if update.change_set is not None:
                    await self._store.apply(update.change_set, True)

                # Set ready event on first valid update
                if update.state == DataSourceState.VALID and not set_on_ready.is_set():
                    set_on_ready.set()

                # Update status
                self._data_source_status_provider.update_status(update.state, update.error)

                # Check if we should fall back to FDv1 immediately. fallback_to_fdv1
                # may ride along on a Valid update (payload + directive in the same
                # response), in which case the ChangeSet has already been applied
                # above before we hand off.
                if update.fallback_to_fdv1:
                    return ConditionDirective.FDV1

                # Check for OFF state indicating permanent failure
                if update.state == DataSourceState.OFF:
                    return ConditionDirective.REMOVE
        except Exception as e:
            log.error("Error consuming synchronizer results: %s", e)
            return ConditionDirective.REMOVE
        finally:
            timer.stop()
            if sync_reader is not None:
                sync_reader.cancel()

            await synchronizer.stop()
            if sync_reader is not None:
                await join_handle(sync_reader, 0.5)

        # If we reach here, the synchronizer's iterator completed normally (no more updates)
        # For continuous synchronizers (streaming/polling), this is unexpected and indicates
        # the synchronizer can't provide more updates, so we should remove it and fall back
        return ConditionDirective.REMOVE

    @property
    def store(self) -> AsyncReadOnlyStore:
        """Get the underlying store for flag evaluation."""
        return self._store_view

    async def data_availability(self) -> DataAvailability:  # type: ignore[override]
        """Reports what form of data is currently available, awaiting the store's
        readiness so a persistent store populated by another process is recognized
        before a synchronizer supplies a basis. A persistent-store error is treated
        as no data: it is logged and reported as ``DEFAULTS`` rather than raised."""
        if self._store.selector().is_defined():
            return DataAvailability.REFRESHED
        if not self._configured_with_data_sources:
            return DataAvailability.CACHED
        try:
            ready = await self._store.is_ready()
        except Exception as e:
            log.warning("Error checking persistent store readiness: %s", e)
            return DataAvailability.DEFAULTS
        return DataAvailability.CACHED if ready else DataAvailability.DEFAULTS


__all__ = [
    'AsyncFDv2',
    'ConditionDirective',
    'DataSourceStatusProviderImpl',
    'DataStoreStatusProviderImpl',
]
