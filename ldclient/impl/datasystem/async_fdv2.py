"""
FDv2 data system coordinator: manages initializers and synchronizers to
obtain and keep the SDK's data up-to-date, operating with an optional
persistent store in read-only or read/write mode.
"""

import time
from typing import List, Optional, cast

from ldclient.async_config import AsyncConfig
from ldclient.config import DataSourceBuilder, DataSystemConfig
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
    DiagnosticAccumulator,
    DiagnosticSource
)
from ldclient.impl.datasystem.fdv2_common import (
    ConditionDirective,
    DataSourceStatusProviderImpl,
    DataStoreStatusProviderImpl,
    FeatureStoreClientWrapper,
    fallback_condition,
    recovery_condition
)
from ldclient.impl.datasystem.store import Store
from ldclient.impl.listeners import Listeners
from ldclient.impl.util import _LD_FD_FALLBACK_HEADER, _Fail, log
from ldclient.interfaces import (
    AsyncInitializer,
    AsyncReadOnlyStore,
    AsyncSynchronizer,
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    DataSourceStatusProvider,
    DataStoreMode,
    DataStoreStatus,
    DataStoreStatusProvider,
    ReadOnlyStore
)


class _AsyncStoreView:
    """Presents FDv2's synchronous in-memory active store as an
    :class:`AsyncReadOnlyStore`, so the client's evaluation path reads through
    one uniform async interface across FDv1 and FDv2. Reads are in-memory dict
    lookups, so there is nothing to await. This is the async analog of the sync
    side's ``FeatureStoreClientWrapper``."""

    def __init__(self, store: ReadOnlyStore):
        self._store = store

    async def get(self, kind, key):
        return self._store.get(kind, key, lambda x: x)

    async def all(self, kind):
        return self._store.all(kind, lambda x: x)


class AsyncFDv2(AsyncDataSystem):
    """
    AsyncFDv2 is an implementation of the AsyncDataSystem interface that uses the Flag Delivery V2 protocol
    for obtaining and keeping data up-to-date. Additionally, it operates with an optional persistent
    store in read-only or read/write mode.
    """

    def __init__(
        self,
        config: AsyncConfig,
        data_system_config: DataSystemConfig,
    ):
        """
        Initialize a new AsyncFDv2 data system.

        :param config: Configuration for initializers and synchronizers
        :param persistent_store: Optional persistent store for data persistence
        :param store_writable: Whether the persistent store should be written to
        :param disabled: Whether the data system is disabled (offline mode)
        """
        self._config = config
        self._data_system_config = data_system_config
        self._synchronizers: List[DataSourceBuilder] = list(data_system_config.synchronizers) if data_system_config.synchronizers else []
        self._fdv1_fallback_synchronizer_builder = data_system_config.fdv1_fallback_synchronizer
        self._disabled = self._config.offline

        # Diagnostic accumulator provided by client for streaming metrics
        self._diagnostic_accumulator: Optional[DiagnosticAccumulator] = None

        # Set up event listeners
        self._flag_change_listeners = Listeners()
        self._change_set_listeners = Listeners()
        self._data_store_listeners = Listeners()

        self._data_store_listeners.add(self._persistent_store_outage_recovery)

        # Create the store
        self._store = Store(self._flag_change_listeners, self._change_set_listeners)

        # Status providers
        self._data_source_status_provider = DataSourceStatusProviderImpl(Listeners())
        self._data_store_status_provider = DataStoreStatusProviderImpl(None, self._data_store_listeners)

        # Configure persistent store if provided
        if self._data_system_config.data_store is not None:
            self._data_store_status_provider = DataStoreStatusProviderImpl(self._data_system_config.data_store, self._data_store_listeners)
            writable = self._data_system_config.data_store_mode == DataStoreMode.READ_WRITE
            wrapper = FeatureStoreClientWrapper(self._data_system_config.data_store, self._data_store_status_provider)
            self._store.with_persistence(
                wrapper, writable, self._data_store_status_provider
            )

        # Concurrency
        self._stop_event = AsyncEvent()
        self._lock = AsyncLock()
        self._active_synchronizer: Optional[AsyncSynchronizer] = None
        self._runner = AsyncTaskRunner()

        # Track configuration
        self._configured_with_data_sources = (
            (data_system_config.initializers is not None and len(data_system_config.initializers) > 0)
            or len(self._synchronizers) > 0
        )

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
        self._store.close()

    def set_diagnostic_accumulator(self, diagnostic_accumulator: DiagnosticAccumulator):
        """
        Sets the diagnostic accumulator for streaming initialization metrics.
        This should be called before start() to ensure metrics are collected.
        """
        self._diagnostic_accumulator = diagnostic_accumulator

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
                # DataSystemConfig types builders with the sync Initializer;
                # async data systems are configured with async builders.
                initializer = cast(AsyncInitializer, initializer_builder.build(self._config))
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
                self._store.apply(basis.change_set, basis.persist)

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

        :return: Tuple of (should_remove_sync, fallback_to_fdv1, directive)
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
                    self._store.apply(update.change_set, True)

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
            await synchronizer.stop()
            timer.stop()

            if sync_reader is not None:
                await join_handle(sync_reader, 0.5)

        # If we reach here, the synchronizer's iterator completed normally (no more updates)
        # For continuous synchronizers (streaming/polling), this is unexpected and indicates
        # the synchronizer can't provide more updates, so we should remove it and fall back
        return ConditionDirective.REMOVE

    def _persistent_store_outage_recovery(self, data_store_status: DataStoreStatus):
        """
        Monitor the data store status. If the store comes online and
        potentially has stale data, we should write our known state to it.
        """
        if not data_store_status.available:
            return

        if not data_store_status.stale:
            return

        err = self._store.commit()
        if err is not None:
            log.error("Failed to reinitialize data store", exc_info=err)

    @property
    def store(self) -> AsyncReadOnlyStore:
        """Get the underlying store for flag evaluation."""
        return _AsyncStoreView(self._store.get_active_store())

    @property
    def data_source_status_provider(self) -> DataSourceStatusProvider:
        """Get the data source status provider."""
        return self._data_source_status_provider

    @property
    def data_store_status_provider(self) -> DataStoreStatusProvider:
        """Get the data store status provider."""
        return self._data_store_status_provider

    @property
    def flag_change_listeners(self) -> Listeners:
        """Get the collection of listeners for flag change events."""
        return self._flag_change_listeners

    @property
    def data_availability(self) -> DataAvailability:
        """Get the current data availability level."""
        if self._store.selector().is_defined():
            return DataAvailability.REFRESHED

        if not self._configured_with_data_sources or self._store.is_initialized():
            return DataAvailability.CACHED

        return DataAvailability.DEFAULTS

    @property
    def target_availability(self) -> DataAvailability:
        """Get the target data availability level based on configuration."""
        if self._configured_with_data_sources:
            return DataAvailability.REFRESHED

        return DataAvailability.CACHED


__all__ = [
    'AsyncFDv2',
    'ConditionDirective',
    'DataSourceStatusProviderImpl',
    'DataStoreStatusProviderImpl',
    'FeatureStoreClientWrapper',
]
