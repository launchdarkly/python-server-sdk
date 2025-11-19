import logging
import time
from queue import Empty, Queue
from threading import Event, Thread
from typing import Any, Callable, Dict, List, Mapping, Optional

from ldclient.config import Builder, Config, DataSystemConfig
from ldclient.feature_store import _FeatureStoreDataSetSorter
from ldclient.impl.datasourcev2.status import (
    DataSourceStatusProviderImpl,
    DataStoreStatusProviderImpl
)
from ldclient.impl.datasystem import (
    DataAvailability,
    DiagnosticAccumulator,
    DiagnosticSource,
    Synchronizer
)
from ldclient.impl.datasystem.store import Store
from ldclient.impl.flag_tracker import FlagTrackerImpl
from ldclient.impl.listeners import Listeners
from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.impl.util import _Fail, log
from ldclient.interfaces import (
    DataSourceState,
    DataSourceStatus,
    DataSourceStatusProvider,
    DataStoreMode,
    DataStoreStatus,
    DataStoreStatusProvider,
    FeatureStore,
    FlagTracker,
    ReadOnlyStore
)
from ldclient.versioned_data_kind import VersionedDataKind


class FeatureStoreClientWrapper(FeatureStore):
    """Provides additional behavior that the client requires before or after feature store operations.
    Currently this just means sorting the data set for init() and dealing with data store status listeners.
    """

    def __init__(self, store: FeatureStore, store_update_sink: DataStoreStatusProviderImpl):
        self.store = store
        self.__store_update_sink = store_update_sink
        self.__monitoring_enabled = self.is_monitoring_enabled()

        # Covers the following variables
        self.__lock = ReadWriteLock()
        self.__last_available = True
        self.__poller: Optional[RepeatingTask] = None

    def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, Dict[Any, Any]]]):
        return self.__wrapper(lambda: self.store.init(_FeatureStoreDataSetSorter.sort_all_collections(all_data)))

    def get(self, kind, key, callback):
        return self.__wrapper(lambda: self.store.get(kind, key, callback))

    def all(self, kind, callback):
        return self.__wrapper(lambda: self.store.all(kind, callback))

    def delete(self, kind, key, version):
        return self.__wrapper(lambda: self.store.delete(kind, key, version))

    def upsert(self, kind, item):
        return self.__wrapper(lambda: self.store.upsert(kind, item))

    @property
    def initialized(self) -> bool:
        return self.store.initialized

    def __wrapper(self, fn: Callable):
        try:
            return fn()
        except BaseException:
            if self.__monitoring_enabled:
                self.__update_availability(False)
            raise

    def __update_availability(self, available: bool):
        try:
            self.__lock.lock()
            if available == self.__last_available:
                return
            self.__last_available = available
        finally:
            self.__lock.unlock()

        if available:
            log.warning("Persistent store is available again")

        status = DataStoreStatus(available, True)
        self.__store_update_sink.update_status(status)

        if available:
            try:
                self.__lock.lock()
                if self.__poller is not None:
                    self.__poller.stop()
                    self.__poller = None
            finally:
                self.__lock.unlock()

            return

        log.warning("Detected persistent store unavailability; updates will be cached until it recovers")
        task = RepeatingTask("ldclient.check-availability", 0.5, 0, self.__check_availability)

        self.__lock.lock()
        self.__poller = task
        self.__poller.start()
        self.__lock.unlock()

    def __check_availability(self):
        try:
            if self.store.is_available():
                self.__update_availability(True)
        except BaseException as e:
            log.error("Unexpected error from data store status function: %s", e)

    def is_monitoring_enabled(self) -> bool:
        """
        This methods determines whether the wrapped store can support enabling monitoring.

        The wrapped store must provide a monitoring_enabled method, which must
        be true. But this alone is not sufficient.

        Because this class wraps all interactions with a provided store, it can
        technically "monitor" any store. However, monitoring also requires that
        we notify listeners when the store is available again.

        We determine this by checking the store's `available?` method, so this
        is also a requirement for monitoring support.

        These extra checks won't be necessary once `available` becomes a part
        of the core interface requirements and this class no longer wraps every
        feature store.
        """

        if not hasattr(self.store, 'is_monitoring_enabled'):
            return False

        if not hasattr(self.store, 'is_available'):
            return False

        monitoring_enabled = getattr(self.store, 'is_monitoring_enabled')
        if not callable(monitoring_enabled):
            return False

        return monitoring_enabled()


class FDv2:
    """
    FDv2 is an implementation of the DataSystem interface that uses the Flag Delivery V2 protocol
    for obtaining and keeping data up-to-date. Additionally, it operates with an optional persistent
    store in read-only or read/write mode.
    """

    def __init__(
        self,
        config: Config,
        data_system_config: DataSystemConfig,
    ):
        """
        Initialize a new FDv2 data system.

        :param config: Configuration for initializers and synchronizers
        :param persistent_store: Optional persistent store for data persistence
        :param store_writable: Whether the persistent store should be written to
        :param disabled: Whether the data system is disabled (offline mode)
        """
        self._config = config
        self._data_system_config = data_system_config
        self._primary_synchronizer_builder: Optional[Builder[Synchronizer]] = data_system_config.primary_synchronizer
        self._secondary_synchronizer_builder = data_system_config.secondary_synchronizer
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

        # Flag tracker (evaluation function set later by client)
        self._flag_tracker = FlagTrackerImpl(
            self._flag_change_listeners,
            lambda key, context: None  # Placeholder, replaced by client
        )

        # Threading
        self._stop_event = Event()
        self._lock = ReadWriteLock()
        self._active_synchronizer: Optional[Synchronizer] = None
        self._threads: List[Thread] = []

        # Track configuration
        self._configured_with_data_sources = (
            (data_system_config.initializers is not None and len(data_system_config.initializers) > 0)
            or data_system_config.primary_synchronizer is not None
        )

    def start(self, set_on_ready: Event):
        """
        Start the FDv2 data system.

        :param set_on_ready: Event to set when the system is ready or has failed
        """
        if self._disabled:
            log.warning("Data system is disabled, SDK will return application-defined default values")
            set_on_ready.set()
            return

        self._stop_event.clear()

        # Start the main coordination thread
        main_thread = Thread(
            target=self._run_main_loop,
            args=(set_on_ready,),
            name="FDv2-main",
            daemon=True
        )
        main_thread.start()
        self._threads.append(main_thread)

    def stop(self):
        """Stop the FDv2 data system and all associated threads."""
        self._stop_event.set()

        self._lock.lock()
        if self._active_synchronizer is not None:
            try:
                self._active_synchronizer.stop()
            except Exception as e:
                log.error("Error stopping active data source: %s", e)
        self._lock.unlock()

        # Wait for all threads to complete
        for thread in self._threads:
            if thread.is_alive():
                thread.join(timeout=5.0)  # 5 second timeout
                if thread.is_alive():
                    log.warning("Thread %s did not terminate in time", thread.name)

        # Close the store
        self._store.close()

    def set_diagnostic_accumulator(self, diagnostic_accumulator: DiagnosticAccumulator):
        """
        Sets the diagnostic accumulator for streaming initialization metrics.
        This should be called before start() to ensure metrics are collected.
        """
        self._diagnostic_accumulator = diagnostic_accumulator

    def _run_main_loop(self, set_on_ready: Event):
        """Main coordination loop that manages initializers and synchronizers."""
        try:
            self._data_source_status_provider.update_status(
                DataSourceState.INITIALIZING, None
            )

            # Run initializers first
            self._run_initializers(set_on_ready)

            # Run synchronizers
            self._run_synchronizers(set_on_ready)

        except Exception as e:
            log.error("Error in FDv2 main loop: %s", e)
            # Ensure ready event is set even on error
            if not set_on_ready.is_set():
                set_on_ready.set()

    def _run_initializers(self, set_on_ready: Event):
        """Run initializers to get initial data."""
        if self._data_system_config.initializers is None:
            return

        for initializer_builder in self._data_system_config.initializers:
            if self._stop_event.is_set():
                return

            try:
                initializer = initializer_builder(self._config)
                log.info("Attempting to initialize via %s", initializer.name)

                basis_result = initializer.fetch(self._store)
                print("@@@@@@", "init", basis_result, "\n")

                if isinstance(basis_result, _Fail):
                    log.warning("Initializer %s failed: %s", initializer.name, basis_result.error)
                    continue

                basis = basis_result.value
                log.info("Initialized via %s", initializer.name)

                # Apply the basis to the store
                self._store.apply(basis.change_set, basis.persist)

                # Set ready event
                if not set_on_ready.is_set():
                    set_on_ready.set()
            except Exception as e:
                log.error("Initializer failed with exception: %s", e)

    def _run_synchronizers(self, set_on_ready: Event):
        """Run synchronizers to keep data up-to-date."""
        # If no primary synchronizer configured, just set ready and return
        if self._data_system_config.primary_synchronizer is None:
            if not set_on_ready.is_set():
                set_on_ready.set()
            return

        def synchronizer_loop(self: 'FDv2'):
            try:
                # Always ensure ready event is set when we exit
                while not self._stop_event.is_set() and self._primary_synchronizer_builder is not None:
                    # Try primary synchronizer
                    try:
                        self._lock.lock()
                        primary_sync = self._primary_synchronizer_builder(self._config)
                        if isinstance(primary_sync, DiagnosticSource) and self._diagnostic_accumulator is not None:
                            primary_sync.set_diagnostic_accumulator(self._diagnostic_accumulator)
                        self._active_synchronizer = primary_sync
                        self._lock.unlock()

                        log.info("Primary synchronizer %s is starting", primary_sync.name)

                        remove_sync, fallback_v1 = self._consume_synchronizer_results(
                            primary_sync, set_on_ready, self._fallback_condition
                        )

                        if remove_sync:
                            self._primary_synchronizer_builder = self._secondary_synchronizer_builder
                            self._secondary_synchronizer_builder = None

                            if fallback_v1:
                                self._primary_synchronizer_builder = self._fdv1_fallback_synchronizer_builder

                            if self._primary_synchronizer_builder is None:
                                log.warning("No more synchronizers available")
                                self._data_source_status_provider.update_status(
                                    DataSourceState.OFF,
                                    self._data_source_status_provider.status.error
                                )
                                break
                        else:
                            log.info("Fallback condition met")

                        if self._stop_event.is_set():
                            break

                        if self._secondary_synchronizer_builder is None:
                            continue

                        self._lock.lock()
                        secondary_sync = self._secondary_synchronizer_builder(self._config)
                        if isinstance(secondary_sync, DiagnosticSource) and self._diagnostic_accumulator is not None:
                            secondary_sync.set_diagnostic_accumulator(self._diagnostic_accumulator)
                        log.info("Secondary synchronizer %s is starting", secondary_sync.name)
                        self._active_synchronizer = secondary_sync
                        self._lock.unlock()

                        remove_sync, fallback_v1 = self._consume_synchronizer_results(
                            secondary_sync, set_on_ready, self._recovery_condition
                        )

                        if remove_sync:
                            self._secondary_synchronizer_builder = None
                            if fallback_v1:
                                self._primary_synchronizer_builder = self._fdv1_fallback_synchronizer_builder

                            if self._primary_synchronizer_builder is None:
                                log.warning("No more synchronizers available")
                                self._data_source_status_provider.update_status(
                                    DataSourceState.OFF,
                                    self._data_source_status_provider.status.error
                                )
                                break

                        log.info("Recovery condition met, returning to primary synchronizer")
                    except Exception as e:
                        log.error("Failed to build primary synchronizer: %s", e)
                        break

            except Exception as e:
                log.error("Error in synchronizer loop: %s", e)
            finally:
                # Ensure we always set the ready event when exiting
                set_on_ready.set()
                self._lock.lock()
                if self._active_synchronizer is not None:
                    self._active_synchronizer.stop()
                self._active_synchronizer = None
                self._lock.unlock()

        sync_thread = Thread(
            target=synchronizer_loop,
            name="FDv2-synchronizers",
            args=(self,),
            daemon=True
        )
        sync_thread.start()
        self._threads.append(sync_thread)

    def _consume_synchronizer_results(
        self,
        synchronizer: Synchronizer,
        set_on_ready: Event,
        condition_func: Callable[[DataSourceStatus], bool]
    ) -> tuple[bool, bool]:
        """
        Consume results from a synchronizer until a condition is met or it fails.

        :return: Tuple of (should_remove_sync, fallback_to_fdv1)
        """
        action_queue: Queue = Queue()
        timer = RepeatingTask(
            label="FDv2-sync-cond-timer",
            interval=10,
            initial_delay=10,
            callable=lambda: action_queue.put("check")
        )

        def reader(self: 'FDv2'):
            try:
                for update in synchronizer.sync(self._store):
                    print("@@@@@@", "update is at", update, "\n")
                    action_queue.put(update)
            finally:
                action_queue.put("quit")

        sync_reader = Thread(
            target=reader,
            name="FDv2-sync-reader",
            args=(self,),
            daemon=True
        )

        try:
            timer.start()
            sync_reader.start()

            while True:
                update = action_queue.get(True)
                if isinstance(update, str):
                    if update == "quit":
                        break

                    if update == "check":
                        # Check condition periodically
                        current_status = self._data_source_status_provider.status
                        if condition_func(current_status):
                            return False, False
                    continue

                log.info("Synchronizer %s update: %s", synchronizer.name, update.state)
                if self._stop_event.is_set():
                    return False, False

                # Handle the update
                if update.change_set is not None:
                    self._store.apply(update.change_set, True)

                # Set ready event on first valid update
                if update.state == DataSourceState.VALID and not set_on_ready.is_set():
                    set_on_ready.set()

                # Update status
                self._data_source_status_provider.update_status(update.state, update.error)

                # Check if we should revert to FDv1 immediately
                if update.revert_to_fdv1:
                    return True, True

                # Check for OFF state indicating permanent failure
                if update.state == DataSourceState.OFF:
                    return True, False
        except Exception as e:
            log.error("Error consuming synchronizer results: %s", e)
            return True, False
        finally:
            synchronizer.stop()
            timer.stop()

            sync_reader.join(0.5)

        return True, False

    def _fallback_condition(self, status: DataSourceStatus) -> bool:
        """
        Determine if we should fallback to secondary synchronizer.

        :param status: Current data source status
        :return: True if fallback condition is met
        """
        interrupted_at_runtime = (
            status.state == DataSourceState.INTERRUPTED
            and time.time() - status.since > 60  # 1 minute
        )
        cannot_initialize = (
            status.state == DataSourceState.INITIALIZING
            and time.time() - status.since > 10  # 10 seconds
        )

        return interrupted_at_runtime or cannot_initialize

    def _recovery_condition(self, status: DataSourceStatus) -> bool:
        """
        Determine if we should try to recover to primary synchronizer.

        :param status: Current data source status
        :return: True if recovery condition is met
        """
        interrupted_at_runtime = (
            status.state == DataSourceState.INTERRUPTED
            and time.time() - status.since > 60  # 1 minute
        )
        healthy_for_too_long = (
            status.state == DataSourceState.VALID
            and time.time() - status.since > 300  # 5 minutes
        )
        cannot_initialize = (
            status.state == DataSourceState.INITIALIZING
            and time.time() - status.since > 10  # 10 seconds
        )

        return interrupted_at_runtime or healthy_for_too_long or cannot_initialize

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
    def store(self) -> ReadOnlyStore:
        """Get the underlying store for flag evaluation."""
        return self._store.get_active_store()

    def set_flag_value_eval_fn(self, eval_fn):
        """
        Set the flag value evaluation function for the flag tracker.

        :param eval_fn: Function with signature (key: str, context: Context) -> Any
        """
        self._flag_tracker = FlagTrackerImpl(self._flag_change_listeners, eval_fn)

    @property
    def data_source_status_provider(self) -> DataSourceStatusProvider:
        """Get the data source status provider."""
        return self._data_source_status_provider

    @property
    def data_store_status_provider(self) -> DataStoreStatusProvider:
        """Get the data store status provider."""
        return self._data_store_status_provider

    @property
    def flag_tracker(self) -> FlagTracker:
        """Get the flag tracker for monitoring flag changes."""
        return self._flag_tracker

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
