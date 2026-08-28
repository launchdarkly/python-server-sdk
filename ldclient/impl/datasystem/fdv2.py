import time
from queue import Queue
from threading import Event, Thread
from typing import Any, Callable, Dict, List, Mapping, Optional

from ldclient.config import Config, DataSourceBuilder, DataSystemConfig
from ldclient.feature_store import _FeatureStoreDataSetSorter
from ldclient.impl.datasystem import DataSystem, DiagnosticSource
from ldclient.impl.datasystem.fdv2_common import (
    ConditionDirective,
    DataSourceStatusProviderImpl,
    DataStoreStatusProviderImpl,
    _FDv2Base,
    fallback_condition,
    recovery_condition
)
from ldclient.impl.datasystem.store import Store, _decode
from ldclient.impl.listeners import Listeners
from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.impl.util import _LD_FD_FALLBACK_HEADER, _Fail, log
from ldclient.interfaces import (
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    DataStoreMode,
    DataStoreStatus,
    FeatureStore,
    ReadOnlyStore,
    Synchronizer
)
from ldclient.versioned_data_kind import VersionedDataKind


class _ReadOnlyStoreView(ReadOnlyStore):
    """Read-only view of the data system store.

    Serves every read from the store's active store, so a held instance follows
    the swap from the persistent store to the in-memory store once it has data.
    Items that a custom persistent store keeps as raw dicts are decoded into
    model objects; items that are already models pass through unchanged.
    """

    def __init__(self, store: Store):
        self._store = store

    def get(self, kind: VersionedDataKind, key: str, callback: Callable[[Any], Any] = lambda x: x) -> Any:
        item = self._store.get_active_store().get(kind, key, lambda x: x)
        return callback(_decode(kind, item))

    def all(self, kind: VersionedDataKind, callback: Callable[[Any], Any] = lambda x: x) -> Any:
        items = self._store.get_active_store().all(kind, lambda x: x)
        return callback({key: _decode(kind, value) for key, value in items.items()})

    @property
    def initialized(self) -> bool:
        return self._store.is_initialized()


class _FeatureStoreClientWrapper(FeatureStore):
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
        self.__closed = False

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

    def disable_cache(self) -> None:
        def _do_disable():
            try:
                inner = self.store
                if hasattr(inner, "disable_cache"):
                    inner.disable_cache()  # type: ignore[attr-defined]
            except Exception as e:
                log.warning("disable_cache failed on inner store: %s", e)

        self.__wrapper(_do_disable)

    def __wrapper(self, fn: Callable):
        try:
            return fn()
        except BaseException:
            if self.__monitoring_enabled:
                self.__update_availability(False)
            raise

    def __update_availability(self, available: bool):
        state_changed = False
        poller_to_stop = None
        task_to_start = None

        with self.__lock.write():
            if self.__closed:
                return
            if available == self.__last_available:
                return

            state_changed = True
            self.__last_available = available

            if available:
                poller_to_stop = self.__poller
                self.__poller = None
            elif self.__poller is None:
                task_to_start = RepeatingTask("ldclient.check-availability", 0.5, 0, self.__check_availability)
                self.__poller = task_to_start

        if available:
            log.warning("Persistent store is available again")
        else:
            log.warning("Detected persistent store unavailability; updates will be cached until it recovers")

        status = DataStoreStatus(available, True)
        self.__store_update_sink.update_status(status)

        if poller_to_stop is not None:
            poller_to_stop.stop()

        if task_to_start is not None:
            task_to_start.start()

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

    def close(self):
        """
        Close the wrapper and stop the repeating task poller if it's running.
        Also forwards the close call to the underlying store if it has a close method.
        """
        poller_to_stop = None

        with self.__lock.write():
            if self.__closed:
                return
            self.__closed = True
            poller_to_stop = self.__poller
            self.__poller = None

        if poller_to_stop is not None:
            poller_to_stop.stop()

        if hasattr(self.store, "close"):
            self.store.close()


class FDv2(_FDv2Base, DataSystem):
    """
    FDv2 is an implementation of the DataSystem interface that uses the Flag Delivery V2 protocol
    for obtaining and keeping data up-to-date. Additionally, it operates with an optional persistent
    store in read-only or read/write mode.
    """

    _store: Store

    def __init__(
        self,
        config: Config,
        data_system_config: DataSystemConfig,
    ):
        """
        Initialize a new FDv2 data system.

        :param config: the SDK configuration
        :param data_system_config: the data system configuration — initializers,
            synchronizers, and the optional persistent store
        """
        super().__init__()

        self._config = config
        self._data_system_config = data_system_config
        self._synchronizers: List[DataSourceBuilder[Synchronizer]] = list(data_system_config.synchronizers) if data_system_config.synchronizers else []
        self._fdv1_fallback_synchronizer_builder = data_system_config.fdv1_fallback_synchronizer
        self._disabled = config.offline
        self._configured_with_data_sources = (
            (data_system_config.initializers is not None and len(data_system_config.initializers) > 0)
            or len(self._synchronizers) > 0
        )

        if data_system_config.data_store is not None:
            self._data_store_status_provider = DataStoreStatusProviderImpl(data_system_config.data_store, self._data_store_listeners)
            writable = data_system_config.data_store_mode == DataStoreMode.READ_WRITE
            wrapper = _FeatureStoreClientWrapper(data_system_config.data_store, self._data_store_status_provider)
            self._store.with_persistence(wrapper, writable, self._data_store_status_provider)

        # Threading
        self._stop_event = Event()
        self._lock = ReadWriteLock()
        self._active_synchronizer: Optional[Synchronizer] = None
        self._threads: List[Thread] = []
        self._environment_id: Optional[str] = None

        self._store_view = _ReadOnlyStoreView(self._store)

    def _create_store(self, flag_change_listeners: Listeners, change_set_listeners: Listeners) -> Store:
        return Store(flag_change_listeners, change_set_listeners)

    def _persistent_store_outage_recovery(self, data_store_status: DataStoreStatus) -> None:
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

        with self._lock.write():
            if self._active_synchronizer is not None:
                try:
                    self._active_synchronizer.stop()
                except Exception as e:
                    log.error("Error stopping active data source: %s", e)

        # Wait for all threads to complete
        for thread in self._threads:
            if thread.is_alive():
                thread.join(timeout=5.0)  # 5 second timeout
                if thread.is_alive():
                    log.warning("Thread %s did not terminate in time", thread.name)

        # Close the store
        self._store.close()

    def _run_main_loop(self, set_on_ready: Event):
        """Main coordination loop that manages initializers and synchronizers."""
        try:
            self._data_source_status_provider.update_status(
                DataSourceState.INITIALIZING, None
            )

            # Run initializers first
            fallback_requested = self._run_initializers(set_on_ready)

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
            self._run_synchronizers(set_on_ready)

        except Exception as e:
            log.error("Error in FDv2 main loop: %s", e)
            # Ensure ready event is set even on error
            if not set_on_ready.is_set():
                set_on_ready.set()

    def _run_initializers(self, set_on_ready: Event) -> bool:
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

                basis_result = initializer.fetch(self._store)

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

                self._record_environment_id(basis.environment_id)

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

    def _run_synchronizers(self, set_on_ready: Event):
        """Run synchronizers to keep data up-to-date."""
        # If no synchronizers configured, just set ready and return
        if len(self._synchronizers) == 0:
            set_on_ready.set()
            return

        def synchronizer_loop(self: 'FDv2'):
            try:
                # Make a working copy of the synchronizers list
                synchronizers_list = list(self._synchronizers)
                current_index = 0

                # Always ensure ready event is set when we exit
                while not self._stop_event.is_set() and len(synchronizers_list) > 0:
                    try:
                        with self._lock.write():
                            synchronizer: Synchronizer = synchronizers_list[current_index].build(self._config)
                            self._active_synchronizer = synchronizer
                            if isinstance(synchronizer, DiagnosticSource) and self._diagnostic_accumulator is not None:
                                synchronizer.set_diagnostic_accumulator(self._diagnostic_accumulator)

                        log.info("Synchronizer %s (index %d) is starting", synchronizer.name, current_index)

                        directive = self._consume_synchronizer_results(
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
                with self._lock.write():
                    if self._active_synchronizer is not None:
                        self._active_synchronizer.stop()
                    self._active_synchronizer = None

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
        check_recovery: bool,
    ) -> ConditionDirective:
        """
        Consume results from a synchronizer until a condition is met or it fails.

        :return: the ConditionDirective describing how to proceed
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
                        if check_recovery and recovery_condition(current_status):
                            return ConditionDirective.RECOVER
                        if fallback_condition(current_status):
                            return ConditionDirective.FALLBACK
                    continue

                log.info("Synchronizer %s update: %s", synchronizer.name, update.state)
                if self._stop_event.is_set():
                    return ConditionDirective.FALLBACK

                if update.state == DataSourceState.VALID:
                    self._record_environment_id(update.environment_id)

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
            synchronizer.stop()
            timer.stop()

            sync_reader.join(0.5)

        # If we reach here, the synchronizer's iterator completed normally (no more updates)
        # For continuous synchronizers (streaming/polling), this is unexpected and indicates
        # the synchronizer can't provide more updates, so we should remove it and fall back
        return ConditionDirective.REMOVE

    def _record_environment_id(self, environment_id: Optional[str]):
        if not isinstance(environment_id, str) or environment_id == '':
            return

        with self._lock.write():
            self._environment_id = environment_id

    @property
    def environment_id(self) -> Optional[str]:
        """Get the environment ID reported by LaunchDarkly, if known."""
        with self._lock.read():
            return self._environment_id

    @property
    def store(self) -> ReadOnlyStore:
        """Get the underlying store for flag evaluation."""
        return self._store_view


__all__ = [
    'ConditionDirective',
    'DataSourceStatusProviderImpl',
    'DataStoreStatusProviderImpl',
    'FDv2',
]
