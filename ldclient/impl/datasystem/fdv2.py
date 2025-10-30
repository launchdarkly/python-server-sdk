import time
from threading import Event, Thread
from typing import Callable, List, Optional

from ldclient.config import Builder, DataSystemConfig
from ldclient.impl.datasourcev2.status import DataSourceStatusProviderImpl
from ldclient.impl.datasystem import DataAvailability, Synchronizer
from ldclient.impl.datasystem.store import Store
from ldclient.impl.flag_tracker import FlagTrackerImpl
from ldclient.impl.listeners import Listeners
from ldclient.impl.util import _Fail
from ldclient.interfaces import (
    DataSourceState,
    DataSourceStatus,
    DataSourceStatusProvider,
    DataStoreStatusProvider,
    FeatureStore,
    FlagTracker
)


class FDv2:
    """
    FDv2 is an implementation of the DataSystem interface that uses the Flag Delivery V2 protocol
    for obtaining and keeping data up-to-date. Additionally, it operates with an optional persistent
    store in read-only or read/write mode.
    """

    def __init__(
        self,
        config: DataSystemConfig,
        # # TODO: These next 2 parameters should be moved into the Config.
        # persistent_store: Optional[FeatureStore] = None,
        # store_writable: bool = True,
        disabled: bool = False,
    ):
        """
        Initialize a new FDv2 data system.

        :param config: Configuration for initializers and synchronizers
        :param persistent_store: Optional persistent store for data persistence
        :param store_writable: Whether the persistent store should be written to
        :param disabled: Whether the data system is disabled (offline mode)
        """
        self._config = config
        self._primary_synchronizer_builder: Optional[Builder[Synchronizer]] = config.primary_synchronizer
        self._secondary_synchronizer_builder = config.secondary_synchronizer
        self._fdv1_fallback_synchronizer_builder = config.fdv1_fallback_synchronizer
        self._disabled = disabled

        # Diagnostic accumulator provided by client for streaming metrics
        # TODO(fdv2): Either we need to use this, or we need to provide it to
        # the streaming synchronizers
        self._diagnostic_accumulator = None

        # Set up event listeners
        self._flag_change_listeners = Listeners()
        self._change_set_listeners = Listeners()

        # Create the store
        self._store = Store(self._flag_change_listeners, self._change_set_listeners)

        # Status providers
        self._data_source_status_provider = DataSourceStatusProviderImpl(Listeners())

        # # Configure persistent store if provided
        # if persistent_store is not None:
        #     self._store.with_persistence(
        #         persistent_store, store_writable, self._data_source_status_provider
        #     )
        #
        # Flag tracker (evaluation function set later by client)
        self._flag_tracker = FlagTrackerImpl(
            self._flag_change_listeners,
            lambda key, context: None  # Placeholder, replaced by client
        )

        # Threading
        self._stop_event = Event()
        self._threads: List[Thread] = []

        # Track configuration
        # TODO: What is the point of checking if primary_synchronizer is not
        # None? Doesn't it have to be set?
        self._configured_with_data_sources = (
            (config.initializers is not None and len(config.initializers) > 0)
            or config.primary_synchronizer is not None
        )

    def start(self, set_on_ready: Event):
        """
        Start the FDv2 data system.

        :param set_on_ready: Event to set when the system is ready or has failed
        """
        if self._disabled:
            print("Data system is disabled, SDK will return application-defined default values")
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

        # Wait for all threads to complete
        for thread in self._threads:
            if thread.is_alive():
                thread.join(timeout=5.0)  # 5 second timeout

        # Close the store
        self._store.close()

    def set_diagnostic_accumulator(self, diagnostic_accumulator):
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

            # # If we have persistent store with status monitoring, start recovery monitoring
            # if (
            #     self._configured_with_data_sources
            #     and self._data_store_status_provider is not None
            #     and hasattr(self._data_store_status_provider, 'add_listener')
            # ):
            #     recovery_thread = Thread(
            #         target=self._run_persistent_store_outage_recovery,
            #         name="FDv2-store-recovery",
            #         daemon=True
            #     )
            #     recovery_thread.start()
            #     self._threads.append(recovery_thread)

            # Run synchronizers
            self._run_synchronizers(set_on_ready)

        except Exception as e:
            print(f"Error in FDv2 main loop: {e}")
            # Ensure ready event is set even on error
            if not set_on_ready.is_set():
                set_on_ready.set()

    def _run_initializers(self, set_on_ready: Event):
        """Run initializers to get initial data."""
        if self._config.initializers is None:
            return

        for initializer_builder in self._config.initializers:
            if self._stop_event.is_set():
                return

            try:
                initializer = initializer_builder()
                print(f"Attempting to initialize via {initializer.name}")

                basis_result = initializer.fetch()

                if isinstance(basis_result, _Fail):
                    print(f"Initializer {initializer.name} failed: {basis_result.error}")
                    continue

                basis = basis_result.value
                print(f"Initialized via {initializer.name}")

                # Apply the basis to the store
                self._store.apply(basis.change_set, basis.persist)

                # Set ready event
                if not set_on_ready.is_set():
                    set_on_ready.set()
            except Exception as e:
                print(f"Initializer failed with exception: {e}")

    def _run_synchronizers(self, set_on_ready: Event):
        """Run synchronizers to keep data up-to-date."""
        # If no primary synchronizer configured, just set ready and return
        if self._config.primary_synchronizer is None:
            if not set_on_ready.is_set():
                set_on_ready.set()
            return

        def synchronizer_loop(self: 'FDv2'):
            try:
                # Always ensure ready event is set when we exit
                while not self._stop_event.is_set() and self._primary_synchronizer_builder is not None:
                    # Try primary synchronizer
                    try:
                        primary_sync = self._primary_synchronizer_builder()
                        print(f"Primary synchronizer {primary_sync.name} is starting")

                        remove_sync, fallback_v1 = self._consume_synchronizer_results(
                            primary_sync, set_on_ready, self._fallback_condition
                        )

                        if remove_sync:
                            self._primary_synchronizer_builder = self._secondary_synchronizer_builder
                            self._secondary_synchronizer_builder = None

                            if fallback_v1:
                                self._primary_synchronizer_builder = self._fdv1_fallback_synchronizer_builder

                            if self._primary_synchronizer_builder is None:
                                print("No more synchronizers available")
                                self._data_source_status_provider.update_status(
                                    DataSourceState.OFF,
                                    self._data_source_status_provider.status.error
                                )
                                break
                        else:
                            print("Fallback condition met")

                        if self._secondary_synchronizer_builder is None:
                            continue

                        secondary_sync = self._secondary_synchronizer_builder()
                        print(f"Secondary synchronizer {secondary_sync.name} is starting")

                        remove_sync, fallback_v1 = self._consume_synchronizer_results(
                            secondary_sync, set_on_ready, self._recovery_condition
                        )

                        if remove_sync:
                            self._secondary_synchronizer_builder = None
                            if fallback_v1:
                                self._primary_synchronizer_builder = self._fdv1_fallback_synchronizer_builder

                            if self._primary_synchronizer_builder is None:
                                print("No more synchronizers available")
                                self._data_source_status_provider.update_status(
                                    DataSourceState.OFF,
                                    self._data_source_status_provider.status.error
                                )
                                # TODO: WE might need to also set that threading.Event here
                                break

                        print("Recovery condition met, returning to primary synchronizer")
                    except Exception as e:
                        print(f"Failed to build primary synchronizer: {e}")
                        break

            except Exception as e:
                print(f"Error in synchronizer loop: {e}")
            finally:
                # Ensure we always set the ready event when exiting
                if not set_on_ready.is_set():
                    set_on_ready.set()

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
        try:
            for update in synchronizer.sync():
                print(f"Synchronizer {synchronizer.name} update: {update.state}")
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

                # Check for OFF state indicating permanent failure
                if update.state == DataSourceState.OFF:
                    return True, update.revert_to_fdv1

                # Check condition periodically
                current_status = self._data_source_status_provider.status
                if condition_func(current_status):
                    return False, False

        except Exception as e:
            print(f"Error consuming synchronizer results: {e}")
            return True, False

        return True, False

    # def _run_persistent_store_outage_recovery(self):
    #     """Monitor persistent store status and trigger recovery when needed."""
    #     # This is a simplified version - in a full implementation we'd need
    #     # to properly monitor store status and trigger commit operations
    #     # when the store comes back online after an outage
    #     pass
    #
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

    @property
    def store(self) -> FeatureStore:
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
        raise NotImplementedError
        # return self._data_store_status_provider

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
