from threading import Event
from typing import Optional

from ldclient.config import Config
from ldclient.impl.datasource.feature_requester import FeatureRequesterImpl
from ldclient.impl.datasource.polling import PollingUpdateProcessor
from ldclient.impl.datasource.status import (
    DataSourceStatusProviderImpl,
    DataSourceUpdateSinkImpl
)
from ldclient.impl.datasource.streaming import StreamingUpdateProcessor
from ldclient.impl.datastore.status import (
    DataStoreStatusProviderImpl,
    DataStoreUpdateSinkImpl
)
from ldclient.impl.datasystem import DataAvailability
from ldclient.impl.flag_tracker import FlagTrackerImpl
from ldclient.impl.listeners import Listeners
from ldclient.impl.stubs import NullUpdateProcessor
from ldclient.interfaces import (
    DataSourceState,
    DataSourceStatus,
    DataSourceStatusProvider,
    DataStoreStatusProvider,
    FeatureStore,
    FlagTracker,
    UpdateProcessor
)

# Delayed import inside __init__ to avoid circular dependency with ldclient.client


class FDv1:
    """
    FDv1 wires the existing v1 data source and store behavior behind the
    generic DataSystem surface.
    """

    def __init__(self, config: Config):
        self._config = config

        # Set up data store plumbing
        self._data_store_listeners = Listeners()
        self._data_store_update_sink = DataStoreUpdateSinkImpl(
            self._data_store_listeners
        )
        # Import here to avoid circular import
        from ldclient.client import _FeatureStoreClientWrapper

        self._store_wrapper: FeatureStore = _FeatureStoreClientWrapper(
            self._config.feature_store, self._data_store_update_sink
        )
        self._data_store_status_provider_impl = DataStoreStatusProviderImpl(
            self._store_wrapper, self._data_store_update_sink
        )

        # Set up data source plumbing
        self._data_source_listeners = Listeners()
        self._flag_change_listeners = Listeners()
        self._flag_tracker_impl = FlagTrackerImpl(
            self._flag_change_listeners,
            lambda key, context: None,  # Replaced by client to use its evaluation method
        )
        self._data_source_update_sink = DataSourceUpdateSinkImpl(
            self._store_wrapper,
            self._data_source_listeners,
            self._flag_change_listeners,
        )
        self._data_source_status_provider_impl = DataSourceStatusProviderImpl(
            self._data_source_listeners, self._data_source_update_sink
        )

        # Ensure v1 processors can find the sink via config for status updates
        self._config._data_source_update_sink = self._data_source_update_sink

        # Update processor created in start(), because it needs the ready Event
        self._update_processor: Optional[UpdateProcessor] = None

        # Diagnostic accumulator provided by client for streaming metrics
        self._diagnostic_accumulator = None

        # Track current data availability
        self._data_availability: DataAvailability = (
            DataAvailability.CACHED
            if getattr(self._store_wrapper, "initialized", False)
            else DataAvailability.DEFAULTS
        )

        # React to data source status updates to adjust availability
        def _on_status_change(status: DataSourceStatus):
            if status.state == DataSourceState.VALID:
                self._data_availability = DataAvailability.REFRESHED

        self._data_source_status_provider_impl.add_listener(_on_status_change)

    def start(self, set_on_ready: Event):
        """
        Starts the v1 update processor and returns immediately. The provided
        Event is set by the processor upon first successful initialization or
        upon permanent failure.
        """
        update_processor = self._make_update_processor(
            self._config, self._store_wrapper, set_on_ready
        )
        self._update_processor = update_processor
        update_processor.start()

    def stop(self):
        if self._update_processor is not None:
            self._update_processor.stop()

    @property
    def store(self) -> FeatureStore:
        return self._store_wrapper

    def set_flag_value_eval_fn(self, eval_fn):
        """
        Injects the flag value evaluation function used by the flag tracker to
        compute FlagValueChange events. The function signature should be
        (key: str, context: Context) -> Any.
        """
        self._flag_tracker_impl = FlagTrackerImpl(self._flag_change_listeners, eval_fn)

    def set_diagnostic_accumulator(self, diagnostic_accumulator):
        """
        Sets the diagnostic accumulator for streaming initialization metrics.
        This should be called before start() to ensure metrics are collected.
        """
        self._diagnostic_accumulator = diagnostic_accumulator

    @property
    def data_source_status_provider(self) -> DataSourceStatusProvider:
        return self._data_source_status_provider_impl

    @property
    def data_store_status_provider(self) -> DataStoreStatusProvider:
        return self._data_store_status_provider_impl

    @property
    def flag_tracker(self) -> FlagTracker:
        return self._flag_tracker_impl

    @property
    def data_availability(self) -> DataAvailability:
        return self._data_availability

    @property
    def target_availability(self) -> DataAvailability:
        if self._config.offline:
            return DataAvailability.DEFAULTS
        # In LDD mode or normal connected modes, the ideal is to be refreshed
        return DataAvailability.REFRESHED

    def _make_update_processor(self, config: Config, store: FeatureStore, ready: Event):
        # Mirrors LDClient._make_update_processor but scoped for FDv1
        if config.update_processor_class:
            return config.update_processor_class(config, store, ready)

        if config.offline or config.use_ldd:
            return NullUpdateProcessor(config, store, ready)

        if config.stream:
            return StreamingUpdateProcessor(config, store, ready, self._diagnostic_accumulator)

        # Polling mode
        feature_requester = (
            config.feature_requester_class(config)
            if config.feature_requester_class is not None
            else FeatureRequesterImpl(config)
        )
        return PollingUpdateProcessor(config, feature_requester, store, ready)
