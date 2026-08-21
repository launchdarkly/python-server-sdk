from threading import Event
from typing import Any, Callable, Dict, Mapping, Optional

from ldclient.config import Config
from ldclient.feature_store import _FeatureStoreDataSetSorter
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
from ldclient.impl.datasystem import (
    DataAvailability,
    DataSystem,
    DiagnosticAccumulator
)
from ldclient.impl.datasystem.store import _decode
from ldclient.impl.listeners import Listeners
from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.impl.stubs import NullUpdateProcessor
from ldclient.impl.util import log
from ldclient.interfaces import (
    DataSourceStatusProvider,
    DataStoreStatus,
    DataStoreStatusProvider,
    DataStoreUpdateSink,
    FeatureStore,
    ReadOnlyStore,
    UpdateProcessor
)
from ldclient.versioned_data_kind import VersionedDataKind


class _FeatureStoreClientWrapper(FeatureStore):
    """Provides additional behavior that the client requires before or after feature store operations.
    Currently this just means sorting the data set for init() and dealing with data store status listeners.
    """

    def __init__(self, store: FeatureStore, store_update_sink: DataStoreUpdateSink):
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
        with self.__lock.write():
            if available == self.__last_available:
                return
            self.__last_available = available

        status = DataStoreStatus(available, False)

        if available:
            log.warn("Persistent store is available again")

        self.__store_update_sink.update_status(status)

        if available:
            with self.__lock.write():
                if self.__poller is not None:
                    self.__poller.stop()
                    self.__poller = None

            return

        log.warn("Detected persistent store unavailability; updates will be cached until it recovers")
        task = RepeatingTask("ldclient.check-availability", 0.5, 0, self.__check_availability)

        with self.__lock.write():
            self.__poller = task
            self.__poller.start()

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


class _ReadOnlyFeatureStoreView(ReadOnlyStore):
    """Read-only view of a feature store.

    Serves every read from the wrapped store. Items that a custom feature store
    keeps as raw dicts are decoded into model objects; items that are already
    models pass through unchanged.
    """

    def __init__(self, store: FeatureStore):
        self._store = store

    def get(self, kind: VersionedDataKind, key: str, callback: Callable[[Any], Any] = lambda x: x) -> Any:
        item = self._store.get(kind, key, lambda x: x)
        return callback(_decode(kind, item))

    def all(self, kind: VersionedDataKind, callback: Callable[[Any], Any] = lambda x: x) -> Any:
        items = self._store.all(kind, lambda x: x)
        return callback({key: _decode(kind, value) for key, value in items.items()})

    @property
    def initialized(self) -> bool:
        return self._store.initialized


class FDv1(DataSystem):
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
        self._store_wrapper: FeatureStore = _FeatureStoreClientWrapper(
            self._config.feature_store, self._data_store_update_sink
        )
        self._store_view = _ReadOnlyFeatureStoreView(self._store_wrapper)
        self._data_store_status_provider_impl = DataStoreStatusProviderImpl(
            self._store_wrapper, self._data_store_update_sink
        )

        # Set up data source plumbing
        self._data_source_listeners = Listeners()
        self._flag_change_listeners = Listeners()
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
        self._diagnostic_accumulator: Optional[DiagnosticAccumulator] = None

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
    def store(self) -> ReadOnlyStore:
        return self._store_view

    @property
    def environment_id(self) -> Optional[str]:
        return self._data_source_update_sink.environment_id

    def set_diagnostic_accumulator(self, diagnostic_accumulator: DiagnosticAccumulator):
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
    def flag_change_listeners(self) -> Listeners:
        return self._flag_change_listeners

    @property
    def data_availability(self) -> DataAvailability:
        if self._config.offline:
            return DataAvailability.DEFAULTS

        if self._update_processor is not None and self._update_processor.initialized():
            return DataAvailability.REFRESHED

        if self._store_wrapper.initialized:
            return DataAvailability.CACHED

        return DataAvailability.DEFAULTS

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
