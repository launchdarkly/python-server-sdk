"""
Support classes shared by the sync and async FDv2 data system coordinators.

These are synchronous (thread-based) components used identically by both
``FDv2`` and ``AsyncFDv2``: status providers, the persistent-store wrapper,
and the condition directive enum.
"""

import time
from copy import copy
from enum import Enum
from typing import Any, Callable, Dict, List, Mapping, Optional

from ldclient.config import DataSourceBuilder, DataSystemConfig
from ldclient.feature_store import _FeatureStoreDataSetSorter
from ldclient.impl.datasystem import DataAvailability, DiagnosticAccumulator
from ldclient.impl.datasystem.store import Store
from ldclient.impl.listeners import Listeners
from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.impl.util import log
from ldclient.interfaces import (
    DataSourceErrorInfo,
    DataSourceState,
    DataSourceStatus,
    DataSourceStatusProvider,
    DataStoreMode,
    DataStoreStatus,
    DataStoreStatusProvider,
    FeatureStore
)
from ldclient.versioned_data_kind import VersionedDataKind


class DataSourceStatusProviderImpl(DataSourceStatusProvider):
    def __init__(self, listeners: Listeners):
        self.__listeners = listeners
        self.__status = DataSourceStatus(DataSourceState.INITIALIZING, time.time(), None)
        self.__lock = ReadWriteLock()

    @property
    def status(self) -> DataSourceStatus:
        with self.__lock.read():
            return self.__status

    def update_status(self, new_state: DataSourceState, new_error: Optional[DataSourceErrorInfo]):
        status_to_broadcast = None

        with self.__lock.write():
            old_status = self.__status

            if new_state == DataSourceState.INTERRUPTED and old_status.state == DataSourceState.INITIALIZING:
                new_state = DataSourceState.INITIALIZING

            if new_state == old_status.state and new_error is None:
                return

            new_since = self.__status.since if new_state == self.__status.state else time.time()
            new_error = self.__status.error if new_error is None else new_error

            self.__status = DataSourceStatus(new_state, new_since, new_error)

            status_to_broadcast = self.__status

        if status_to_broadcast is not None:
            self.__listeners.notify(status_to_broadcast)

    def add_listener(self, listener: Callable[[DataSourceStatus], None]):
        self.__listeners.add(listener)

    def remove_listener(self, listener: Callable[[DataSourceStatus], None]):
        self.__listeners.remove(listener)


class DataStoreStatusProviderImpl(DataStoreStatusProvider):
    def __init__(self, store: Optional[FeatureStore], listeners: Listeners):
        self.__store = store
        self.__listeners = listeners

        self.__lock = ReadWriteLock()
        self.__status = DataStoreStatus(True, False)

    def update_status(self, status: DataStoreStatus):
        """
        update_status is called from the data store to push a status update.
        """
        modified = False

        with self.__lock.write():
            if self.__status != status:
                self.__status = status
                modified = True

        if modified:
            self.__listeners.notify(status)

    @property
    def status(self) -> DataStoreStatus:
        with self.__lock.read():
            return copy(self.__status)

    def is_monitoring_enabled(self) -> bool:
        if self.__store is None:
            return False
        if hasattr(self.__store, "is_monitoring_enabled") is False:
            return False

        return self.__store.is_monitoring_enabled()  # type: ignore

    def add_listener(self, listener: Callable[[DataStoreStatus], None]):
        self.__listeners.add(listener)

    def remove_listener(self, listener: Callable[[DataStoreStatus], None]):
        self.__listeners.remove(listener)


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


class ConditionDirective(str, Enum):
    """
    ConditionDirective represents the possible directives that can be returned from a condition check.
    """

    REMOVE = "remove"
    """
    REMOVE suggests that the current data source should be permanently removed from consideration.
    """

    FALLBACK = "fallback"
    """
    FALLBACK suggests that this data source should be abandoned in favor of the next one.
    """

    RECOVER = "recover"
    """
    RECOVER suggests that we should try to return to the primary data source.
    """

    FDV1 = "fdv1"
    """
    FDV1 suggests that we should immediately fall back to the FDv1 Fallback Synchronizer.
    """


def fallback_condition(status: DataSourceStatus) -> bool:
    """
    Determine if we should fallback to the next synchronizer in the list.
    This applies at any position in the synchronizers list.

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


def recovery_condition(status: DataSourceStatus) -> bool:
    """
    Determine if we should try to recover to the first (preferred) synchronizer.
    This only applies when not already at the first synchronizer (index > 0).

    :param status: Current data source status
    :return: True if recovery condition is met
    """
    healthy_for_too_long = (
        status.state == DataSourceState.VALID
        and time.time() - status.since > 300  # 5 minutes
    )

    return healthy_for_too_long


class _FDv2Base:
    """
    Common construction and read-only accessors for the FDv2 data system
    coordinators.

    This wires up the listeners, the in-memory store, the status providers, and
    the optional persistent store, and it reports data availability. Subclasses
    add their own concurrency primitives and the loops that run initializers and
    synchronizers.
    """

    def __init__(self, config, data_system_config: DataSystemConfig):
        self._config = config
        self._data_system_config = data_system_config
        self._synchronizers: List[DataSourceBuilder] = list(data_system_config.synchronizers) if data_system_config.synchronizers else []
        self._fdv1_fallback_synchronizer_builder = data_system_config.fdv1_fallback_synchronizer
        self._disabled = config.offline

        # Diagnostic accumulator provided by the client for streaming metrics.
        self._diagnostic_accumulator: Optional[DiagnosticAccumulator] = None

        # Set up event listeners.
        self._flag_change_listeners = Listeners()
        self._change_set_listeners = Listeners()
        self._data_store_listeners = Listeners()

        self._data_store_listeners.add(self._persistent_store_outage_recovery)

        # Create the store.
        self._store = Store(self._flag_change_listeners, self._change_set_listeners)

        # Status providers.
        self._data_source_status_provider = DataSourceStatusProviderImpl(Listeners())
        self._data_store_status_provider = DataStoreStatusProviderImpl(None, self._data_store_listeners)

        # Configure the persistent store if one is provided.
        if self._data_system_config.data_store is not None:
            self._data_store_status_provider = DataStoreStatusProviderImpl(self._data_system_config.data_store, self._data_store_listeners)
            writable = self._data_system_config.data_store_mode == DataStoreMode.READ_WRITE
            wrapper = FeatureStoreClientWrapper(self._data_system_config.data_store, self._data_store_status_provider)
            self._store.with_persistence(
                wrapper, writable, self._data_store_status_provider
            )

        # Track configuration.
        self._configured_with_data_sources = (
            (data_system_config.initializers is not None and len(data_system_config.initializers) > 0)
            or len(self._synchronizers) > 0
        )

    def set_diagnostic_accumulator(self, diagnostic_accumulator: DiagnosticAccumulator):
        """
        Sets the diagnostic accumulator for streaming initialization metrics.
        This should be called before start() to ensure metrics are collected.
        """
        self._diagnostic_accumulator = diagnostic_accumulator

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
    'ConditionDirective',
    'DataSourceStatusProviderImpl',
    'DataStoreStatusProviderImpl',
    'FeatureStoreClientWrapper',
    'fallback_condition',
    'recovery_condition',
]
