"""
Support classes shared by the sync and async FDv2 data system coordinators.

These are synchronous (thread-based) components used identically by both
``FDv2`` and ``AsyncFDv2``: status providers, and the condition directive
enum.
"""

import time
from copy import copy
from enum import Enum
from typing import Callable, Optional

from ldclient.impl.datasystem import DataAvailability, DiagnosticAccumulator
from ldclient.impl.datasystem.store import _StoreBase
from ldclient.impl.listeners import Listeners
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.impl.util import log
from ldclient.interfaces import (
    DataSourceErrorInfo,
    DataSourceState,
    DataSourceStatus,
    DataSourceStatusProvider,
    DataStoreStatus,
    DataStoreStatusProvider,
    FeatureStore
)


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

    This wires up the listeners, the store, and the status providers, and it
    reports data availability. Following the same split as
    :class:`ldclient.impl.datasystem.store._StoreBase`, this base holds only the
    shared, store-agnostic wiring. Subclasses own the config, supply the store
    through :meth:`_create_store`, configure the optional persistent store, react
    to persistent-store recovery through
    :meth:`_persistent_store_outage_recovery`, and add their own concurrency
    primitives and the loops that run initializers and synchronizers.
    """

    # Set by subclasses from their config; read by ``data_availability``.
    _configured_with_data_sources: bool

    def __init__(self) -> None:
        # Diagnostic accumulator provided by the client for streaming metrics.
        self._diagnostic_accumulator: Optional[DiagnosticAccumulator] = None

        # Set up event listeners.
        self._flag_change_listeners = Listeners()
        self._change_set_listeners = Listeners()
        self._data_store_listeners = Listeners()

        self._data_store_listeners.add(self._persistent_store_outage_recovery)

        # Create the store; the subclass supplies the concrete type.
        self._store = self._create_store(self._flag_change_listeners, self._change_set_listeners)

        # Status providers. A child that has a persistent store replaces the
        # data store provider with one that wraps it.
        self._data_source_status_provider = DataSourceStatusProviderImpl(Listeners())
        self._data_store_status_provider = DataStoreStatusProviderImpl(None, self._data_store_listeners)

    def _create_store(self, flag_change_listeners: Listeners, change_set_listeners: Listeners) -> _StoreBase:
        """Create and return the coordinator's store."""
        raise NotImplementedError

    def _persistent_store_outage_recovery(self, data_store_status: DataStoreStatus) -> None:
        """
        Monitor the data store status. If the store comes online and potentially
        has stale data, write the known state back to it.
        """
        raise NotImplementedError

    def set_diagnostic_accumulator(self, diagnostic_accumulator: DiagnosticAccumulator):
        """
        Sets the diagnostic accumulator for streaming initialization metrics.
        This should be called before start() to ensure metrics are collected.
        """
        self._diagnostic_accumulator = diagnostic_accumulator

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

        if not self._configured_with_data_sources:
            return DataAvailability.CACHED

        try:
            store_initialized = self._store.is_initialized()
        except Exception as e:
            log.error("Error checking persistent store readiness; treating data as unavailable: %s", e)
            return DataAvailability.DEFAULTS

        return DataAvailability.CACHED if store_initialized else DataAvailability.DEFAULTS

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
    'fallback_condition',
    'recovery_condition',
]
