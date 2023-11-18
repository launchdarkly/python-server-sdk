from ldclient.impl.listeners import Listeners
from ldclient.interfaces import DataSourceStatusProvider, DataSourceUpdateSink, DataSourceStatus, FeatureStore, DataSourceState, DataSourceErrorInfo, DataSourceErrorKind
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.versioned_data_kind import VersionedDataKind

import time
from typing import Callable, Mapping, Optional


class DataSourceUpdateSinkImpl(DataSourceUpdateSink):
    def __init__(self, store: FeatureStore, listeners: Listeners):
        self.__store = store
        self.__listeners = listeners

        self.__lock = ReadWriteLock()
        self.__status = DataSourceStatus(
            DataSourceState.INITIALIZING,
            time.time(),
            None
        )

    @property
    def status(self) -> DataSourceStatus:
        try:
            self.__lock.rlock()
            return self.__status
        finally:
            self.__lock.runlock()

    def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]):
        self.__monitor_store_update(lambda: self.__store.init(all_data))

    def upsert(self, kind: VersionedDataKind, item: dict):
        self.__monitor_store_update(lambda: self.__store.upsert(kind, item))

    def delete(self, kind: VersionedDataKind, key: str, version: int):
        self.__monitor_store_update(lambda: self.__store.delete(kind, key, version))

    def update_status(self, new_state: DataSourceState, new_error: Optional[DataSourceErrorInfo]):
        status_to_broadcast = None

        try:
            self.__lock.lock()
            old_status = self.__status

            if new_state == DataSourceState.INTERRUPTED and old_status.state == DataSourceState.INITIALIZING:
                new_state = DataSourceState.INITIALIZING

            if new_state == old_status.state and new_error is None:
                return

            self.__status = DataSourceStatus(
                new_state,
                self.__status.since if new_state == self.__status.state else time.time(),
                self.__status.error if new_error is None else new_error
            )

            status_to_broadcast = self.__status
        finally:
            self.__lock.unlock()

        if status_to_broadcast is not None:
            self.__listeners.notify(status_to_broadcast)

    def __monitor_store_update(self, fn: Callable[[], None]):
        try:
            fn()
        except Exception as e:
            error_info = DataSourceErrorInfo(
                DataSourceErrorKind.STORE_ERROR,
                0,
                time.time(),
                str(e)
            )
            self.update_status(DataSourceState.INTERRUPTED, error_info)
            raise


class DataSourceStatusProviderImpl(DataSourceStatusProvider):
    def __init__(self, listeners: Listeners, updates_sink: DataSourceUpdateSinkImpl):
        self.__listeners = listeners
        self.__updates_sink = updates_sink

    @property
    def status(self) -> DataSourceStatus:
        return self.__updates_sink.status

    def add_listener(self, listener: Callable[[DataSourceStatus], None]):
        self.__listeners.add(listener)

    def remove_listener(self, listener: Callable[[DataSourceStatus], None]):
        self.__listeners.remove(listener)
