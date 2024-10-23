from __future__ import annotations

from copy import copy
from typing import TYPE_CHECKING, Callable

from ldclient.impl.listeners import Listeners
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.interfaces import (DataStoreStatus, DataStoreStatusProvider,
                                 DataStoreUpdateSink)

if TYPE_CHECKING:
    from ldclient.client import _FeatureStoreClientWrapper


class DataStoreUpdateSinkImpl(DataStoreUpdateSink):
    def __init__(self, listeners: Listeners):
        self.__listeners = listeners

        self.__lock = ReadWriteLock()
        self.__status = DataStoreStatus(True, False)

    @property
    def listeners(self) -> Listeners:
        return self.__listeners

    def status(self) -> DataStoreStatus:
        self.__lock.rlock()
        status = copy(self.__status)
        self.__lock.runlock()

        return status

    def update_status(self, status: DataStoreStatus):
        self.__lock.lock()
        old_value, self.__status = self.__status, status
        self.__lock.unlock()

        if old_value != status:
            self.__listeners.notify(status)


class DataStoreStatusProviderImpl(DataStoreStatusProvider):
    def __init__(self, store: _FeatureStoreClientWrapper, update_sink: DataStoreUpdateSinkImpl):
        self.__store = store
        self.__update_sink = update_sink

    @property
    def status(self) -> DataStoreStatus:
        return self.__update_sink.status()

    def is_monitoring_enabled(self) -> bool:
        return self.__store.is_monitoring_enabled()

    def add_listener(self, listener: Callable[[DataStoreStatus], None]):
        self.__update_sink.listeners.add(listener)

    def remove_listener(self, listener: Callable[[DataStoreStatus], None]):
        self.__update_sink.listeners.remove(listener)
