import time
from typing import Callable, Optional

from ldclient.impl.listeners import Listeners
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.interfaces import (
    DataSourceErrorInfo,
    DataSourceState,
    DataSourceStatus,
    DataSourceStatusProvider
)


class DataSourceStatusProviderImpl(DataSourceStatusProvider):
    def __init__(self, listeners: Listeners):
        self.__listeners = listeners
        self.__status = DataSourceStatus(DataSourceState.INITIALIZING, 0, None)
        self.__lock = ReadWriteLock()

    @property
    def status(self) -> DataSourceStatus:
        self.__lock.rlock()
        status = self.__status
        self.__lock.runlock()

        return status

    def update_status(self, new_state: DataSourceState, new_error: Optional[DataSourceErrorInfo]):
        status_to_broadcast = None

        try:
            self.__lock.lock()
            old_status = self.__status

            if new_state == DataSourceState.INTERRUPTED and old_status.state == DataSourceState.INITIALIZING:
                new_state = DataSourceState.INITIALIZING

            if new_state == old_status.state and new_error is None:
                return

            new_since = self.__status.since if new_state == self.__status.state else time.time()
            new_error = self.__status.error if new_error is None else new_error

            self.__status = DataSourceStatus(new_state, new_since, new_error)

            status_to_broadcast = self.__status
        finally:
            self.__lock.unlock()

        if status_to_broadcast is not None:
            self.__listeners.notify(status_to_broadcast)

    def add_listener(self, listener: Callable[[DataSourceStatus], None]):
        self.__listeners.add(listener)

    def remove_listener(self, listener: Callable[[DataSourceStatus], None]):
        self.__listeners.remove(listener)
