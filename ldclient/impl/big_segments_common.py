"""
Shared, I/O-free big-segments status provider used by both the sync
:mod:`ldclient.impl.big_segments` and the async
:mod:`ldclient.impl.async_big_segments`. It holds the last known status and
notifies listeners; nothing here touches the store or network, so it is
identical across the two managers.
"""

from typing import Callable, Optional

from ldclient.impl.listeners import Listeners
from ldclient.interfaces import (
    BigSegmentStoreStatus,
    BigSegmentStoreStatusProvider
)


class BigSegmentStoreStatusProviderImpl(BigSegmentStoreStatusProvider):
    """
    Default implementation of the BigSegmentStoreStatusProvider interface.

    The real implementation of getting the status is in the big segment store manager - we pass in a lambda that
    allows us to get the current status from that class. So this class provides a facade for that, and
    also adds the listener mechanism.
    """

    def __init__(self, status_getter: Callable[[], BigSegmentStoreStatus]):
        self.__status_getter = status_getter
        self.__status_listeners = Listeners()
        self.__last_status = None  # type: Optional[BigSegmentStoreStatus]

    @property
    def status(self) -> BigSegmentStoreStatus:
        return self.__status_getter()

    def add_listener(self, listener: Callable[[BigSegmentStoreStatus], None]) -> None:
        self.__status_listeners.add(listener)

    def remove_listener(self, listener: Callable[[BigSegmentStoreStatus], None]) -> None:
        self.__status_listeners.remove(listener)

    def _update_status(self, new_status: BigSegmentStoreStatus):
        last = self.__last_status
        if last is None:
            self.__last_status = new_status
        elif new_status.available != last.available or new_status.stale != last.stale:
            self.__last_status = new_status
            self.__status_listeners.notify(new_status)
