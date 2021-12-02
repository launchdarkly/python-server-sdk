from ldclient.config import BigSegmentsConfig
from ldclient.evaluation import BigSegmentsStatus
from ldclient.impl.listeners import Listeners
from ldclient.impl.repeating_task import RepeatingTask
from ldclient.interfaces import BigSegmentStoreStatus, BigSegmentStoreStatusProvider
from ldclient.util import log

import base64
from expiringdict import ExpiringDict
from hashlib import md5
import time
from typing import Callable, Optional, Tuple


class BigSegmentStoreStatusProviderImpl(BigSegmentStoreStatusProvider):
    """
    Default implementation of the BigSegmentStoreStatusProvider interface.
    
    The real implementation of getting the status is in BigSegmentStoreManager - we pass in a lambda that
    allows us to get the current status from that class. So this class provides a facade for that, and
    also adds the listener mechanism.
    """
    def __init__(self, status_getter: Callable[[], BigSegmentStoreStatus]):
        self.__status_getter = status_getter
        self.__status_listeners = Listeners()
        self.__last_status: Optional[BigSegmentStoreStatus] = None
    
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

class BigSegmentStoreManager:
    """
    Internal component that decorates the Big Segment store with caching behavior, and also polls the
    store to track its status.
    """
    def __init__(self, config: BigSegmentsConfig):
        self.__store = config.store

        self.__stale_after_millis = config.stale_after * 1000
        self.__status_provider = BigSegmentStoreStatusProviderImpl(self.get_status)
        self.__last_status: Optional[BigSegmentStoreStatus] = None
        self.__poll_task: Optional[RepeatingTask] = None

        if self.__store:
            self.__cache = ExpiringDict(max_len = config.user_cache_size, max_age_seconds=config.user_cache_size)
            self.__poll_task = RepeatingTask(config.status_poll_interval, 0, self.poll_store_and_update_status)
            self.__poll_task.start()

    def stop(self):
        if self.__poll_task:
            self.__poll_task.stop()
        if self.__store:
            self.__store.stop()

    @property
    def status_provider(self) -> BigSegmentStoreStatusProvider:
        return self.__status_provider
    
    def get_user_membership(self, user_key: str) -> Tuple[Optional[dict], str]:
        if not self.__store:
            return (None, BigSegmentsStatus.NOT_CONFIGURED)
        membership = self.__cache.get(user_key)
        if membership is None:
            try:
                membership = self.__store.get_membership(_hash_for_user_key(user_key))
                self.__cache[user_key] = membership
            except Exception as e:
                log.exception("Big Segment store membership query returned error: %s" % e)
        status = self.__last_status
        if not status:
            status = self.poll_store_and_update_status()
        if not status.available:
            return (membership, BigSegmentsStatus.STORE_ERROR)
        return (membership, BigSegmentsStatus.STALE if status.stale else BigSegmentsStatus.HEALTHY)

    def get_status(self) -> BigSegmentStoreStatus:
        status = self.__last_status
        return status if status else self.poll_store_and_update_status()

    def poll_store_and_update_status(self) -> BigSegmentStoreStatus:
        new_status = BigSegmentStoreStatus(False, False) # default to "unavailable" if we don't get a new status below
        if self.__store:
            try:
                metadata = self.__store.get_metadata()
                new_status = BigSegmentStoreStatus(True, (metadata is None) or self.is_stale(metadata.last_up_to_date))
            except Exception as e:
                log.exception("Big Segment store status query returned error: %s" % e)
        self.__last_status = new_status
        self.__status_provider._update_status(new_status)
        return new_status

    def is_stale(self, timestamp) -> bool:
        return (timestamp is None) or ((int(time.time() * 1000) - timestamp) >= self.__stale_after_millis)

def _hash_for_user_key(user_key: str) -> str:
    return base64.b64encode(md5(user_key.encode('utf-8')).digest()).decode('utf-8')
