import base64
import time
from hashlib import sha256
from typing import Callable, Optional, Tuple

from expiringdict import ExpiringDict

from ldclient.async_config import AsyncBigSegmentsConfig
from ldclient.evaluation import BigSegmentsStatus
from ldclient.impl.aio.concurrency import AsyncRepeatingTask
from ldclient.impl.listeners import Listeners
from ldclient.impl.util import log
from ldclient.interfaces import (
    BigSegmentStoreStatus,
    BigSegmentStoreStatusProvider
)


class BigSegmentStoreStatusProviderImpl(BigSegmentStoreStatusProvider):
    """
    Default implementation of the BigSegmentStoreStatusProvider interface.

    The real implementation of getting the status is in AsyncBigSegmentStoreManager - we pass in a lambda that
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


class AsyncBigSegmentStoreManager:
    # use EMPTY_MEMBERSHIP as a singleton whenever a membership query returns None; it's safe to reuse it
    # because we will never modify the membership properties after they're queried
    EMPTY_MEMBERSHIP = {}  # type: dict

    """
    Internal component that decorates the Big Segment store with caching behavior, and also polls the
    store to track its status. The constructor starts the polling task.
    """

    # Because the constructor starts the polling task, it must run within a running event loop.
    def __init__(self, config: AsyncBigSegmentsConfig):
        self.__store = config.store

        self.__stale_after_millis = config.stale_after * 1000
        self.__status_provider = BigSegmentStoreStatusProviderImpl(self.get_status)
        self.__last_status = None  # type: Optional[BigSegmentStoreStatus]
        self.__poll_task = None  # type: Optional[AsyncRepeatingTask]

        if self.__store:
            self.__cache = ExpiringDict(max_len=config.context_cache_size, max_age_seconds=config.context_cache_time)
            self.__poll_task = AsyncRepeatingTask("ldclient.bigsegment.status-poll", config.status_poll_interval, 0, self.poll_store_and_update_status)
            self.__poll_task.start()

    async def stop(self):
        if self.__poll_task:
            self.__poll_task.stop()
        if self.__store:
            await self.__store.stop()

    @property
    def status_provider(self) -> BigSegmentStoreStatusProvider:
        return self.__status_provider

    def get_status(self) -> BigSegmentStoreStatus:
        """Return the most recently polled status.

        When no status has been cached yet, the sync variant polls the store
        inline; the async variant (whose status getter cannot await) reports
        the store as unavailable until the polling task has run.
        """
        status = self.__last_status
        if status is None:
            return BigSegmentStoreStatus(False, False)
        return status if status else self.poll_store_and_update_status()  # type: ignore[return-value]

    async def get_user_membership(self, user_key: str) -> Tuple[Optional[dict], str]:
        if not self.__store:
            return None, BigSegmentsStatus.NOT_CONFIGURED
        membership = self.__cache.get(user_key)
        if membership is None:
            user_hash = _hash_for_user_key(user_key)
            try:
                membership = await self.__store.get_membership(user_hash)  # type: ignore[misc]
                if membership is None:
                    membership = self.EMPTY_MEMBERSHIP
                self.__cache[user_key] = membership
            except Exception as e:
                log.exception("Big Segment store membership query returned error: %s" % e)
                return None, BigSegmentsStatus.STORE_ERROR
        # First-call fallback: if the polling task hasn't run yet, poll inline now
        status = self.__last_status
        if status is None:
            status = await self.poll_store_and_update_status()
        if not status.available:
            return membership, BigSegmentsStatus.STORE_ERROR
        return membership, BigSegmentsStatus.STALE if status.stale else BigSegmentsStatus.HEALTHY

    async def poll_store_and_update_status(self) -> BigSegmentStoreStatus:
        new_status = BigSegmentStoreStatus(False, False)  # default to "unavailable" if we don't get a new status below
        if self.__store:
            try:
                metadata = await self.__store.get_metadata()  # type: ignore[misc]
                new_status = BigSegmentStoreStatus(True, (metadata is None) or self.is_stale(metadata.last_up_to_date))
            except Exception as e:
                log.exception("Big Segment store status query returned error: %s" % e)
        self.__last_status = new_status
        self.__status_provider._update_status(new_status)
        return new_status

    def is_stale(self, timestamp) -> bool:
        return (timestamp is None) or ((int(time.time() * 1000) - timestamp) >= self.__stale_after_millis)


def _hash_for_user_key(user_key: str) -> str:
    return base64.b64encode(sha256(user_key.encode('utf-8')).digest()).decode('utf-8')
