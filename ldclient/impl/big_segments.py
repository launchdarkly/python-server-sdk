from typing import Optional, Tuple

from expiringdict import ExpiringDict

from ldclient.config import BigSegmentsConfig
from ldclient.evaluation import BigSegmentsStatus
from ldclient.impl.big_segments_common import (
    EMPTY_MEMBERSHIP,
    BigSegmentStoreStatusProviderImpl,
    _hash_for_user_key,
    is_stale
)
from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.util import log
from ldclient.interfaces import (
    BigSegmentStoreStatus,
    BigSegmentStoreStatusProvider
)


class BigSegmentStoreManager:
    """
    Internal component that decorates the Big Segment store with caching behavior, and also polls the
    store to track its status.
    """

    def __init__(self, config: BigSegmentsConfig):
        self.__store = config.store

        self.__stale_after_millis = config.stale_after * 1000
        self.__status_provider = BigSegmentStoreStatusProviderImpl(self.get_status)
        self.__last_status = None  # type: Optional[BigSegmentStoreStatus]
        self.__poll_task = None  # type: Optional[RepeatingTask]

        if self.__store:
            self.__cache = ExpiringDict(max_len=config.context_cache_size, max_age_seconds=config.context_cache_time)
            self.__poll_task = RepeatingTask("ldclient.bigsegment.status-poll", config.status_poll_interval, 0, self.poll_store_and_update_status)
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
            return None, BigSegmentsStatus.NOT_CONFIGURED
        membership = self.__cache.get(user_key)
        if membership is None:
            user_hash = _hash_for_user_key(user_key)
            try:
                membership = self.__store.get_membership(user_hash)
                if membership is None:
                    membership = EMPTY_MEMBERSHIP
                self.__cache[user_key] = membership
            except Exception as e:
                log.exception("Big Segment store membership query returned error: %s" % e)
                return None, BigSegmentsStatus.STORE_ERROR
        status = self.__last_status
        if not status:
            status = self.poll_store_and_update_status()
        if not status.available:
            return membership, BigSegmentsStatus.STORE_ERROR
        return membership, BigSegmentsStatus.STALE if status.stale else BigSegmentsStatus.HEALTHY

    def get_status(self) -> BigSegmentStoreStatus:
        status = self.__last_status
        return status if status else self.poll_store_and_update_status()

    def poll_store_and_update_status(self) -> BigSegmentStoreStatus:
        new_status = BigSegmentStoreStatus(False, False)  # default to "unavailable" if we don't get a new status below
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
        return is_stale(timestamp, self.__stale_after_millis)
