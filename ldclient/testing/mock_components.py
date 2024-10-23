import time
from typing import Callable

from ldclient.interfaces import BigSegmentStore, BigSegmentStoreMetadata


class MockBigSegmentStore(BigSegmentStore):
    def __init__(self):
        self.__get_metadata = lambda: BigSegmentStoreMetadata(time.time())
        self.__memberships = {}
        self.__membership_queries = []
        self.setup_metadata_always_up_to_date()

    def get_metadata(self) -> BigSegmentStoreMetadata:
        return self.__get_metadata()

    def get_membership(self, user_hash: str) -> dict:
        self.__membership_queries.append(user_hash)
        return self.__memberships.get(user_hash, None)

    def setup_metadata(self, callback: Callable[[], BigSegmentStoreMetadata]):
        self.__get_metadata = callback

    def setup_metadata_always_up_to_date(self):
        self.setup_metadata(lambda: BigSegmentStoreMetadata(time.time() * 1000))

    def setup_metadata_always_stale(self):
        self.setup_metadata(lambda: BigSegmentStoreMetadata(0))

    def setup_metadata_none(self):
        self.setup_metadata(lambda: None)

    def setup_metadata_error(self):
        self.setup_metadata(self.__fail)

    def setup_membership(self, user_hash: str, membership: dict):
        self.__memberships[user_hash] = membership

    @property
    def membership_queries(self) -> list:
        return self.__membership_queries.copy()

    def __fail(self):
        raise Exception("deliberate error")
