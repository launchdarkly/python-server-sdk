from ldclient.interfaces import BigSegmentStoreStatus, BigSegmentStoreStatusProvider
from typing import Callable, Optional

class NullBigSegmentStoreStatusProvider(BigSegmentStoreStatusProvider):
    def status(self) -> Optional[BigSegmentStoreStatus]:
        return None

    def add_listener(self, listener: Callable[[BigSegmentStoreStatus], None]) -> None:
        pass

    def remove_listener(self, listener: Callable[[BigSegmentStoreStatus], None]) -> None:
        pass
