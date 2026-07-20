"""
Configuration types for the async SDK client. Currently this holds
:class:`AsyncBigSegmentsConfig`; the full :class:`AsyncConfig` lands in a later slice.

.. caution::
    This feature is experimental and should NOT be considered ready for production
    use. It may change or be removed without notice and is not subject to backwards
    compatibility guarantees.
"""

from typing import Optional

from ldclient.interfaces import AsyncBigSegmentStore


class AsyncBigSegmentsConfig:
    """Configuration options related to Big Segments for the async SDK client.

    Big Segments are a specific type of segments. For more information, read the LaunchDarkly
    documentation: https://docs.launchdarkly.com/home/users/big-segments

    If your application uses Big Segments, you will need to create an ``AsyncBigSegmentsConfig``
    that at a minimum specifies what database integration to use, and then pass the
    ``AsyncBigSegmentsConfig`` object as the ``big_segments`` parameter when creating an
    :class:`AsyncConfig`.
    """

    def __init__(self, store: Optional[AsyncBigSegmentStore] = None, context_cache_size: int = 1000, context_cache_time: float = 5, status_poll_interval: float = 5, stale_after: float = 120):
        """
        :param store: the implementation of :class:`ldclient.interfaces.AsyncBigSegmentStore` that
            will be used to query the Big Segments database
        :param context_cache_size: the maximum number of contexts whose Big Segment state will be cached
            by the SDK at any given time
        :param context_cache_time: the maximum length of time (in seconds) that the Big Segment state
            for a context will be cached by the SDK
        :param status_poll_interval: the interval (in seconds) at which the SDK will poll the Big
            Segment store to make sure it is available and to determine how long ago it was updated
        :param stale_after: the maximum length of time between updates of the Big Segments data
            before the data is considered out of date
        """
        self.__store = store
        self.__context_cache_size = context_cache_size
        self.__context_cache_time = context_cache_time
        self.__status_poll_interval = status_poll_interval
        self.__stale_after = stale_after

    @property
    def store(self) -> Optional[AsyncBigSegmentStore]:
        return self.__store

    @property
    def context_cache_size(self) -> int:
        return self.__context_cache_size

    @property
    def context_cache_time(self) -> float:
        return self.__context_cache_time

    @property
    def status_poll_interval(self) -> float:
        return self.__status_poll_interval

    @property
    def stale_after(self) -> float:
        return self.__stale_after
