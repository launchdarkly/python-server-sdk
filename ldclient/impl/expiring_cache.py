import time
from collections import OrderedDict
from threading import Lock
from typing import Any, Callable, Hashable, Optional, Tuple


class ExpiringCache:
    """A thread-safe cache with a maximum size and a maximum entry age.

    An entry expires ``max_age_seconds`` after it was last written. When the cache is full, a new
    key evicts the entry that was written first. Reads do not change the eviction order.
    """

    def __init__(self, max_len: int, max_age_seconds: float, clock: Callable[[], float] = time.monotonic):
        """
        :param max_len: the maximum number of entries; must be at least 1
        :param max_age_seconds: the time after a write at which an entry expires
        :param clock: returns the current time in seconds
        """
        if max_len < 1:
            raise ValueError("max_len must be at least 1")
        self._max_len = max_len
        self._max_age = max_age_seconds
        self._clock = clock
        self._data: 'OrderedDict[Hashable, Tuple[Any, float]]' = OrderedDict()
        self._lock = Lock()

    def get(self, key: Hashable, default: Optional[Any] = None) -> Any:
        """Returns the value for the key, or ``default`` if the key is missing or expired."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return default
            value, written_at = entry
            if self._clock() - written_at >= self._max_age:
                del self._data[key]
                return default
            return value

    def __setitem__(self, key: Hashable, value: Any) -> None:
        with self._lock:
            if key in self._data:
                del self._data[key]
            elif len(self._data) >= self._max_len:
                self._data.popitem(last=False)
            self._data[key] = (value, self._clock())

    def pop(self, key: Hashable, default: Optional[Any] = None) -> Any:
        """Removes the key and returns its value, or ``default`` if the key is missing or expired."""
        with self._lock:
            entry = self._data.pop(key, None)
            if entry is None:
                return default
            value, written_at = entry
            if self._clock() - written_at >= self._max_age:
                return default
            return value

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def __len__(self) -> int:
        """Returns the number of stored entries, including expired entries not yet removed."""
        with self._lock:
            return len(self._data)
