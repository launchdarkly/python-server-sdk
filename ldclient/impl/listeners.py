from threading import RLock
from typing import Any, Callable

from ldclient.impl.util import log


class Listeners:
    """
    Simple abstraction for a list of callbacks that can receive a single value. Callbacks are
    done synchronously on the caller's thread.
    """

    def __init__(self):
        self.__listeners = []
        self.__lock = RLock()

    def has_listeners(self) -> bool:
        with self.__lock:
            return len(self.__listeners) > 0

    def add(self, listener: Callable):
        with self.__lock:
            self.__listeners.append(listener)

    def remove(self, listener: Callable):
        with self.__lock:
            try:
                self.__listeners.remove(listener)
            except ValueError:
                pass  # removing a listener that wasn't in the list is a no-op

    def notify(self, value: Any):
        with self.__lock:
            listeners_copy = self.__listeners.copy()
        for listener in listeners_copy:
            try:
                listener(value)
            except Exception as e:
                log.exception("Unexpected error in listener for %s: %s" % (type(value), e))
