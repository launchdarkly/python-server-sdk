from typing import Callable

from ldclient.context import Context
from ldclient.impl.listeners import Listeners
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.interfaces import FlagChange, FlagTracker, FlagValueChange


class FlagValueChangeListener:
    def __init__(self, key: str, context: Context, listener: Callable[[FlagValueChange], None], eval_fn: Callable):
        self.__key = key
        self.__context = context
        self.__listener = listener
        self.__eval_fn = eval_fn

        self.__lock = ReadWriteLock()
        self.__value = eval_fn(key, context)

    def __call__(self, flag_change: FlagChange):
        if flag_change.key != self.__key:
            return

        new_value = self.__eval_fn(self.__key, self.__context)

        self.__lock.lock()
        old_value, self.__value = self.__value, new_value
        self.__lock.unlock()

        if new_value == old_value:
            return

        self.__listener(FlagValueChange(self.__key, old_value, new_value))


class FlagTrackerImpl(FlagTracker):
    def __init__(self, listeners: Listeners, eval_fn: Callable):
        self.__listeners = listeners
        self.__eval_fn = eval_fn

    def add_listener(self, listener: Callable[[FlagChange], None]):
        self.__listeners.add(listener)

    def remove_listener(self, listener: Callable[[FlagChange], None]):
        self.__listeners.remove(listener)

    def add_flag_value_change_listener(self, key: str, context: Context, fn: Callable[[FlagValueChange], None]) -> Callable[[FlagChange], None]:
        listener = FlagValueChangeListener(key, context, fn, self.__eval_fn)
        self.add_listener(listener)

        return listener
