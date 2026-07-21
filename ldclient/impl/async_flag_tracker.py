from typing import Any, Callable

from ldclient.context import Context
from ldclient.impl.aio.concurrency import AsyncCallbackScheduler, AsyncLock
from ldclient.impl.listeners import Listeners
from ldclient.interfaces import FlagChange, FlagTracker, FlagValueChange


class AsyncFlagValueChangeListener:
    """Calls the user's listener when a specific flag's evaluated value changes for a specific context."""

    def __init__(self, key: str, context: Context, listener: Callable[[FlagValueChange], None], eval_fn: Callable, scheduler: AsyncCallbackScheduler):
        self.__key = key
        self.__context = context
        self.__listener = listener
        self.__eval_fn = eval_fn
        self.__scheduler = scheduler

        self.__lock = AsyncLock()
        self.__value: Any = None

    @classmethod
    async def create(cls, key: str, context: Context, listener: Callable[[FlagValueChange], None], eval_fn: Callable, scheduler: AsyncCallbackScheduler) -> 'AsyncFlagValueChangeListener':
        """Evaluates the flag once to capture the baseline value, then returns the listener."""
        instance = cls(key, context, listener, eval_fn, scheduler)
        instance.__value = await eval_fn(key, context)
        return instance

    def __call__(self, flag_change: FlagChange):
        self.__scheduler.call(self._on_flag_change, flag_change)

    async def _on_flag_change(self, flag_change: FlagChange):
        if flag_change.key != self.__key:
            return

        async with self.__lock:
            new_value = await self.__eval_fn(self.__key, self.__context)
            old_value, self.__value = self.__value, new_value

        if new_value == old_value:
            return

        self.__listener(FlagValueChange(self.__key, old_value, new_value))


class AsyncFlagTrackerImpl(FlagTracker):
    def __init__(self, listeners: Listeners, eval_fn: Callable):
        self.__listeners = listeners
        self.__eval_fn = eval_fn
        self.__scheduler = AsyncCallbackScheduler()

    def add_listener(self, listener: Callable[[FlagChange], None]):
        self.__listeners.add(listener)

    def remove_listener(self, listener: Callable[[FlagChange], None]):
        self.__listeners.remove(listener)

    async def add_flag_value_change_listener(self, key: str, context: Context, fn: Callable[[FlagValueChange], None]) -> Callable[[FlagChange], None]:
        listener = await AsyncFlagValueChangeListener.create(key, context, fn, self.__eval_fn, self.__scheduler)
        self.add_listener(listener)

        return listener
