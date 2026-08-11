from typing import Any, Callable, Optional

from ldclient.context import Context
from ldclient.impl.aio.concurrency import AsyncCallbackScheduler, AsyncLock
from ldclient.impl.listeners import Listeners
from ldclient.interfaces import AsyncFlagTracker, FlagChange, FlagValueChange


class AsyncFlagValueChangeListener:
    """Calls the user's listener when a specific flag's evaluated value changes for a specific context."""

    def __init__(self, key: str, context: Context, listener: Callable[[FlagValueChange], None], eval_fn: Callable, scheduler: AsyncCallbackScheduler, initial_value: Any):
        self.__key = key
        self.__context = context
        self.__listener = listener
        self.__eval_fn = eval_fn
        self.__scheduler = scheduler

        self.__lock = AsyncLock()
        self.__value = initial_value

    @classmethod
    async def create(cls, key: str, context: Context, listener: Callable[[FlagValueChange], None], eval_fn: Callable, scheduler: AsyncCallbackScheduler) -> 'AsyncFlagValueChangeListener':
        """Evaluates the flag once to capture the baseline value, then returns the listener."""
        initial_value = await eval_fn(key, context)
        return cls(key, context, listener, eval_fn, scheduler, initial_value)

    def __call__(self, flag_change: FlagChange):
        if flag_change.key != self.__key:
            return
        self.__scheduler.call(self._on_flag_change)

    async def _on_flag_change(self):
        async with self.__lock:
            new_value = await self.__eval_fn(self.__key, self.__context)
            old_value, self.__value = self.__value, new_value

        if new_value == old_value:
            return

        self.__listener(FlagValueChange(self.__key, old_value, new_value))


class AsyncFlagTrackerImpl(AsyncFlagTracker):
    def __init__(self, listeners: Listeners, eval_fn: Callable):
        self.__listeners = listeners
        self.__eval_fn = eval_fn
        self.__scheduler: Optional[AsyncCallbackScheduler] = None

    def _get_scheduler(self) -> AsyncCallbackScheduler:
        """Creates the callback scheduler on first use. Called only from async
        methods, so a running loop always exists for it to capture."""
        if self.__scheduler is None:
            self.__scheduler = AsyncCallbackScheduler()
        return self.__scheduler

    def add_listener(self, listener: Callable[[FlagChange], None]):
        self.__listeners.add(listener)

    def remove_listener(self, listener: Callable[[FlagChange], None]):
        self.__listeners.remove(listener)

    async def add_flag_value_change_listener(self, key: str, context: Context, fn: Callable[[FlagValueChange], None]) -> Callable[[FlagChange], None]:
        listener = await AsyncFlagValueChangeListener.create(key, context, fn, self.__eval_fn, self._get_scheduler())
        self.add_listener(listener)

        return listener
