"""
Async concurrency primitives used by the async data source, event processor, and
data system. Each wraps a piece of fiddly asyncio plumbing (timeout-aware waits,
queue exception normalization, an interval-from-start repeating task, a bounded
task pool) that callers would otherwise inline repeatedly. The sync code uses the
equivalent stdlib/SDK primitives (``threading.Event``/``Lock``, ``queue.Queue``,
``RepeatingTask``, ``FixedThreadPool``) directly, so these have no sync twin.
"""

import asyncio
import inspect
import time
from queue import Empty as QueueEmpty  # noqa: F401  (shared timeout exception)
from queue import Full as QueueFull  # noqa: F401  (shared capacity exception)
from typing import Any, Callable, Optional, Set

from ldclient.impl.util import log


class AsyncEvent:
    """Wraps ``asyncio.Event``, adding a ``wait(timeout)`` that returns False on
    timeout instead of raising, to match ``threading.Event.wait``."""

    def __init__(self):
        self._event = asyncio.Event()

    def set(self) -> None:
        self._event.set()

    def clear(self) -> None:
        self._event.clear()

    def is_set(self) -> bool:
        return self._event.is_set()

    async def wait(self, timeout: Optional[float] = None) -> bool:
        if timeout is None:
            await self._event.wait()
            return True
        try:
            await asyncio.wait_for(self._event.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False


class AsyncLock:
    """Wraps ``asyncio.Lock`` as an async context manager."""

    def __init__(self):
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        await self._lock.acquire()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self._lock.release()
        return False

    def locked(self) -> bool:
        return self._lock.locked()


class AsyncQueue:
    """Wraps ``asyncio.Queue`` with a ``get(timeout)`` that raises the shared
    ``QueueEmpty`` on timeout, and a ``put_nowait`` that raises the shared
    ``QueueFull`` when a bounded queue is at capacity."""

    def __init__(self, maxsize: int = 0):
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)

    async def put(self, item: Any) -> None:
        await self._queue.put(item)

    def put_nowait(self, item: Any) -> None:
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            raise QueueFull() from None

    async def get(self, timeout: Optional[float] = None, block: bool = True) -> Any:
        if not block:
            try:
                return self._queue.get_nowait()
            except asyncio.QueueEmpty:
                raise QueueEmpty() from None
        if timeout is None:
            return await self._queue.get()
        try:
            return await asyncio.wait_for(self._queue.get(), timeout)
        except asyncio.TimeoutError:
            raise QueueEmpty()

    def empty(self) -> bool:
        return self._queue.empty()


# The handle type returned by spawn_handle.
TaskHandle = asyncio.Task


def _log_task_exception(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception() is not None:
        log.error("Unhandled exception in background task", exc_info=task.exception())


def spawn_handle(name: str, fn: Callable) -> TaskHandle:
    """Starts ``fn()`` as a background task and returns the task handle.
    Unhandled exceptions are logged."""
    task = asyncio.ensure_future(fn())
    try:
        task.set_name(name)
    except AttributeError:
        pass
    task.add_done_callback(_log_task_exception)
    return task


async def join_handle(handle: TaskHandle, timeout: float) -> None:
    """Waits up to ``timeout`` seconds for a spawned task to finish, mirroring
    ``Thread.join(timeout)``: the task's result/exception is not re-raised, and
    on timeout the task is cancelled so it does not leak. If the *calling* task
    is cancelled while joining, that cancellation propagates and the joined task
    is left running (its lifecycle is owned elsewhere).

    Uses ``asyncio.wait`` rather than ``wait_for``: it never cancels the joined
    task itself and never raises the task's result into us — completion is
    reported via the returned sets — so caller cancellation propagates without
    having to disambiguate it from the joined task's own cancellation."""
    done, _ = await asyncio.wait({handle}, timeout=timeout)
    if handle not in done:
        # Timed out — cancel so the task does not outlive the join.
        handle.cancel()
        return
    if not handle.cancelled():
        # Consume any exception so it is neither re-raised nor reported as
        # "never retrieved" (background failures are logged by spawn_handle).
        handle.exception()


class AsyncCallbackScheduler:
    """Bridges sync notification paths to async callbacks: ``call`` schedules
    a coroutine callback onto the event loop captured at construction time,
    logging any unhandled exception. Safe to invoke from any thread."""

    def __init__(self):
        self._loop = asyncio.get_running_loop()

    def call(self, fn: Callable, *args) -> None:
        future = asyncio.run_coroutine_threadsafe(fn(*args), self._loop)
        future.add_done_callback(self._log_exception)

    @staticmethod
    def _log_exception(future) -> None:
        if not future.cancelled() and future.exception() is not None:
            log.error("Unhandled exception in scheduled callback", exc_info=future.exception())


class AsyncTaskRunner:
    """Spawns named background tasks and stops them all on demand."""

    def __init__(self):
        self._tasks: Set[asyncio.Task] = set()
        self._stopped = False

    def spawn(self, name: str, fn: Callable) -> asyncio.Task:
        """Starts ``fn()`` as a background task and returns the task handle.
        Unhandled exceptions are logged."""
        task = asyncio.ensure_future(fn())
        try:
            task.set_name(name)
        except AttributeError:
            pass
        task.add_done_callback(self._on_done)
        self._tasks.add(task)
        return task

    def _on_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            log.error("Unhandled exception in background task", exc_info=task.exception())

    def is_stopped(self) -> bool:
        return self._stopped

    async def stop_all(self, timeout: float = 1) -> None:
        """Cancels all running tasks and waits for them to finish, logging a
        warning for any that do not terminate within ``timeout`` seconds."""
        self._stopped = True
        tasks = list(self._tasks)
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        _, pending = await asyncio.wait(tasks, timeout=timeout)
        for task in pending:
            log.warning("Task %s did not terminate in time", task.get_name())


class AsyncRepeatingTask:
    """Calls a callback repeatedly at fixed intervals on a background task.
    Mirrors the semantics of ``ldclient.impl.repeating_task.RepeatingTask``:
    the interval is measured from the start of each invocation, exceptions
    from the callback are logged, and ``stop()`` prevents any further
    invocations but cannot be undone."""

    def __init__(self, label: str, interval: float, initial_delay: float, callable: Callable):
        self.__label = label
        self.__interval = interval
        self.__initial_delay = initial_delay
        self.__action = callable
        self.__stop = AsyncEvent()
        self.__task: Optional[asyncio.Task] = None

    def start(self):
        """Starts the background task. Like a thread, the task can only be
        started once."""
        if self.__task is not None:
            raise RuntimeError("tasks can only be started once")
        self.__task = asyncio.ensure_future(self._run())
        try:
            self.__task.set_name(f"{self.__label}.repeating")
        except AttributeError:
            pass

    def stop(self):
        """Tells the background task to stop. It cannot be restarted after this."""
        self.__stop.set()
        task = self.__task
        # When stop() is called from within the action itself, let the loop
        # exit via the stop event rather than cancelling the current task.
        if task is not None and task is not asyncio.current_task():
            task.cancel()

    async def _run(self):
        try:
            if self.__initial_delay > 0:
                if await self.__stop.wait(self.__initial_delay):
                    return
            stopped = self.__stop.is_set()
            while not stopped:
                next_time = time.time() + self.__interval
                try:
                    result = self.__action()
                    if inspect.isawaitable(result):
                        await result
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.exception("Unexpected exception on worker task: %s" % e)
                delay = next_time - time.time()
                if delay > 0:
                    stopped = await self.__stop.wait(delay)
                else:
                    # Yield to the event loop between back-to-back invocations
                    await asyncio.sleep(0)
                    stopped = self.__stop.is_set()
        except asyncio.CancelledError:
            pass


class AsyncWorkerPool:
    """A fixed-size pool of concurrent tasks that rejects jobs when its limit
    is reached. Matches the contract of
    ``ldclient.impl.fixed_thread_pool.FixedThreadPool``."""

    def __init__(self, size: int, name: str):
        self._size = size
        self._name = name
        self._busy: Set[asyncio.Task] = set()
        self._event = AsyncEvent()
        self._stopped = False

    def execute(self, jobFn: Callable) -> bool:
        """Schedules a job for execution if the pool is not already at its
        limit, and returns True if successful; returns False if all workers
        are busy."""
        if self._stopped or len(self._busy) >= self._size:
            return False
        task = asyncio.ensure_future(self._run_job(jobFn))
        self._busy.add(task)
        return True

    async def _run_job(self, jobFn: Callable) -> None:
        try:
            result = jobFn()
            if inspect.isawaitable(result):
                await result
        except Exception:
            log.warning('Unhandled exception in worker thread', exc_info=True)
        finally:
            task = asyncio.current_task()
            if task is not None:
                self._busy.discard(task)
            self._event.set()

    async def wait(self) -> None:
        """Waits until all currently busy workers have completed their jobs."""
        while len(self._busy) > 0:
            self._event.clear()
            if len(self._busy) == 0:
                return
            await self._event.wait()

    def stop(self) -> None:
        """Tells the pool to reject any further jobs; active jobs run to
        completion."""
        self._stopped = True
