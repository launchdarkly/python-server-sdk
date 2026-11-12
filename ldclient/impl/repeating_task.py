from threading import TIMEOUT_MAX, Event, Thread
from typing import Any, Callable

from ldclient.impl.delay import DelaySource, FixedDelay
from ldclient.impl.util import log


class RepeatingTask:
    """
    A generic mechanism for calling a callback repeatedly on a worker thread.

    The wait between invocations comes from a
    :class:`~ldclient.impl.delay.DelaySource`, which the
    task reads after each one. Use :meth:`at_interval` for the common case of
    a fixed interval.
    """

    def __init__(self, label: str, delays: DelaySource, initial_delay: float, callable: Callable[[], Any]):
        """
        Creates the task, but does not start the worker thread yet.

        :param label: names the worker thread, and appears in log messages
        :param delays: supplies the wait after each invocation returns
        :param initial_delay: time in seconds to wait before the first invocation
        :param callable: the function to execute repeatedly. Anything it
            returns is ignored.
        """
        self.__label = label
        self.__delays = delays
        self.__initial_delay = initial_delay
        self.__action = callable
        self.__stop = Event()
        self.__started = False
        self.__thread = Thread(target=self._run, name=f"{label}.repeating")
        self.__thread.daemon = True

    @staticmethod
    def at_interval(label: str, interval: float, initial_delay: float, callable: Callable[[], Any]) -> 'RepeatingTask':
        """
        Creates a task that runs at a fixed interval.

        :param interval: time in seconds to wait after each invocation returns
        """
        return RepeatingTask(label, FixedDelay(interval), initial_delay, callable)

    def start(self):
        """
        Starts the worker thread, if it is not running already.

        Starting a task twice logs and does nothing, rather than raising, so a
        caller that is safe to call more than once stays safe.
        """
        if self.__started:
            log.info("Task %s has already been started; ignoring" % self.__label)
            return
        self.__started = True
        self.__thread.start()

    def stop(self):
        """
        Tells the worker thread to stop.

        The stop is permanent. A later :meth:`start` does not resume the task.
        """
        self.__stop.set()

    def _run(self):
        if self.__initial_delay > 0:
            if self.__stop.wait(min(self.__initial_delay, TIMEOUT_MAX)):
                return
        stopped = self.__stop.is_set()
        while not stopped:
            try:
                self.__action()
            except Exception as e:
                log.exception("Unexpected exception on worker thread: %s" % e)
            # The wait starts when the callback returns, so a slow callback
            # never shortens it.
            delay = self.__delays.next_delay
            stopped = self.__stop.wait(min(delay, TIMEOUT_MAX)) if delay > 0 else self.__stop.is_set()
