import time
from threading import Event, Thread
from typing import Callable

from ldclient.impl.util import log


class RepeatingTask:
    """
    A generic mechanism for calling a callback repeatedly at fixed intervals on a worker thread.
    """

    def __init__(self, label, interval: float, initial_delay: float, callable: Callable):
        """
        Creates the task, but does not start the worker thread yet.

        :param interval: maximum time in seconds between invocations of the callback
        :param initial_delay: time in seconds to wait before the first invocation
        :param callable: the function to execute repeatedly
        """
        self.__interval = interval
        self.__initial_delay = initial_delay
        self.__action = callable
        self.__stop = Event()
        self.__thread = Thread(target=self._run, name=f"{label}.repeating")
        self.__thread.daemon = True

    def start(self):
        """
        Starts the worker thread.
        """
        self.__thread.start()

    def stop(self):
        """
        Tells the worker thread to stop. It cannot be restarted after this.
        """
        self.__stop.set()

    def _run(self):
        if self.__initial_delay > 0:
            if self.__stop.wait(self.__initial_delay):
                return
        stopped = self.__stop.is_set()
        while not stopped:
            next_time = time.time() + self.__interval
            try:
                self.__action()
            except Exception as e:
                log.exception("Unexpected exception on worker thread: %s" % e)
            delay = next_time - time.time()
            stopped = self.__stop.wait(delay) if delay > 0 else self.__stop.is_set()
