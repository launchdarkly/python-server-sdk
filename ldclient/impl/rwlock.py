import threading
from contextlib import contextmanager


class ReadWriteLock:
    """A lock object that allows many simultaneous "read locks", but
    only one "write lock." """

    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0

    def rlock(self):
        """Acquire a read lock. Blocks only if a thread has
        acquired the write lock."""
        self._read_ready.acquire()
        try:
            self._readers += 1
        finally:
            self._read_ready.release()

    def runlock(self):
        """Release a read lock."""
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notify_all()
        finally:
            self._read_ready.release()

    def lock(self):
        """Acquire a write lock. Blocks until there are no
        acquired read or write locks."""
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()

    def unlock(self):
        """Release a write lock."""
        self._read_ready.release()

    @contextmanager
    def read(self):
        """Context manager for acquiring a read lock.

        Usage:
            with lock.read():
                # read lock held here
                pass
        """
        self.rlock()
        try:
            yield self
        finally:
            self.runlock()

    @contextmanager
    def write(self):
        """Context manager for acquiring a write lock.

        Usage:
            with lock.write():
                # write lock held here
                pass
        """
        self.lock()
        try:
            yield self
        finally:
            self.unlock()
