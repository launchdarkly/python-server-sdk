"""
Internal helper class for caching. No longer used.
"""
# currently excluded from documentation - see docs/README.md

from threading import RLock

class MemoizedValue(object):
    """Simple implementation of a thread-safe memoized value whose generator function will never be
    run more than once, and whose value can be overridden by explicit assignment.

    .. deprecated:: 6.7.0
      No longer used. Retained here only in case third parties were using it for another purpose.
    """
    def __init__(self, generator):
        self.generator = generator
        self.inited = False
        self.value = None
        self.lock = RLock()

    def get(self):
        with self.lock:
            if not self.inited:
                self.value = self.generator()
                self.inited = True
            return self.value

    def set(self, value):
        with self.lock:
            self.value = value
            self.inited = True
