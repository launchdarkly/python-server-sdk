'''
Simple implementation of a thread-safe memoized value whose generator function will never be
run more than once, and whose value can be overridden by explicit assignment.
'''

from threading import RLock

class MemoizedValue(object):

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
