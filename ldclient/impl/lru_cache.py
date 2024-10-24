from collections import OrderedDict


class SimpleLRUCache:
    """A dictionary-based cache that removes the oldest entries when its limit is exceeded.
    Values are only refreshed by writing, not by reading. Not thread-safe.
    """

    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = OrderedDict()

    def get(self, key):
        return self.cache.get(key)

    '''
    Stores a value in the cache, evicting an old entry if necessary. Returns true if
    the item already existed, or false if it was newly added.
    '''

    def put(self, key, value):
        found = key in self.cache
        if found:
            self.cache.move_to_end(key)
        else:
            if len(self.cache) >= self.capacity:
                self.cache.popitem(last=False)
        self.cache[key] = value
        return found

    def clear(self):
        self.cache.clear()
