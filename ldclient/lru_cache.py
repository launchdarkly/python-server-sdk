'''
A dictionary-based cache that removes the oldest entries when its limit is exceeded.
Values are only refreshed by writing, not by reading. Not thread-safe.
'''

from collections import OrderedDict


# Backport of Python 3.2 move_to_end method which doesn't exist in 2.7
class OrderedDictWithReordering(OrderedDict):
    if not hasattr(OrderedDict, 'move_to_end'):
        # backport of Python 3.2 logic
        def move_to_end(self, key, last=True):
            link_prev, link_next, key = link = self._OrderedDict__map[key]
            link_prev[1] = link_next
            link_next[0] = link_prev
            root = self._OrderedDict__root
            if last:
                last = root[0]
                link[0] = last
                link[1] = root
                last[1] = root[0] = link
            else:
                first = root[1]
                link[0] = root
                link[1] = first
                root[1] = first[0] = link


class SimpleLRUCache(object):
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = OrderedDictWithReordering()

    def get(self, key):
        return self.cache.get(key)

    '''
    Stores a value in the cache, evicting an old entry if necessary. Returns true if
    the item already existed, or false if it was newly added.
    '''
    def put(self, key, value):
        found = (key in self.cache)
        if found:
            self.cache.move_to_end(key)
        else:
            if len(self.cache) >= self.capacity:
                self.cache.popitem(last=False)
        self.cache[key] = value
        return found

    def clear(self):
        self.cache.clear()
