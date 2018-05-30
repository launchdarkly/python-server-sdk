'''
A dictionary-based cache that removes the oldest entries when its limit is exceeded.
Values are only refreshed by writing, not by reading. Not thread-safe.
'''

from collections import OrderedDict

class SimpleLRUCache(object):
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
        found = (key in self.cache)
        if found:
            SimpleLRUCache._move_to_end(self.cache, key)
        else:
            if len(self.cache) >= self.capacity:
                self.cache.popitem(last=False)
        self.cache[key] = value
        return found

    if hasattr(OrderedDict, 'move_to_end'):
        def _move_to_end(od, key, last=True):
            od.move_to_end(key, last)
    else:
        # backport of Python 3.2 logic
        def _move_to_end(od, key, last=True):
            '''Move an existing element to the end (or beginning if last==False).
            Raises KeyError if the element does not exist.
            When last=True, acts like a fast version of self[key]=self.pop(key).
            '''
            link_prev, link_next, key = link = od.__map[key]
            link_prev[1] = link_next
            link_next[0] = link_prev
            root = od.__root
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
