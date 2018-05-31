import pytest

from ldclient.lru_cache import SimpleLRUCache

def test_retains_values_up_to_capacity():
    lru = SimpleLRUCache(3)
    assert lru.put("a", True) == False
    assert lru.put("b", True) == False
    assert lru.put("c", True) == False
    assert lru.put("a", True) == True
    assert lru.put("b", True) == True
    assert lru.put("c", True) == True

def test_discards_oldest_value_on_overflow():
    lru = SimpleLRUCache(2)
    assert lru.put("a", True) == False
    assert lru.put("b", True) == False
    assert lru.put("c", True) == False
    assert lru.get("a") is None
    assert lru.get("b") == True
    assert lru.get("c") == True

def test_value_becomes_new_on_replace():
    lru = SimpleLRUCache(2)
    assert lru.put("a", True) == False
    assert lru.put("b", True) == False
    assert lru.put("a", True) == True  # b is now oldest
    assert lru.put("c", True) == False  # b is discarded as oldest
    assert lru.get("a") is True
    assert lru.get("b") is None
    assert lru.get("c") is True
