from ldclient.impl.lru_cache import SimpleLRUCache


def test_retains_values_up_to_capacity():
    lru = SimpleLRUCache(3)
    assert lru.put("a", True) is False
    assert lru.put("b", True) is False
    assert lru.put("c", True) is False
    assert lru.put("a", True) is True
    assert lru.put("b", True) is True
    assert lru.put("c", True) is True


def test_discards_oldest_value_on_overflow():
    lru = SimpleLRUCache(2)
    assert lru.put("a", True) is False
    assert lru.put("b", True) is False
    assert lru.put("c", True) is False
    assert lru.get("a") is None
    assert lru.get("b") is True
    assert lru.get("c") is True


def test_value_becomes_new_on_replace():
    lru = SimpleLRUCache(2)
    assert lru.put("a", True) is False
    assert lru.put("b", True) is False
    assert lru.put("a", True) is True  # b is now oldest
    assert lru.put("c", True) is False  # b is discarded as oldest
    assert lru.get("a") is True
    assert lru.get("b") is None
    assert lru.get("c") is True
