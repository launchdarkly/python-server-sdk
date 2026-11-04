import threading
import time

import pytest

from ldclient.impl.expiring_cache import ExpiringCache


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def make_cache(max_len=10, max_age=5.0):
    clock = FakeClock()
    return ExpiringCache(max_len, max_age, clock=clock), clock


@pytest.mark.parametrize("max_len", [0, -1])
def test_rejects_max_len_below_one(max_len):
    with pytest.raises(ValueError):
        ExpiringCache(max_len, 5.0)


def test_uses_monotonic_clock_by_default():
    cache = ExpiringCache(10, 5.0)
    cache["a"] = 1
    assert cache.get("a") == 1


def test_get_returns_stored_value():
    cache, _ = make_cache()
    cache["a"] = 1
    assert cache.get("a") == 1


def test_get_returns_none_for_missing_key():
    cache, _ = make_cache()
    assert cache.get("missing") is None


def test_get_returns_default_for_missing_key():
    cache, _ = make_cache()
    sentinel = object()
    assert cache.get("missing", sentinel) is sentinel


def test_get_returns_falsy_values():
    cache, _ = make_cache()
    cache["empty"] = []
    cache["zero"] = 0
    assert cache.get("empty", "default") == []
    assert cache.get("zero", "default") == 0


def test_value_is_available_until_max_age():
    cache, clock = make_cache(max_age=5.0)
    cache["a"] = 1
    clock.advance(4.999)
    assert cache.get("a") == 1


def test_value_expires_at_max_age():
    cache, clock = make_cache(max_age=5.0)
    cache["a"] = 1
    clock.advance(5.0)
    assert cache.get("a") is None


def test_get_of_expired_value_returns_default():
    cache, clock = make_cache(max_age=5.0)
    cache["a"] = 1
    clock.advance(10)
    assert cache.get("a", "default") == "default"


def test_get_removes_expired_entry():
    cache, clock = make_cache(max_age=5.0)
    cache["a"] = 1
    clock.advance(5.0)
    assert len(cache) == 1
    cache.get("a")
    assert len(cache) == 0


def test_get_does_not_extend_lifetime():
    cache, clock = make_cache(max_age=5.0)
    cache["a"] = 1
    clock.advance(3)
    assert cache.get("a") == 1
    clock.advance(2)
    assert cache.get("a") is None


def test_rewrite_extends_lifetime():
    cache, clock = make_cache(max_age=5.0)
    cache["a"] = 1
    clock.advance(3)
    cache["a"] = 2
    clock.advance(3)
    assert cache.get("a") == 2


def test_zero_max_age_expires_immediately():
    cache, _ = make_cache(max_age=0)
    cache["a"] = 1
    assert cache.get("a") is None


def test_entries_expire_independently():
    cache, clock = make_cache(max_age=5.0)
    cache["a"] = 1
    clock.advance(3)
    cache["b"] = 2
    clock.advance(2)
    assert cache.get("a") is None
    assert cache.get("b") == 2


def test_retains_entries_up_to_max_len():
    cache, _ = make_cache(max_len=3)
    for key in ("a", "b", "c"):
        cache[key] = key
    assert len(cache) == 3
    for key in ("a", "b", "c"):
        assert cache.get(key) == key


def test_evicts_first_written_entry_when_full():
    cache, _ = make_cache(max_len=2)
    cache["a"] = 1
    cache["b"] = 2
    cache["c"] = 3
    assert len(cache) == 2
    assert cache.get("a") is None
    assert cache.get("b") == 2
    assert cache.get("c") == 3


def test_read_does_not_protect_entry_from_eviction():
    cache, _ = make_cache(max_len=2)
    cache["a"] = 1
    cache["b"] = 2
    assert cache.get("a") == 1
    cache["c"] = 3
    assert cache.get("a") is None
    assert cache.get("b") == 2


def test_replacing_key_when_full_does_not_evict_other_entries():
    cache, _ = make_cache(max_len=2)
    cache["a"] = 1
    cache["b"] = 2
    cache["a"] = 10
    assert len(cache) == 2
    assert cache.get("a") == 10
    assert cache.get("b") == 2


def test_replaced_key_becomes_newest_for_eviction():
    cache, _ = make_cache(max_len=2)
    cache["a"] = 1
    cache["b"] = 2
    cache["a"] = 10
    cache["c"] = 3
    assert cache.get("b") is None
    assert cache.get("a") == 10
    assert cache.get("c") == 3


def test_max_len_of_one():
    cache, _ = make_cache(max_len=1)
    cache["a"] = 1
    cache["b"] = 2
    assert len(cache) == 1
    assert cache.get("a") is None
    assert cache.get("b") == 2


def test_expired_entries_count_toward_max_len():
    cache, clock = make_cache(max_len=2, max_age=5.0)
    cache["a"] = 1
    cache["b"] = 2
    clock.advance(10)
    cache["c"] = 3
    assert len(cache) == 2
    assert cache.get("c") == 3


def test_pop_removes_and_returns_value():
    cache, _ = make_cache()
    cache["a"] = 1
    assert cache.pop("a") == 1
    assert cache.get("a") is None
    assert len(cache) == 0


def test_pop_of_missing_key_returns_default():
    cache, _ = make_cache()
    assert cache.pop("missing") is None
    assert cache.pop("missing", "default") == "default"


def test_pop_of_expired_key_returns_default_and_removes_entry():
    cache, clock = make_cache(max_age=5.0)
    cache["a"] = 1
    clock.advance(5.0)
    assert cache.pop("a", "default") == "default"
    assert len(cache) == 0


def test_pop_frees_space_for_new_key():
    cache, _ = make_cache(max_len=2)
    cache["a"] = 1
    cache["b"] = 2
    cache.pop("a")
    cache["c"] = 3
    assert cache.get("b") == 2
    assert cache.get("c") == 3


def test_clear_removes_all_entries():
    cache, _ = make_cache()
    cache["a"] = 1
    cache["b"] = 2
    cache.clear()
    assert len(cache) == 0
    assert cache.get("a") is None
    assert cache.get("b") is None


def test_cache_is_usable_after_clear():
    cache, _ = make_cache()
    cache["a"] = 1
    cache.clear()
    cache["a"] = 2
    assert cache.get("a") == 2


def test_len_of_empty_cache_is_zero():
    cache, _ = make_cache()
    assert len(cache) == 0


def test_keys_can_be_tuples():
    cache, _ = make_cache()
    cache[("features", "flag")] = 1
    assert cache.get(("features", "flag")) == 1


def test_concurrent_writes_never_exceed_max_len():
    max_len = 50
    cache = ExpiringCache(max_len, 60)
    thread_count = 8
    writes_per_thread = 2000
    barrier = threading.Barrier(thread_count)
    errors = []

    def writer(thread_id):
        try:
            barrier.wait()
            for i in range(writes_per_thread):
                cache[(thread_id, i % (max_len * 2))] = i
                assert len(cache) <= max_len
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=writer, args=(t,)) for t in range(thread_count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert len(cache) == max_len


def test_concurrent_reads_writes_and_pops_do_not_raise():
    cache = ExpiringCache(20, 0.001)
    thread_count = 8
    operations_per_thread = 2000
    barrier = threading.Barrier(thread_count)
    errors = []

    def worker(thread_id):
        try:
            barrier.wait()
            for i in range(operations_per_thread):
                key = i % 40
                operation = (thread_id + i) % 4
                if operation == 0:
                    cache[key] = i
                elif operation == 1:
                    cache.get(key)
                elif operation == 2:
                    cache.pop(key)
                else:
                    len(cache)
                if i % 500 == 0:
                    time.sleep(0.001)
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(thread_count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert len(cache) <= 20
