from ldclient.impl.retry_delay import RetryDelayStrategy, DefaultBackoffStrategy, DefaultJitterStrategy

import math
import time

def test_fixed_retry_delay():
    d0 = 10
    r = RetryDelayStrategy(d0, 0, None, None)
    t0 = time.time() - 60
    d1 = r.next_retry_delay(t0)
    d2 = r.next_retry_delay(t0 + 1)
    d3 = r.next_retry_delay(t0 + 2)
    assert d1 == d0
    assert d2 == d0
    assert d3 == d0

def test_backoff_without_jitter():
    d0 = 10
    max = 60
    r = RetryDelayStrategy(d0, 0, DefaultBackoffStrategy(max), None)
    t0 = time.time() - 60
    d1 = r.next_retry_delay(t0)
    d2 = r.next_retry_delay(t0 + 1)
    d3 = r.next_retry_delay(t0 + 2)
    d4 = r.next_retry_delay(t0 + 3)
    assert d1 == d0
    assert d2 == d0 * 2
    assert d3 == d0 * 4
    assert d4 == max

def test_jitter_without_backoff():
    d0 = 1
    seed = 1000
    r = RetryDelayStrategy(d0, 0, None, DefaultJitterStrategy(0.5, seed))
    t0 = time.time() - 60
    d1 = r.next_retry_delay(t0)
    d2 = r.next_retry_delay(t0 + 1)
    d3 = r.next_retry_delay(t0 + 2)
    assert math.trunc(d1 * 1000) == 611 # these are the randomized values we expect from that fixed seed value
    assert math.trunc(d2 * 1000) == 665
    assert math.trunc(d3 * 1000) == 950

def test_jitter_with_backoff():
    d0 = 1
    max = 60
    seed = 1000
    r = RetryDelayStrategy(d0, 0, DefaultBackoffStrategy(max), DefaultJitterStrategy(0.5, seed))
    t0 = time.time() - 60
    d1 = r.next_retry_delay(t0)
    d2 = r.next_retry_delay(t0 + 1)
    d3 = r.next_retry_delay(t0 + 2)
    assert math.trunc(d1 * 1000) == 611
    assert math.trunc(d2 / 2 * 1000) == 665
    assert math.trunc(d3 / 4 * 1000) == 950

def test_backoff_reset_interval():
    d0 = 10
    max = 60
    reset_interval = 45
    r = RetryDelayStrategy(d0, reset_interval, DefaultBackoffStrategy(max), None)

    t0 = time.time() - 60
    r.set_good_since(50)

    t1 = t0 + 1
    d1 = r.next_retry_delay(t1)
    assert d1 == d0

    t2 = t1 + 1
    r.set_good_since(t2)

    t3 = t2 + 10
    d2 = r.next_retry_delay(t3)
    assert d2 == d0 * 2

    t4 = t3 + d2
    r.set_good_since(t4)

    t5 = t4 + reset_interval
    d3 = r.next_retry_delay(t5)
    assert d3 == d0  # it's gone back to the initial delay because reset_interval has elapsed since t4
