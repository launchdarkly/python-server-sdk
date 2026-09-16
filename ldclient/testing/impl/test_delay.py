from ldclient.impl.delay import FixedDelay


def test_fixed_delay_always_gives_the_same_wait():
    delays = FixedDelay(2.5)
    assert delays.next_delay == 2.5
    assert delays.next_delay == 2.5
