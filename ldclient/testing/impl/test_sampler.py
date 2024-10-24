from random import Random

from ldclient.impl.sampler import Sampler


def test_is_false_for_noninteger_values():
    sampler = Sampler(Random())
    for value in ["not an int", True, 3.0]:
        assert sampler.sample(value) is False


def test_is_false_for_nonpositive_integers():
    sampler = Sampler(Random())
    for value in range(-10, 1):
        assert sampler.sample(value) is False


def test_one_is_true():
    sampler = Sampler(Random())
    assert sampler.sample(1)


def test_can_control_sampling_ratio():
    sampler = Sampler(Random(0))

    count = 0
    for _ in range(0, 1_000):
        if sampler.sample(10):
            count += 1

    assert count == 114
