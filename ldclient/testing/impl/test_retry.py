"""
Tests for ldclient.impl.retry.

Nothing here sleeps. A test that needs to move time on uses ``frozen_clock``,
which replaces the ``time`` module the retry module reads. Jitter is removed
for every test by an autouse fixture, so a delay assertion reads the
undisturbed value; the tests that are about jitter override it.
"""

import logging
import random
from contextlib import contextmanager
from unittest import mock

import pytest

from ldclient.impl import retry
from ldclient.impl.retry import (
    DEFAULT_INITIAL_RECONNECT_DELAY,
    EXTENDED_INITIAL_DELAY,
    EXTENDED_MAX_DELAY,
    POLLING_RESET_SUCCESSES,
    STREAMING_MAX_DELAY,
    STREAMING_RESET_INTERVAL,
    AfterConsecutiveSuccesses,
    AfterHealthyFor,
    FailureKind,
    RetryState,
    classify_http_status,
    for_polling,
    for_streaming
)
from ldclient.testing.test_util import fixed_retry_jitter, no_retry_jitter

NORMAL = FailureKind.NORMAL
UNEXPECTED = FailureKind.UNEXPECTED


class _FrozenClock:
    """Stands in for the ``time`` module. Time only moves when a test says so."""

    def __init__(self, now: float):
        self.now = now

    def time(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@contextmanager
def frozen_clock(now: float = 1000.0):
    """Freezes the clock the retry module reads.

    Patching the module's own ``time`` reference keeps the change local to
    ``ldclient.impl.retry``; every other module keeps the real clock.
    """
    clock = _FrozenClock(now)
    with mock.patch.object(retry, 'time', clock):
        yield clock


# The random draw just below 1, which subtracts as much jitter as the spec
# allows: half the delay.
FULL_JITTER = 0.9999999


@pytest.fixture(autouse=True)
def without_jitter():
    """Removes jitter for every test in this module, so a delay assertion can
    read the undisturbed value."""
    with no_retry_jitter():
        yield


@contextmanager
def real_jitter():
    """Restores the real random source, for a test that asserts the bounds hold
    for any draw rather than for one fixed value."""
    with mock.patch.object(retry, 'random', random):
        yield


def streaming_state(initial_delay=1):
    return for_streaming(initial_delay)


def polling_state(poll_interval=30):
    return for_polling(poll_interval)


class TestClassifyHttpStatus:
    @pytest.mark.parametrize("status", [400, 408, 429])
    def test_retryable_4xx_statuses_are_normal(self, status):
        assert classify_http_status(status) is NORMAL

    @pytest.mark.parametrize("status", [401, 403, 404, 405, 418, 499])
    def test_other_4xx_statuses_are_unexpected(self, status):
        assert classify_http_status(status) is UNEXPECTED

    @pytest.mark.parametrize("status", [500, 502, 503, 504, 599])
    def test_5xx_statuses_are_normal(self, status):
        assert classify_http_status(status) is NORMAL

    @pytest.mark.parametrize("status", [200, 204, 301, 399])
    def test_non_error_statuses_are_normal(self, status):
        assert classify_http_status(status) is NORMAL


class TestStreamingInitialDelayGuard:
    """``Config`` does not check ``initial_reconnect_delay``, and a value of
    zero would reconnect with no wait at all."""

    @pytest.mark.parametrize("configured", [0, -1, -0.5])
    def test_a_non_positive_delay_falls_back_to_the_default(self, configured, caplog):
        caplog.set_level(logging.WARNING)

        state = for_streaming(configured)

        assert state.min_delay == DEFAULT_INITIAL_RECONNECT_DELAY
        assert state.record_failure(NORMAL) == DEFAULT_INITIAL_RECONNECT_DELAY
        assert caplog.records[0].getMessage() == (
            "initial_reconnect_delay must be greater than zero; using the default of 1s"
        )

    @pytest.mark.parametrize("configured", [0.001, 0.5, 1, 5, 45])
    def test_a_positive_delay_is_left_alone(self, configured, caplog):
        caplog.set_level(logging.WARNING)

        state = for_streaming(configured)

        assert state.min_delay == configured
        assert state.record_failure(NORMAL) == configured
        assert caplog.records == []


class TestStreamingDelayTable:
    def test_normal_regime_doubles_up_to_the_ceiling(self):
        state = streaming_state(initial_delay=1)
        delays = [state.record_failure(NORMAL) for _ in range(8)]
        assert delays == [1, 2, 4, 8, 16, 30, 30, 30]

    def test_extended_regime_doubles_up_to_the_ceiling(self):
        state = streaming_state(initial_delay=1)
        delays = [state.record_failure(UNEXPECTED)]
        delays += [state.record_failure(NORMAL) for _ in range(5)]
        assert delays == [5 * 60, 10 * 60, 20 * 60, 40 * 60, 60 * 60, 60 * 60]

    def test_a_configured_initial_delay_raises_the_ceiling_with_it(self):
        # RETRY 1.5.4 as amended: maxDelay must not fall below initialDelay.
        state = streaming_state(initial_delay=45)
        assert state.max_delay == 45
        assert state.record_failure(NORMAL) == 45

    def test_the_ceiling_is_sticky_once_the_extended_regime_starts(self):
        # RETRY 1.5.5: a normal failure after an unexpected one must not lower
        # the bounds back to the normal regime.
        state = streaming_state(initial_delay=1)
        state.record_failure(UNEXPECTED)
        assert state.in_extended_regime
        assert state.max_delay == EXTENDED_MAX_DELAY

        state.record_failure(NORMAL)
        assert state.in_extended_regime
        assert state.max_delay == EXTENDED_MAX_DELAY
        assert state.min_delay == EXTENDED_INITIAL_DELAY

    def test_a_second_unexpected_failure_keeps_counting_up(self):
        # Restarting the count on every unexpected failure would pin the delay
        # at the extended initial delay for ever.
        state = streaming_state(initial_delay=1)
        assert state.record_failure(UNEXPECTED) == 5 * 60
        assert state.record_failure(UNEXPECTED) == 10 * 60
        assert state.record_failure(UNEXPECTED) == 20 * 60

    def test_the_streaming_defaults_match_the_spec(self):
        state = streaming_state(initial_delay=1)
        assert state.max_delay == STREAMING_MAX_DELAY
        assert state.operating_cadence == 0
        assert STREAMING_RESET_INTERVAL == 60


class TestJitter:
    def test_jitter_never_removes_more_than_half_the_delay(self):
        with fixed_retry_jitter(FULL_JITTER):
            state = streaming_state(initial_delay=8)
            delay = state.record_failure(NORMAL)
            assert 4 <= delay < 8

    def test_no_jitter_leaves_the_delay_alone(self):
        state = streaming_state(initial_delay=8)
        assert state.record_failure(NORMAL) == 8

    def test_every_delay_stays_within_the_jitter_bounds(self):
        # The real random source, so the bound has to hold for any draw rather
        # than for one seeded sequence.
        with real_jitter():
            state = streaming_state(initial_delay=1)
            for base in [1, 2, 4, 8, 16, 30, 30, 30]:
                delay = state.record_failure(NORMAL)
                assert base / 2 <= delay <= base


class TestStreamingReset:
    def test_a_minute_of_healthy_operation_resets_the_state(self):
        # RETRY 1.8.2. The whole minute passes instantly.
        with frozen_clock() as clock:
            state = streaming_state(initial_delay=1)
            state.record_failure(UNEXPECTED)
            state.record_failure(NORMAL)

            state.record_healthy()
            assert state.in_extended_regime, "the window has not elapsed yet"

            clock.advance(STREAMING_RESET_INTERVAL)
            assert state.maybe_reset()
            assert not state.in_extended_regime
            assert state.max_delay == STREAMING_MAX_DELAY
            assert state.record_failure(NORMAL) == 1

    def test_a_reset_also_happens_on_the_failure_that_ends_a_healthy_stretch(self):
        with frozen_clock() as clock:
            state = streaming_state(initial_delay=1)
            state.record_failure(NORMAL)
            state.record_failure(NORMAL)

            state.record_healthy()
            clock.advance(STREAMING_RESET_INTERVAL)

            # The state resets before this failure is counted, so the delay is
            # the first-retry delay again rather than the fourth.
            assert state.record_failure(NORMAL) == 1

    def test_a_short_healthy_stretch_does_not_reset(self):
        with frozen_clock() as clock:
            state = streaming_state(initial_delay=1)
            state.record_failure(NORMAL)

            state.record_healthy()
            clock.advance(STREAMING_RESET_INTERVAL - 1)
            assert state.record_failure(NORMAL) == 2

    def test_a_fast_flapping_connection_does_not_ratchet_into_the_extended_regime(self):
        # Every transport failure is normal, so no amount of flapping reaches
        # the extended regime. Each cycle is a healthy stretch shorter than the
        # reset window, so the delay climbs, but only to the normal ceiling.
        with frozen_clock() as clock:
            state = streaming_state(initial_delay=1)
            delays = []
            for _ in range(20):
                state.record_healthy()
                clock.advance(5)
                delays.append(state.record_failure(NORMAL))
                clock.advance(1)

            assert not state.in_extended_regime
            assert max(delays) == STREAMING_MAX_DELAY
            assert state.max_delay == STREAMING_MAX_DELAY


class TestPollingCadence:
    def test_a_normal_failure_polls_again_on_schedule(self):
        state = polling_state(poll_interval=30)
        assert [state.record_failure(NORMAL) for _ in range(4)] == [30, 30, 30, 30]

    def test_the_extended_regime_doubles_up_to_an_hour(self):
        state = polling_state(poll_interval=30)
        delays = [state.record_failure(UNEXPECTED)]
        delays += [state.record_failure(NORMAL) for _ in range(5)]
        assert delays == [5 * 60, 10 * 60, 20 * 60, 40 * 60, 60 * 60, 60 * 60]

    def test_the_wait_never_falls_below_the_poll_interval(self):
        # RETRY 1.4.9. Full jitter would otherwise halve the delay.
        with fixed_retry_jitter(FULL_JITTER):
            state = polling_state(poll_interval=30)
            assert state.record_failure(NORMAL) == 30
            assert state.record_failure(UNEXPECTED) >= 30

    def test_a_poll_interval_longer_than_the_extended_bounds_wins(self):
        state = polling_state(poll_interval=2 * 60 * 60)
        assert state.record_failure(UNEXPECTED) == 2 * 60 * 60
        assert state.max_delay == 2 * 60 * 60

    def test_one_success_restores_the_cadence_while_the_state_is_still_raised(self):
        # RETRY 1.4.8. Conflating this with the reset is the bug another SDK
        # shipped: its first successful poll after an outage still waited
        # twenty minutes or more.
        state = polling_state(poll_interval=30)
        state.record_failure(UNEXPECTED)
        state.record_failure(NORMAL)
        assert state.record_failure(NORMAL) == 20 * 60

        assert state.record_success() == 30
        assert state.in_extended_regime, "one success does not reset the state"

    def test_two_successes_in_a_row_reset_the_state(self):
        # RETRY 1.8.2 with the polling reset policy.
        state = polling_state(poll_interval=30)
        state.record_failure(UNEXPECTED)

        state.record_success()
        assert state.in_extended_regime

        state.record_success()
        assert not state.in_extended_regime
        assert state.record_failure(NORMAL) == 30

    def test_a_failure_between_two_successes_clears_the_first(self):
        state = polling_state(poll_interval=30)
        state.record_failure(UNEXPECTED)
        state.record_success()
        state.record_failure(NORMAL)
        state.record_success()
        assert state.in_extended_regime

        state.record_success()
        assert not state.in_extended_regime

    def test_the_polling_defaults_match_the_spec(self):
        state = polling_state(poll_interval=30)
        assert state.operating_cadence == 30
        assert state.min_delay == 30
        assert state.max_delay == 30
        assert POLLING_RESET_SUCCESSES == 2


class TestWaitOverride:
    def test_an_override_replaces_the_computed_wait(self):
        state = streaming_state(initial_delay=1)
        state.record_failure(NORMAL)
        assert state.record_failure(NORMAL, wait_override=7) == 7

    def test_an_override_still_respects_the_cadence(self):
        state = polling_state(poll_interval=30)
        assert state.record_failure(NORMAL, wait_override=1) == 30


class TestAttemptCount:
    def test_attempts_counts_every_failure(self):
        state = streaming_state(initial_delay=1)
        for _ in range(5):
            state.record_failure(NORMAL)
        assert state.attempts == 5

    def test_a_reset_does_not_clear_the_attempt_count(self):
        # The count is for logging, so it should keep counting across a reset.
        with frozen_clock() as clock:
            state = streaming_state(initial_delay=1)
            state.record_failure(NORMAL)
            state.record_healthy()
            clock.advance(STREAMING_RESET_INTERVAL)
            state.maybe_reset()
            assert state.attempts == 1


class TestResetPolicies:
    def test_healthy_for_tracks_the_start_of_the_stretch(self):
        with frozen_clock() as clock:
            policy = AfterHealthyFor(60)
            assert not policy.is_satisfied()

            policy.note_healthy()
            started = policy.healthy_since

            # A later signal must not push the start of the stretch out.
            clock.advance(40)
            policy.note_healthy()
            assert policy.healthy_since == started

            clock.advance(20)
            assert policy.is_satisfied()

    def test_healthy_for_is_cleared_by_a_failure(self):
        with frozen_clock() as clock:
            policy = AfterHealthyFor(60)
            policy.note_healthy()
            policy.note_failure()
            assert policy.healthy_since is None

            clock.advance(900)
            assert not policy.is_satisfied()

    def test_consecutive_successes_counts_up(self):
        policy = AfterConsecutiveSuccesses(2)
        policy.note_healthy()
        assert not policy.is_satisfied()
        policy.note_healthy()
        assert policy.is_satisfied()

    def test_consecutive_successes_is_cleared_by_a_failure(self):
        policy = AfterConsecutiveSuccesses(2)
        policy.note_healthy()
        policy.note_failure()
        assert policy.successes == 0
        assert not policy.is_satisfied()


class TestLongOutage:
    def test_a_long_outage_cannot_overflow_the_delay(self):
        state = RetryState(
            initial_delay=1,
            normal_ceiling=30,
            extended_initial_delay=EXTENDED_INITIAL_DELAY,
            extended_ceiling=EXTENDED_MAX_DELAY,
            reset_policy=AfterHealthyFor(60),
        )
        for _ in range(5000):
            delay = state.record_failure(NORMAL)
        assert delay == 30
