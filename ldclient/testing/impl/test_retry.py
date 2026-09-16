"""
Tests for ldclient.impl.retry.

Nothing here sleeps. A test that needs to move time on uses ``frozen_clock``,
which replaces the ``time`` module the retry module reads. Jitter is removed
for every test by an autouse fixture, so a delay assertion reads the
undisturbed value; the tests that are about jitter override it.
"""

import logging
import math
import random
from contextlib import contextmanager
from unittest import mock

import pytest

from ldclient.config import (
    DEFAULT_INITIAL_RECONNECT_DELAY,
    DEFAULT_POLL_INTERVAL
)
from ldclient.impl import retry
from ldclient.impl.retry import (
    EXTENDED_CEILING_DELAY,
    EXTENDED_INITIAL_DELAY,
    NORMAL_STREAMING_CEILING_DELAY,
    POLLING_RESET_SUCCESSES,
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

    def monotonic(self) -> float:
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


# The random draw just below 1, which subtracts the most jitter possible:
# half the delay.
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


def failure_delay(state, kind=NORMAL) -> float:
    """Records a failure and reads back the wait it decided, which is the value
    a data source reads."""
    state.record_failure(kind)
    return state.next_delay


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


class TestFactoryInputGuards:
    """``Config`` does not check ``initial_reconnect_delay`` at all, and only
    clamps ``poll_interval``. A non-positive value would retry with no wait; a
    non-finite one makes the jitter arithmetic produce NaN."""

    @pytest.mark.parametrize(
        "configured",
        [0, -1, -0.5, float('inf'), float('-inf'), float('nan')],
        ids=["zero", "negative", "negative-fraction", "inf", "-inf", "nan"],
    )
    def test_streaming_falls_back_to_the_default(self, configured, caplog):
        caplog.set_level(logging.WARNING)

        state = for_streaming(configured)
        delay = failure_delay(state, NORMAL)

        assert state._min_delay == DEFAULT_INITIAL_RECONNECT_DELAY
        assert delay == DEFAULT_INITIAL_RECONNECT_DELAY
        assert math.isfinite(delay) and delay > 0
        assert caplog.records[0].getMessage() == (
            "initial_reconnect_delay must be a positive, finite number of seconds; "
            "using the default of 1s"
        )

    @pytest.mark.parametrize(
        "configured",
        [0, -5, float('inf'), float('-inf'), float('nan')],
        ids=["zero", "negative", "inf", "-inf", "nan"],
    )
    def test_polling_falls_back_to_the_default(self, configured, caplog):
        caplog.set_level(logging.WARNING)

        state = for_polling(configured)
        delay = failure_delay(state, NORMAL)

        assert state._operating_cadence == DEFAULT_POLL_INTERVAL
        assert delay == DEFAULT_POLL_INTERVAL
        assert math.isfinite(delay) and delay > 0
        assert caplog.records[0].getMessage() == (
            "poll_interval must be a positive, finite number of seconds; "
            "using the default of 30s"
        )

    @pytest.mark.parametrize("configured", [0.001, 0.5, 1, 5, 45])
    def test_a_positive_streaming_delay_is_left_alone(self, configured, caplog):
        caplog.set_level(logging.WARNING)

        state = for_streaming(configured)

        assert state._min_delay == configured
        assert failure_delay(state, NORMAL) == configured
        assert caplog.records == []

    @pytest.mark.parametrize("configured", [0.001, 1, 30, 300, 2 * 60 * 60])
    def test_a_positive_poll_interval_is_left_alone(self, configured, caplog):
        caplog.set_level(logging.WARNING)

        state = for_polling(configured)

        assert state._operating_cadence == configured
        assert failure_delay(state, NORMAL) == configured
        assert caplog.records == []


class TestStreamingExtendedDelayFloor:
    """A delay that applies after an unexpected failure must not be below the
    component's initial delay."""

    @pytest.mark.parametrize(
        "configured,expected",
        [(1, 300), (30, 300), (300, 300), (600, 600), (3600, 3600), (0, 300), (-5, 300)],
    )
    def test_the_extended_delay_never_starts_below_the_configured_delay(self, configured, expected):
        assert failure_delay(for_streaming(configured), UNEXPECTED) == expected

    @pytest.mark.parametrize("configured", [1, 30, 300, 600, 3600])
    def test_an_unexpected_failure_never_waits_less_than_a_normal_one(self, configured):
        normal = failure_delay(for_streaming(configured), NORMAL)
        unexpected = failure_delay(for_streaming(configured), UNEXPECTED)
        assert unexpected >= normal

    @pytest.mark.parametrize(
        "configured,ladder",
        [
            (1, [300, 600, 1200, 2400, 3600]),
            (600, [600, 1200, 2400, 3600, 3600]),
        ],
        ids=["default", "clamped"],
    )
    def test_the_extended_ladder_still_doubles_to_the_ceiling(self, configured, ladder):
        state = for_streaming(configured)
        delays = [failure_delay(state, UNEXPECTED)]
        delays += [failure_delay(state, NORMAL) for _ in range(4)]
        assert delays == ladder


class TestStreamingDelayTable:
    def test_normal_regime_doubles_up_to_the_ceiling(self):
        state = for_streaming(1)
        delays = [failure_delay(state, NORMAL) for _ in range(8)]
        assert delays == [1, 2, 4, 8, 16, 30, 30, 30]

    def test_extended_regime_doubles_up_to_the_ceiling(self):
        state = for_streaming(1)
        delays = [failure_delay(state, UNEXPECTED)]
        delays += [failure_delay(state, NORMAL) for _ in range(5)]
        assert delays == [5 * 60, 10 * 60, 20 * 60, 40 * 60, 60 * 60, 60 * 60]

    def test_a_configured_initial_delay_raises_the_ceiling_with_it(self):
        # The ceiling must not fall below the initial delay.
        state = for_streaming(45)
        assert state._max_delay == 45
        assert failure_delay(state, NORMAL) == 45

    def test_the_ceiling_is_sticky_once_the_extended_regime_starts(self):
        # A normal failure after an unexpected one must not lower the bounds
        # back to the normal regime.
        state = for_streaming(1)
        state.record_failure(UNEXPECTED)
        assert state._extended
        assert state._max_delay == EXTENDED_CEILING_DELAY

        state.record_failure(NORMAL)
        assert state._extended
        assert state._max_delay == EXTENDED_CEILING_DELAY
        assert state._min_delay == EXTENDED_INITIAL_DELAY

    def test_a_second_unexpected_failure_keeps_counting_up(self):
        # Restarting the count on every unexpected failure would pin the delay
        # at the extended initial delay for ever.
        state = for_streaming(1)
        assert failure_delay(state, UNEXPECTED) == 5 * 60
        assert failure_delay(state, UNEXPECTED) == 10 * 60
        assert failure_delay(state, UNEXPECTED) == 20 * 60

    def test_the_streaming_defaults_match_the_spec(self):
        state = for_streaming(1)
        assert state._max_delay == NORMAL_STREAMING_CEILING_DELAY
        assert state._operating_cadence == 0
        assert STREAMING_RESET_INTERVAL == 60


class TestJitter:
    def test_jitter_never_removes_more_than_half_the_delay(self):
        with fixed_retry_jitter(FULL_JITTER):
            state = for_streaming(8)
            delay = failure_delay(state, NORMAL)
            assert 4 <= delay < 8

    def test_no_jitter_leaves_the_delay_alone(self):
        state = for_streaming(8)
        assert failure_delay(state, NORMAL) == 8

    def test_every_delay_stays_within_the_jitter_bounds(self):
        # The real random source, so the bound has to hold for any draw rather
        # than for one seeded sequence.
        with real_jitter():
            state = for_streaming(1)
            for base in [1, 2, 4, 8, 16, 30, 30, 30]:
                delay = failure_delay(state, NORMAL)
                assert base / 2 <= delay <= base


class TestWaitBetweenOperations:
    def test_a_streaming_success_schedules_no_wait(self):
        """Streaming's cadence is zero, and a success schedules the cadence."""
        state = for_streaming(1)
        state.record_failure(NORMAL)
        state.record_success()

        assert state.next_delay == 0

    def test_a_zero_cadence_puts_no_floor_under_a_retry(self):
        """The floor is the cadence, so streaming's jitter is free to take a
        retry below the configured delay."""
        with fixed_retry_jitter(FULL_JITTER):
            state = for_streaming(1)
            assert failure_delay(state, NORMAL) == pytest.approx(0.5)

    def test_only_streaming_can_yield_a_zero_wait(self):
        """Polling is the only data source that reads next_delay as a
        DelaySource, so a zero there would busy-loop its scheduler. Its
        cadence floor rules that out for every outcome."""
        state = for_polling(30)
        assert state.next_delay == 30
        with real_jitter():
            for kind in (NORMAL, UNEXPECTED, NORMAL, UNEXPECTED):
                state.record_failure(kind)
                assert state.next_delay >= 30
                state.record_success()
                assert state.next_delay == 30

    def test_a_polling_success_schedules_the_cadence(self):
        state = for_polling(30)
        state.record_failure(NORMAL)
        state.record_success()

        assert state.next_delay == 30

    def test_a_fresh_state_and_a_success_agree(self):
        """The constructor and record_success share one expression, so the two
        cannot drift apart."""
        for state in (for_streaming(5), for_polling(45)):
            fresh = state.next_delay
            state.record_success()
            assert state.next_delay == fresh


class TestStreamingReset:
    def test_a_minute_of_healthy_operation_resets_the_state(self):
        # The whole minute passes instantly.
        with frozen_clock() as clock:
            state = for_streaming(1)
            state.record_failure(UNEXPECTED)
            state.record_failure(NORMAL)

            state.record_success()
            assert state._extended, "the window has not elapsed yet"

            clock.advance(STREAMING_RESET_INTERVAL)
            state.record_success()
            assert not state._extended
            assert state._max_delay == NORMAL_STREAMING_CEILING_DELAY
            assert failure_delay(state, NORMAL) == 1

    def test_a_reset_also_happens_on_the_failure_that_ends_a_healthy_stretch(self):
        with frozen_clock() as clock:
            state = for_streaming(1)
            state.record_failure(NORMAL)
            state.record_failure(NORMAL)

            state.record_success()
            clock.advance(STREAMING_RESET_INTERVAL)

            # The state resets before this failure is counted, so the delay is
            # the first-retry delay again rather than the fourth.
            assert failure_delay(state, NORMAL) == 1

    def test_a_short_healthy_stretch_does_not_reset(self):
        with frozen_clock() as clock:
            state = for_streaming(1)
            state.record_failure(NORMAL)

            state.record_success()
            clock.advance(STREAMING_RESET_INTERVAL - 1)
            assert failure_delay(state, NORMAL) == 2

    def test_a_fast_flapping_connection_does_not_ratchet_into_the_extended_regime(self):
        # Every transport failure is normal, so no amount of flapping reaches
        # the extended regime. Each cycle is a healthy stretch shorter than the
        # reset window, so the delay climbs, but only to the normal ceiling.
        with frozen_clock() as clock:
            state = for_streaming(1)
            delays = []
            for _ in range(20):
                state.record_success()
                clock.advance(5)
                delays.append(failure_delay(state, NORMAL))
                clock.advance(1)

            assert not state._extended
            assert max(delays) == NORMAL_STREAMING_CEILING_DELAY
            assert state._max_delay == NORMAL_STREAMING_CEILING_DELAY


class TestPollingCadence:
    def test_a_normal_failure_polls_again_on_schedule(self):
        state = for_polling(30)
        assert [failure_delay(state, NORMAL) for _ in range(4)] == [30, 30, 30, 30]

    def test_the_extended_regime_doubles_up_to_an_hour(self):
        state = for_polling(30)
        delays = [failure_delay(state, UNEXPECTED)]
        delays += [failure_delay(state, NORMAL) for _ in range(5)]
        assert delays == [5 * 60, 10 * 60, 20 * 60, 40 * 60, 60 * 60, 60 * 60]

    def test_the_wait_never_falls_below_the_poll_interval(self):
        # Full jitter would otherwise halve the delay.
        with fixed_retry_jitter(FULL_JITTER):
            state = for_polling(30)
            assert failure_delay(state, NORMAL) == 30
            assert failure_delay(state, UNEXPECTED) >= 30

    def test_a_poll_interval_longer_than_the_extended_bounds_wins(self):
        # The ceiling is lifted by record_failure clamping it against the
        # initial delay, not by for_polling clamping the ceiling itself.
        state = for_polling(2 * 60 * 60)
        assert failure_delay(state, UNEXPECTED) == 2 * 60 * 60
        assert state._max_delay == 2 * 60 * 60
        assert state._min_delay == 2 * 60 * 60

    def test_one_success_restores_the_cadence_while_the_state_is_still_raised(self):
        # A backoff wait applies to a retry, not to every operation.
        # Conflating this with the reset would leave the first successful poll
        # after an outage still waiting twenty minutes.
        state = for_polling(30)
        state.record_failure(UNEXPECTED)
        state.record_failure(NORMAL)
        assert failure_delay(state, NORMAL) == 20 * 60

        state.record_success()
        assert state.next_delay == 30
        assert state._extended, "one success does not reset the state"

    def test_two_successes_in_a_row_reset_the_state(self):
        state = for_polling(30)
        state.record_failure(UNEXPECTED)

        state.record_success()
        assert state._extended

        state.record_success()
        assert not state._extended
        assert state.next_delay == 30
        assert failure_delay(state, NORMAL) == 30

    def test_a_failure_between_two_successes_clears_the_first(self):
        state = for_polling(30)
        state.record_failure(UNEXPECTED)
        state.record_success()
        state.record_failure(NORMAL)
        state.record_success()
        assert state._extended

        state.record_success()
        assert not state._extended

    def test_the_polling_defaults_match_the_spec(self):
        state = for_polling(30)
        assert state._operating_cadence == 30
        assert state._min_delay == 30
        assert state._max_delay == 30
        assert POLLING_RESET_SUCCESSES == 2


class TestAttemptCount:
    def test_attempts_counts_every_failure(self):
        state = for_streaming(1)
        for _ in range(5):
            state.record_failure(NORMAL)
        assert state._attempts == 5

    def test_a_reset_starts_the_attempt_count_over(self):
        # A reset clears the counter, so the next failure is attempt 1.
        with frozen_clock() as clock:
            state = for_streaming(1)
            state.record_failure(NORMAL)
            state.record_failure(NORMAL)
            assert state._attempts == 2

            state.record_success()
            clock.advance(STREAMING_RESET_INTERVAL)
            state.record_success()

            # The delay drops back to the first-retry value, and the count
            # starts over with it.
            assert failure_delay(state, NORMAL) == 1
            assert state._attempts == 1


class TestResetPolicies:
    def test_healthy_for_tracks_the_start_of_the_stretch(self):
        with frozen_clock() as clock:
            policy = AfterHealthyFor(60)
            assert not policy.is_satisfied()

            policy.note_healthy()
            started = policy._healthy_since

            # A later signal must not push the start of the stretch out.
            clock.advance(40)
            policy.note_healthy()
            assert policy._healthy_since == started

            clock.advance(20)
            assert policy.is_satisfied()

    def test_many_healthy_signals_do_not_move_the_window(self):
        """Streaming signals on every message, so an unconditional assignment
        here would push its reset out for ever."""
        with frozen_clock() as clock:
            policy = AfterHealthyFor(60)
            policy.note_healthy()
            first = policy._healthy_since

            for _ in range(59):
                clock.advance(1)
                policy.note_healthy()

            assert policy._healthy_since == first
            assert not policy.is_satisfied()

            # The threshold lands 60s after the first signal, not the last.
            clock.advance(1)
            policy.note_healthy()
            assert policy.is_satisfied()

    def test_healthy_for_is_cleared_by_a_failure(self):
        with frozen_clock() as clock:
            policy = AfterHealthyFor(60)
            policy.note_healthy()
            policy.note_failure()
            assert policy._healthy_since is None

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
        assert policy._successes == 0
        assert not policy.is_satisfied()


class TestLongOutage:
    def test_a_long_outage_cannot_overflow_the_delay(self):
        state = RetryState(
            normal_initial_delay=1,
            normal_ceiling_delay=30,
            extended_initial_delay=EXTENDED_INITIAL_DELAY,
            extended_ceiling_delay=EXTENDED_CEILING_DELAY,
            reset_policy=AfterHealthyFor(60),
        )
        for _ in range(5000):
            delay = failure_delay(state, NORMAL)
        assert delay == 30
